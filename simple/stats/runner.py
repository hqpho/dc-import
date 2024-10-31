# Copyright 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import StrEnum
import json
import logging

from stats import constants
from stats import schema
from stats import stat_var_hierarchy_generator
from stats.cache import generate_svg_cache
from stats.config import Config
from stats.data import ImportType
from stats.data import InputFileFormat
from stats.data import ParentSVG2ChildSpecializedNames
from stats.data import Triple
from stats.data import VerticalSpec
from stats.db import create_and_update_db
from stats.db import create_main_dc_config
from stats.db import create_sqlite_config
from stats.db import get_cloud_sql_config_from_env
from stats.db import get_sqlite_config_from_env
from stats.db import ImportStatus
from stats.entities_importer import EntitiesImporter
from stats.events_importer import EventsImporter
from stats.importer import Importer
from stats.mcf_importer import McfImporter
import stats.nl as nl
from stats.nodes import Nodes
from stats.observations_importer import ObservationsImporter
from stats.reporter import ImportReporter
import stats.schema_constants as sc
from stats.variable_per_row_importer import VariablePerRowImporter
from util.filesystem import create_store
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import Store


class RunMode(StrEnum):
  CUSTOM_DC = "customdc"
  SCHEMA_UPDATE = "schemaupdate"
  MAIN_DC = "maindc"


class Runner:
  """Runs and coordinates all imports.
    """

  def __init__(self,
               config_file_path: str,
               input_dir_path: str,
               output_dir_path: str,
               mode: RunMode = RunMode.CUSTOM_DC) -> None:
    assert config_file_path or input_dir_path, "One of config_file or input_dir must be specified"
    assert output_dir_path, "output_dir must be specified"

    self.mode = mode

    # File systems, both input and output. Must be closed when run finishes.
    self.stores: list[Store] = []
    # Input-only stores
    self.input_stores: list[Store] = []

    # "Special" file handlers.
    # i.e. if files of these types are present, they are handled in specific ways.
    # self.special_handlers: dict[str, FileHandler] = {}
    self.special_files: dict[str, File] = {}
    self.svg_specialized_names: ParentSVG2ChildSpecializedNames = {}

    # Config file driven.
    if config_file_path:
      with create_store(config_file_path) as config_store:
        config_data = config_store.as_file().read()
        self.config = Config(data=json.loads(config_data))

      input_urls = self.config.data_download_urls()
      if not input_urls:
        raise ValueError("Data Download URLs not found in config.")
      for input_url in input_urls:
        input_store = create_store(input_url)
        self.stores.append(input_store)
        self.input_stores.append(input_store)

    # Input dir driven.
    else:
      input_store = create_store(input_dir_path)
      self.stores.append(input_store)
      self.input_stores.append(input_store)

      config_file = input_store.as_dir().open_file(
          constants.CONFIG_JSON_FILE_NAME, create_if_missing=False)
      # TODO Catch FileNotFoundError
      self.config = Config(data=json.loads(config_file.read()))

    # Get dict of special file type string to special file name.
    # Example entry: verticalSpecsFile -> vertical_specs.json
    self.special_file_names_by_type = self.config.special_files()

    # Output directories
    output_store = create_store(output_dir_path, create_if_missing=True)
    self.stores.append(output_store)
    self.output_dir = output_store.as_dir()
    self.nl_dir = self.output_dir.open_dir(constants.NL_DIR_NAME)
    self.process_dir = self.output_dir.open_dir(constants.PROCESS_DIR_NAME)

    # Reporter.
    self.reporter = ImportReporter(
        report_file=self.process_dir.open_file(constants.REPORT_JSON_FILE_NAME))

    self.nodes = Nodes(self.config)
    self.db = None

  def run(self):
    try:
      if (self.db is None):
        self.db = create_and_update_db(self._get_db_config())

      if self.mode == RunMode.SCHEMA_UPDATE:
        logging.info("Skipping imports because run mode is schema update.")

      elif self.mode == RunMode.CUSTOM_DC or self.mode == RunMode.MAIN_DC:
        self._run_imports_and_do_post_import_work()

      else:
        raise ValueError(f"Unsupported mode: {self.mode}")

      # Commit and close DB.
      self.db.commit_and_close()

      # Report done.
      self.reporter.report_done()
    except Exception as e:
      logging.exception("Error updating stats")
      self.reporter.report_failure(error=str(e))

  def _get_db_config(self) -> dict:
    if self.mode == RunMode.MAIN_DC:
      logging.info("Using Main DC config.")
      return create_main_dc_config(self.output_dir.path)
    # Attempt to get from env (cloud sql, then sqlite),
    # then config file, then default.
    db_cfg = get_cloud_sql_config_from_env()
    if db_cfg:
      logging.info("Using Cloud SQL settings from env.")
      return db_cfg
    db_cfg = get_sqlite_config_from_env()
    if db_cfg:
      logging.info("Using SQLite settings from env.")
      return db_cfg
    logging.info("Using default DB settings.")
    return create_sqlite_config(
        self.output_dir.open_file(constants.DB_FILE_NAME).full_path())

  def _run_imports_and_do_post_import_work(self):
    # (SQL only) Drop data in existing tables (except import metadata).
    # Also drop indexes for faster writes.
    self.db.maybe_clear_before_import()

    # Import data from all input files.
    self._run_all_data_imports()

    # Generate triples.
    triples = self.nodes.triples()
    # Write triples to DB.
    self.db.insert_triples(triples)

    # Generate SVG hierarchy.
    self._generate_svg_hierarchy()

    # Generate SVG cache.
    self._generate_svg_cache()

    # Generate NL sentences for creating embeddings.
    self._generate_nl_sentences()

    # Write import info to DB.
    self.db.insert_import_info(status=ImportStatus.SUCCESS)

  def _generate_nl_sentences(self):
    triples: list[Triple] = []
    # Get topic triples if generating topics else get SV triples.
    if self.config.generate_topics():
      triples = self.db.select_triples_by_subject_type(sc.TYPE_TOPIC)
    else:
      triples = self.db.select_triples_by_subject_type(
          sc.TYPE_STATISTICAL_VARIABLE)

    # Generate sentences.
    nl.generate_nl_sentences(triples, self.nl_dir)

  def _generate_svg_hierarchy(self):
    if self.mode == RunMode.MAIN_DC:
      logging.info("Hierarchy generation not supported for main dc, skipping.")
      return
    if not self.config.generate_hierarchy():
      logging.info("Hierarchy generation not enabled, skipping.")
      return

    logging.info("Generating SVG hierarchy.")
    sv_triples = self.db.select_triples_by_subject_type(
        sc.TYPE_STATISTICAL_VARIABLE)
    if not sv_triples:
      logging.info("No SV triples found, skipping SVG generating hierarchy.")
    logging.info("Generating SVG hierarchy for %s SV triples.", len(sv_triples))

    vertical_specs: list[VerticalSpec] = []
    vertical_specs_file = self.special_files.get(
        constants.VERTICAL_SPECS_FILE_TYPE)
    if vertical_specs_file:
      logging.info("Loading vertical specs from: %s",
                   vertical_specs_file.name())
      vertical_specs = stat_var_hierarchy_generator.load_vertical_specs(
          vertical_specs_file.read())

    # Collect all dcids that can be used to generate SVG names and get their schema names.
    schema_dcids = list(
        self._triples_dcids(sv_triples) |
        self._vertical_specs_dcids(vertical_specs))
    dcid2name = schema.get_schema_names(schema_dcids, self.db)

    sv_hierarchy_result = stat_var_hierarchy_generator.generate(
        triples=sv_triples, vertical_specs=vertical_specs, dcid2name=dcid2name)
    self.svg_specialized_names = sv_hierarchy_result.svg_specialized_names
    logging.info("Inserting %s SVG triples into DB.",
                 len(sv_hierarchy_result.svg_triples))
    self.db.insert_triples(sv_hierarchy_result.svg_triples)

  # Returns all unique predicates and object ids from the specified triples.
  def _triples_dcids(self, triples: list[Triple]) -> set[str]:
    dcids: set[str] = set()
    for triple in triples:
      if triple.predicate and triple.object_id:
        dcids.add(triple.predicate)
        dcids.add(triple.object_id)
    return dcids

  # Returns all unique pop types and verticals from the specified vertical specs.
  def _vertical_specs_dcids(self,
                            vertical_specs: list[VerticalSpec]) -> set[str]:
    dcids: set[str] = set()
    for vertical_spec in vertical_specs:
      if vertical_spec.population_type:
        dcids.add(vertical_spec.population_type)
      dcids.update(vertical_spec.verticals)
    return dcids

  def _generate_svg_cache(self):
    generate_svg_cache(self.db, self.svg_specialized_names)

  def _check_if_special_file(self, file: File) -> None:
    for file_type in self.special_file_names_by_type.keys():
      if file_type in self.special_files:
        # Already found this special file.
        continue
      file_name = self.special_file_names_by_type[file_type]
      if file.match([file_name]):
        self.special_files[file_type] = file

  def _run_all_data_imports(self):
    input_files: list[File] = []
    input_csv_files: list[File] = []
    input_mcf_files: list[File] = []

    for input_store in self.input_stores:
      if input_store.isdir():
        input_files.extend(input_store.as_dir().all_files())
      else:
        input_files.append(input_store.as_file())

    for input_file in input_files:
      self._check_if_special_file(input_file)
      if input_file.match(["*.csv"]):
        input_csv_files.append(input_file)
      if input_file.match(["*.mcf"]):
        input_mcf_files.append(input_file)

    # Sort input files alphabetically.
    input_csv_files.sort(key=lambda f: f.name())
    input_mcf_files.sort(key=lambda f: f.name())

    self.reporter.report_started(import_files=list(input_csv_files +
                                                   input_mcf_files))
    for input_csv_file in input_csv_files:
      self._run_single_import(input_csv_file)
    for input_mcf_file in input_mcf_files:
      self._run_single_mcf_import(input_mcf_file)

  def _run_single_import(self, input_file: File):
    logging.info("Importing file: %s", input_file.name())
    self._create_importer(input_file).do_import()

  def _run_single_mcf_import(self, input_mcf_file: File):
    logging.info("Importing MCF file: %s", input_mcf_file.name())
    self._create_mcf_importer(input_mcf_file, self.output_dir,
                              self.mode == RunMode.MAIN_DC).do_import()

  def _create_mcf_importer(self, input_file: File, output_dir: Dir,
                           is_main_dc: bool) -> Importer:
    # TODO Detect collisions
    output_file = output_dir.open_file(input_file.path)
    reporter = self.reporter.get_file_reporter(input_file)
    return McfImporter(input_file=input_file,
                       output_file=output_file,
                       db=self.db,
                       reporter=reporter,
                       is_main_dc=is_main_dc)

  def _create_importer(self, input_file: File) -> Importer:
    import_type = self.config.import_type(input_file)
    sanitized_path = input_file.full_path().replace("://",
                                                    "_").replace("/", "_")
    debug_resolve_file = self.process_dir.open_file(
        f"{constants.DEBUG_RESOLVE_FILE_NAME_PREFIX}_{sanitized_path}")
    reporter = self.reporter.get_file_reporter(input_file)

    if import_type == ImportType.OBSERVATIONS:
      input_file_format = self.config.format(input_file)
      if input_file_format == InputFileFormat.VARIABLE_PER_ROW:
        return VariablePerRowImporter(input_file=input_file,
                                      db=self.db,
                                      reporter=reporter,
                                      nodes=self.nodes)
      return ObservationsImporter(input_file=input_file,
                                  db=self.db,
                                  debug_resolve_file=debug_resolve_file,
                                  reporter=reporter,
                                  nodes=self.nodes)

    if import_type == ImportType.EVENTS:
      return EventsImporter(input_file=input_file,
                            db=self.db,
                            debug_resolve_file=debug_resolve_file,
                            reporter=reporter,
                            nodes=self.nodes)

    if import_type == ImportType.ENTITIES:
      return EntitiesImporter(input_file=input_file,
                              db=self.db,
                              reporter=reporter,
                              nodes=self.nodes)

    raise ValueError(
        f"Unsupported import type: {import_type} ({input_file.full_path()})")
