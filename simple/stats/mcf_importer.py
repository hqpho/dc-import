# Copyright 2024 Google Inc.
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

import logging

from kg_util.mcf_parser import mcf_to_triples
import pandas as pd
from stats import constants
from stats.data import RowEntity
from stats.data import Triple
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from util.filesystem import File

_ID = 'ID'
_DCID = 'dcid'

# The max length of a node value.
# We limit it to the max size of a mysql text field.
_MAX_CHARS = 2**16 - 1


class McfImporter(Importer):
  """Imports a MCF file.

  For main DC, the file is simply copied to the output directory.
  For custom DC, the MCF nodes are inserted as triples in the DB.
    """

  def __init__(self, input_file: File, output_file: File, db: Db,
               reporter: FileImportReporter, is_main_dc: bool) -> None:
    self.input_file = input_file
    self.output_file = output_file
    self.db = db
    self.reporter = reporter
    self.is_main_dc = is_main_dc

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      # For main DC, simply copy the file over.
      if self.is_main_dc:
        self.output_file.write(self.input_file.read())
      else:
        triples = self._mcf_to_triples()
        logging.info("Inserting %s triples from %s", len(triples),
                     self.input_file.full_path())
        self.db.insert_triples(triples)

      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def _mcf_to_triples(self) -> list[Triple]:
    parser_triples: list[list[str]] = []
    # DCID references
    local2dcid: dict[str, str] = {}
    for parser_triple in mcf_to_triples(self.input_file.read_string_io()):
      [subject_id, predicate, value, _] = parser_triple
      if predicate == _DCID:
        local2dcid[subject_id] = value
      else:
        parser_triples.append(parser_triple)

    return list(map(lambda x: _to_triple(x, local2dcid), parser_triples))


def _to_triple(parser_triple: list[str], local2dcid: dict[str, str]) -> Triple:
  [subject_id, predicate, value, value_type] = parser_triple
  if len(value) > _MAX_CHARS:
    raise ValueError(
        f"Value of property {predicate} in node {subject_id} too long (got: {len(value)}, max: {_MAX_CHARS})"
    )
  if subject_id not in local2dcid:
    raise ValueError(f"dcid not specified for node: {subject_id}")

  subject_id = local2dcid[subject_id]
  if value_type == _ID:
    return Triple(subject_id, predicate, object_id=value)
  else:
    return Triple(subject_id, predicate, object_value=value)
