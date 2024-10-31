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

import json
import os
import shutil
import tempfile
import unittest

from stats.config import Config
from stats.db import create_and_update_db
from stats.db import create_sqlite_config
from stats.events_importer import EventsImporter
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import write_observations
from tests.stats.test_util import write_triples
from util.filesystem import create_store

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "events_importer")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _test_import(test: unittest.TestCase, test_name: str):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_store = create_store(_INPUT_DIR)
    temp_store = create_store(temp_dir)

    input_file_name = f"{test_name}.csv"
    input_file = input_store.as_dir().open_file(input_file_name,
                                                create_if_missing=False)
    input_config_file = input_store.as_dir().open_file("config.json",
                                                       create_if_missing=False)
    db_file_name = f"{test_name}.db"
    db_path = os.path.join(temp_dir, db_file_name)
    db_file = temp_store.as_dir().open_file(db_file_name)

    output_triples_path = os.path.join(temp_dir, f"{test_name}.triples.db.csv")
    expected_triples_path = os.path.join(_EXPECTED_DIR,
                                         f"{test_name}.triples.db.csv")
    output_observations_path = os.path.join(temp_dir,
                                            f"{test_name}.observations.db.csv")
    expected_observations_path = os.path.join(
        _EXPECTED_DIR, f"{test_name}.observations.db.csv")

    config = Config(data=json.loads(input_config_file.read()))
    nodes = Nodes(config)

    db = create_and_update_db(create_sqlite_config(db_file))
    debug_resolve_file = temp_store.as_dir().open_file("debug.csv")
    report_file = temp_store.as_dir().open_file("report.json")
    reporter = FileImportReporter(input_file.full_path(),
                                  ImportReporter(report_file))

    EventsImporter(input_file=input_file,
                   db=db,
                   debug_resolve_file=debug_resolve_file,
                   reporter=reporter,
                   nodes=nodes).do_import()
    db.insert_triples(nodes.triples())
    db.commit_and_close()

    write_triples(db_path, output_triples_path)
    write_observations(db_path, output_observations_path)

    if is_write_mode():
      shutil.copy(output_triples_path, expected_triples_path)
      shutil.copy(output_observations_path, expected_observations_path)
      return

    compare_files(test, output_triples_path, expected_triples_path)
    compare_files(test, output_observations_path, expected_observations_path)

    input_store.close()
    temp_store.close()


class TestEventsImporter(unittest.TestCase):

  def test_countryalpha3codes(self):
    _test_import(self, "countryalpha3codes")

  def test_idcolumns(self):
    _test_import(self, "idcolumns")
