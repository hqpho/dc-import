# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A thin wrapper on PyFilesystem for working with files and directories
in a platform-agnostic way."""

import io

import fs
import fs.base
import fs.path as fspath

_GCS_PATH_PREFIX = "gs://"


class _FSWrapper():

  def __init__(self, fs: fs.base.FS, parent_path: str):
    # TODO Maybe put Store here instead?
    self.fs = fs
    self.parent_path = parent_path


class File(_FSWrapper):

  def __init__(self, fs: fs.base.FS, path: str, parent_path: str,
               create_if_missing: bool):
    super().__init__(fs, parent_path)
    self.path = path
    if not self.fs.exists(self.path):
      if create_if_missing:
        if not self.fs.isdir(fspath.dirname(path)):
          self.fs.makedirs(fspath.dirname(path))
        self.fs.touch(path)
      else:
        raise FileNotFoundError(f"File not found: {path}")

  def __str__(self) -> str:
    return self.path

  def name(self) -> str:
    return fs.path.basename(self.path)

  def full_path(self) -> str:
    return fspath.join(self.parent_path, self.path)

  def match(self, patterns: list[str]) -> bool:
    return self.fs.match(patterns, self.path)

  def read(self) -> str:
    return self.fs.readtext(self.path)

  def write(self, content: str) -> None:
    self.fs.writetext(self.path, content)

  def read_bytes(self) -> bytes:
    return self.fs.readbytes(self.path)

  def write_bytes(self, content: bytes) -> None:
    self.fs.writebytes(self.path, content)

  def read_string_io(self) -> io.StringIO:
    return io.StringIO(self.read())

  def open(self) -> io.IOBase:
    return self.fs.open(self.path, "w")

  def copy_to(self, dest: "File"):
    dest.writebytes(self.readbytes())


class Dir(_FSWrapper):

  def __init__(self, fs: fs.base.FS, parent_path: str):
    super().__init__(fs, parent_path)

  def full_path(self) -> str:
    return self.parent_path

  def open_dir(self, path: str) -> "Dir":
    # TODO make create_if_missing consistent
    if not self.fs.exists(path):
      self.fs.makedirs(path)
    if not self.fs.isdir(path):
      raise ValueError(f"{self.full_path()} exists and is not a directory")
    return Dir(self.fs.opendir(path),
               parent_path=fspath.join(self.full_path(), path))

  def open_file(self, path: str, create_if_missing: bool = True) -> File:
    if self.fs.isdir(path):
      raise ValueError(
          f"{self.full_path()} exists and is a directory, not a file")
    return File(self.fs, path, self.parent_path, create_if_missing)

  def all_files(self):
    files = []
    for abspath in self.fs.walk.files():
      files.append(self.open_file(fspath.relpath(abspath)))
    return files

  # def find_matching(self, patterns: list[str]) -> list[File]:
  #   files = []
  #   for path in self.fs.walk.files():
  #     if self.fs.match_glob(patterns, path):
  #       files.append(self.open_file(path))
  #   return files


class Store:

  def __init__(self, path: str, create_if_missing: bool, treat_as_file: bool):

    self.path = path

    if not treat_as_file:
      try:
        self.fs = fs.open_fs(path, create=create_if_missing)
        self._dir = Dir(self.fs, parent_path=self.path)
        self._isdir = True
      except fs.errors.CreateFailed:
        # Fall back to treating the path as a file path.
        treat_as_file = True

    if treat_as_file:
      self._isdir = False
      dir_path = fspath.dirname(path)
      file_name = fspath.basename(path)
      self.fs = fs.open_fs(dir_path, create=create_if_missing)
      self._file = File(self.fs,
                        file_name,
                        parent_path=dir_path,
                        create_if_missing=create_if_missing)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close()

  def close(self) -> None:
    self.fs.close()

  def isdir(self) -> bool:
    return self._isdir

  def as_dir(self) -> Dir:
    return self._dir

  def as_file(self) -> File:
    return self._file

  def is_high_latency(self) -> bool:
    return self.path.startswith(_GCS_PATH_PREFIX)


def create_store(path: str,
                 create_if_missing: bool = False,
                 treat_as_file: bool = False) -> Store:
  return Store(path, create_if_missing, treat_as_file)
