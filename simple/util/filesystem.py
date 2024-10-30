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

_GCS_PATH_PREFIX = "gs://"


class _FSWrapper():

  def __init__(self, fs: fs.base.FS):
    self.fs = fs


class File(_FSWrapper):

  def __init__(self,
               fs: fs.base.FS,
               path: str,
               create_if_missing: bool = False):
    super.__init__(self, fs)
    self.path = path
    if not self.fs.exists(self.path):
      if create_if_missing:
        self.fs.touch(path)
      else:
        raise FileNotFoundError(f"File not found: ${path}")

  def name(self) -> str:
    return fs.path.basename(self.path)

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

  def openbin(self) -> io.IOBase:
    return self.fs.openbin(self.path)


class Dir(_FSWrapper):

  def __init__(self, fs: fs.base.FS):
    super.__init__(self, fs)

  # def exists(self, path: str) -> bool:
  #   return self.fs.exists(path)

  def open_dir(self, path: str) -> "Dir":
    # TODO make create_if_missing consistent
    if not self.fs.exists(path):
      self.fs.makedirs(path)
    if not self.fs.isdir(path):
      raise ValueError(f"{path} exists and is not a directory")
    return Dir(self.fs.opendir(path))

  def open_file(self, path: str, create_if_missing: bool = False) -> File:
    if self.fs.isdir(path):
      raise ValueError(f"{path} exists and is a directory, not a file")
    return File(self.fs, path, create_if_missing)

  def find_by_extension(self, extension: str) -> list[File]:
    files = []
    for path in self.fs.walk.files():
      if path.endswith(extension):
        files.append(self.open_file(path))
    return files


class Store:

  def __init__(self, path: str, create_if_missing: bool):
    self.fs = fs.open_fs(path, create=create_if_missing)
    self.path = path

  def __exit__(self):
    self.close()

  def close(self) -> None:
    self.fs.close()

  def isdir(self) -> bool:
    return self.fs.isdir(".")

  def as_dir(self) -> Dir:
    return Dir(self.fs)

  def as_file(self, create_if_missing: bool = False) -> File:
    return File(self.fs, create_if_missing)

  def is_high_latency(self) -> bool:
    return self.path.startswith(_GCS_PATH_PREFIX)


def create_store(path: str, create_if_missing: bool = False) -> Store:
  return Store(path, create_if_missing)
