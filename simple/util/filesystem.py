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

import fs


class _SubFSWrapper():

  def __init__(self, fs: fs.subfs.SubFS):
    self.fs = fs


class File(_SubFSWrapper):

  def __init__(self, fs: fs.subfs.SubFS, path: str):
    super.__init__(self, fs)
    self.path = path

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


class Dir(_SubFSWrapper):

  def __init__(self, fs: fs.subfs.SubFS):
    super.__init__(self, fs)

  # def exists(self, path: str) -> bool:
  #   return self.fs.exists(path)

  def open_dir(self, path: str) -> "Dir":
    if not self.fs.exists(path):
      self.fs.makedirs(path)
    if not self.fs.isdir(path):
      raise ValueError(f"{path} exists and is not a directory")
    return Dir(self.fs.opendir(path))

  def open_file(self, path: str) -> File:
    if not self.fs.exists(self.path):
      self.fs.touch(path)
    if self.fs.isdir(path):
      raise ValueError(f"{path} exists and is a directory, not a file")
    return File(self.fs, path)

  def find_by_extension(self, extension: str) -> list[File]:
    files = []
    for path in self.fs.walk.files():
      if path.endswith(extension):
        files.append(self.open_file(path))
    return files


class Store:

  def __init__(self, path: str):
    self.fs = fs.open_fs(path)
    self.path = path

  def close(self) -> None:
    self.fs.close()

  def as_dir(self) -> Dir:
    subfs = self.fs.opendir(".")
    return Dir(subfs)

  def open_dir(self, path: str) -> Dir:
    return self.as_dir.open_dir(path)

  def open_file(self, path: str) -> File:
    return self.as_dir.open_file(path)


def create_store(path: str) -> Store:
  return Store(path)
