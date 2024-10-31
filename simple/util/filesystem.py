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
import re

import fs
import fs.base
import fs.path as fspath
from fs_gcsfs import GCSFS

_GCS_PATH_PREFIX = "gs:"


class _FSWrapper():

  def __init__(self, fs: fs.base.FS, parent_path: str):
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

  def syspath(self) -> str | None:
    if self.fs.hassyspath(self.path):
      return self.fs.getsyspath(self.path)
    return None

  def match(self, pattern: str) -> bool:
    allow_partial_match = not pattern.startswith("/")
    if not pattern.count("*"):
      return (allow_partial_match and
              self.name() == pattern) or self.path == pattern
    else:
      segments = pattern.split("/")
      segments = [s.replace(".", r"\.") for s in segments]
      regex_segments = []
      for segment in segments:
        if segment == "**":
          # Match a variable level of dirs
          regex_segments.append(".*")
        elif segment == "*":
          # Match a single level of dirs
          regex_segments.append(r"[^\/]*")
        else:
          # Single wildcard as part of a pattern can be anything but a slash
          regex_segment = segment.replace("*", r"[^\/]*")
          regex_segments.append(regex_segment)

      regex = (r"\/").join(regex_segments)
      # re.search matches anywhere in the path,
      # while re.match matches at the beginning.
      return (allow_partial_match and
              re.search(regex, self.path) is not None) or re.match(
                  regex, self.path) is not None

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

  def size(self) -> int:
    return self.fs.getsize(self.path)

  def copy_to(self, dest: "File"):
    dest.write_bytes(self.read_bytes())


class Dir(_FSWrapper):

  def __init__(self, fs: fs.base.FS, parent_path: str):
    super().__init__(fs, parent_path)

  def full_path(self) -> str:
    return self.parent_path

  def open_dir(self, path: str) -> "Dir":
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


class Store:

  def __init__(self, path: str, create_if_missing: bool, treat_as_file: bool):

    self.path = path

    if not treat_as_file:
      try:
        self.fs = fs.open_fs(path, create=create_if_missing)
        self._dir = Dir(self.fs, parent_path=self.path)
        self._isdir = True
        dir_path = path
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

    if self.path.startswith(_GCS_PATH_PREFIX):
      _fix_gcsfs_storage(self.fs)

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


def _fix_gcsfs_storage(fs: GCSFS) -> None:
  """Utility function that walks the entire `root_path` and makes sure that all intermediate directories are correctly marked with empty blobs.

  As GCS is no real file system but only a key-value store, there is also no concept of folders. S3FS and GCSFS overcome this limitation by adding
  empty files with the name "<path>/" every time a directory is created, see https://fs-gcsfs.readthedocs.io/en/latest/#limitations.

  This is the same as GCSFS.fix_storage() but with a fix for an infinite loop when root_path does not end with a slash.
  """
  names = [
      blob.name
      for blob in fs.bucket.list_blobs(prefix=fspath.forcedir(fs.root_path))
  ]
  marked_dirs = set()
  all_dirs = set()

  for name in names:
    # If a blob ends with a slash, it's a directory marker
    if name.endswith("/"):
      marked_dirs.add(fspath.dirname(name))

    name = fspath.dirname(name)
    while name != fs.root_path:
      all_dirs.add(name)
      name = fspath.dirname(name)

  if fspath.forcedir(fs.root_path) != "/":
    all_dirs.add(fs.root_path)

  unmarked_dirs = all_dirs.difference(marked_dirs)

  if len(unmarked_dirs) > 0:
    for unmarked_dir in unmarked_dirs:
      dir_name = fspath.forcedir(unmarked_dir)
      blob = fs.bucket.blob(dir_name)
      blob.upload_from_string(b"")
