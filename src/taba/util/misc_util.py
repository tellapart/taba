# Copyright 2014 TellApart, Inc.
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

"""Miscellaneous utilities.
"""

from cStringIO import StringIO
import functools
import itertools
import os
import zlib

import cjson

def StreamingDecompressor(input_stream, chunk_size=1024):
  """Generator which takes a file-like stream of zlib-compressed data, and
  yields the uncompressed plain text results line-by-line.

  Args:
    input_stream - File-like object containing zlib compressed data.
    chunk_size - Number of bytes to read from the stream at each pass.
  """
  remnant = None
  d = zlib.decompressobj()

  input_chunk = input_stream.read(chunk_size)
  while input_chunk:
    decompressed = StringIO(d.decompress(input_chunk))

    for line in decompressed:
      if line[-1] == '\n':
        # A full line was pulled from the decompressed buffer. Prepend any
        # remnant from the previous block, and yield the line.
        if remnant is not None:
          line = '%s%s' % (remnant, line)
          remnant = None

        yield line

      else:
        # A partial line was pulled from the decompressed buffer. This means
        # we're at the end of the chunk. Save the remnant (prepending and
        # existing remnant), and continue to the next chunk.
        if remnant is not None:
          remnant = '%s%s' % (remnant, line)
        else:
          remnant = line

    input_chunk = input_stream.read(chunk_size)

  if remnant is not None:
    yield remnant

def DecodeSettingsFile(settings_path):
  """Load and parse a settings file.

  Args:
    settings_file - Path to the file containing the settings data.

  Returns:
    Decoded dictionary of settings data.
  """
  # Read in the settings data.
  s = open(settings_path)
  data = s.read()

  if not data:
    raise Exception('No settings data to extract')

  # Decode the settings data.
  settings = cjson.decode(data)

  return settings

def BootstrapCython(id, base_dir='~'):
  """Bootstrap Cython compilation, with a process-specific build directory. The
  specified id will bed used to uniquely identify this process. Using ids that
  are durable across restarts makes the bootstrapping process more efficient.

  Args:
    id - ID unique to this process.
  """
  import shutil

  pyx_build_dir = os.path.expanduser('%s/.pyxbld-%s' % (base_dir, id))

  # Clean up stale Cython pyximport state.
  if os.path.exists(pyx_build_dir):
    shutil.rmtree(pyx_build_dir)

  os.makedirs(pyx_build_dir)

  import pyximport
  pyximport.install(build_dir=pyx_build_dir)

def Slicer(iterable, slice_size):
  """Split an iterable into smaller iterables of of a maximum size.

  NOTE: Any occurrence of None in the iterable will be skipped over.

  Args:
    iterable - Iterable to slice.
    slice_size - Maximum number of elements per output iterable.

  Returns:
    Iterable of iterables.
      e.g. Slicer([1,2,3,4,5,6,7], 3) => [[1,2,3], [4,5,6], [7]]
  """
  args = [iter(iterable)] * slice_size
  return itertools.imap(
      functools.partial(filter, lambda x: x is not None),
      itertools.izip_longest(*args))

class Aggregator(object):
  """Callable helper object that sums a stream of numbers.
  """

  def __init__(self):
    self.value = 0

  def __call__(self, new_value):
    self.value += new_value
