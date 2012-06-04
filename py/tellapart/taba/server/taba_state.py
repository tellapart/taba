# Copyright 2012 TellApart, Inc.
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

"""
Utilities for dealing with State objects.
"""

import cPickle as pickle
import ctypes
import zlib

try:
  import snappy
  use_snappy = True
except ImportError:
  use_snappy = False

STATE_MARK_ZIP = 78
STATE_MARK_SNAPPY = 65
STATE_MARK_LIGHT = 21

class StateStruct(object):
  """Object for holding a State object and associated meta-data.
  """

  def __init__(self, payload, version):
    """
    Args:
      payload - A binary serialized State object.
      version - Handler specific State version number.
    """
    self.payload = payload
    self.version = version

  def __eq__(self, other):
    return \
        hasattr(other, 'payload') and other.payload == self.payload and \
        hasattr(other, 'version') and other.version == self.version

class StateMeta(ctypes.Structure):
  """Structure which can serialize/deserialize itself to a binary
  representation. Holds State meta-data prepended to packed objects.
  """
  _fields_ = [
      ('mark', ctypes.c_byte),
      ('checksum', ctypes.c_int32)]

  def Serialize(self):
    """Serialize to a buffer.

    Returns:
      A contiguous buffer containing the binary representation of the Structure.
    """
    return buffer(self)[:]

  def Deserialize(self, bytes):
    """Read a binary buffer into the Structure object.

    Args:
      bytes - Buffer containing the binary representation of the Structure,
    """
    ctypes.memmove(ctypes.addressof(self), bytes, self.Size())

  def Size(self):
    """Return the size, in bytes, of this Struct.

    Returns:
      Size of this structure in bytes
    """
    return ctypes.sizeof(self)

def PackState(state_struct):
  """Convert a StateStruct into a packed, compressed binary string with embedded
  metadata.

  Args:
    state_struct - A populated StateStruct object.

  Returns:
    Binary string of the packed state ready for persisting in the database.
  """
  state_serialized = pickle.dumps(state_struct, protocol=2)

  if use_snappy:
    state_compressed = snappy.compress(state_serialized)
    mark = STATE_MARK_SNAPPY

  else:
    state_compressed = zlib.compress(state_serialized, 2)
    mark = STATE_MARK_ZIP

  checksum = zlib.adler32(state_compressed)
  meta = StateMeta(mark=mark, checksum=checksum)

  return ''.join([meta.Serialize(), state_compressed])

def PackStateLight(state_struct):
  """Like PackState(), except without compression or checksum. Useful for
  serializing StateStruct objects in a compound State.

  Args:
    state_struct - A populated StateStruct object.

  Returns:
    Binary string of the packed State.
  """
  state_serialized = pickle.dumps(state_struct, protocol=2)

  meta = StateMeta(mark=STATE_MARK_LIGHT, checksum=0)

  return ''.join([meta.Serialize(), state_serialized])

def UnpackState(packed_state):
  """Convert a packed State binary string into a StateStruct object. If the
  input doesn't have the STATE_MARK_ZIP prefix, it is assumed to be an old-style
  compressed state object, and is directly decompressed.

  Args:
    packed_state - Binary string of the type produces by PackState.

  Returns:
    Populated StateStruct object.
  """
  if not packed_state:
    return None

  if ord(packed_state[0]) == STATE_MARK_ZIP:
    # Extract the meta-data Struct from the packed data.
    meta = StateMeta()
    meta.Deserialize(packed_state)

    # Extract the compressed State from the packed data.
    compressed_state = packed_state[meta.Size():]

    # Compute the checksum and make sure it matches the metadata.
    cksum = zlib.adler32(compressed_state)
    if cksum != meta.checksum:
      raise ValueError('Compressed State Checksum Error')

    # Return the decompressed State.
    return pickle.loads(zlib.decompress(compressed_state))

  elif ord(packed_state[0]) == STATE_MARK_SNAPPY:
    # Extract the meta-data Struct from the packed data.
    meta = StateMeta()
    meta.Deserialize(packed_state)

    # Extract the compressed State from the packed data.
    compressed_state = packed_state[meta.Size():]

    # Compute the checksum and make sure it matches the metadata.
    cksum = zlib.adler32(compressed_state)
    if cksum != meta.checksum:
      raise ValueError('Compressed State Checksum Error')

    # Return the decompressed State.
    return pickle.loads(snappy.decompress(compressed_state))

  elif ord(packed_state[0]) == STATE_MARK_LIGHT:
    # Extract the meta-data Struct from the packed data.
    meta = StateMeta()
    meta.Deserialize(packed_state)

    # Extract the State buffer from the packed data.
    state_buffer = packed_state[meta.Size():]

    # Return the decompressed State.
    return pickle.load(state_buffer)

  else:
    # Unsupported format.
    raise ValueError('Unrecognized State serialization format')
