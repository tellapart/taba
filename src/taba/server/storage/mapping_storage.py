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

"""Generic implementation of a simple mapping backed by a Redis Hash.
"""

import logging

from taba.server.storage import util
from taba.util.ttl_heartbeat_cache import TtlHeartbeatCache

LOG = logging.getLogger(__name__)

# Pattern for Redis key.
_KEY_PATTERN_MAP = '%smap'

class MappingStorageManager(object):
  """Generic implementation of an index. Each mapping is cached locally for a
  finite period.
  """

  def __init__(self,
      engine,
      key_prefix,
      cache_ttl):
    """
    Args:
      engine - Storage engine compatible with RedisEngine.
      key_prefix - Unique prefix to use on Redis keys for the index.
      cache_ttl - Seconds for which values will be cached locally.
    """
    self.engine = engine

    self.key_prefix = key_prefix
    self.map_key = _KEY_PATTERN_MAP % key_prefix

    self.cache_ttl = cache_ttl
    self.cache = TtlHeartbeatCache(self.cache_ttl)

  def GetBatch(self, keys, from_cache=True):
    """Retrieve the values for a list of keys.

    Args:
      keys - Iterable of keys to lookup the values of.
      from_cache - If True, look for the keys in the local cache, and return
          that values that exist there.

    Returns:
      Operation object, with the value in the response_value.
    """
    values = {}
    keys_to_find = set(filter(None, keys))

    # If enabled, look for the keys in the local cache.
    if from_cache:
      for key in keys:
        if key in self.cache:
          values[key] = self.cache[key]
          keys_to_find.remove(key)

    if len(keys_to_find) == 0:
      return util.Operation(success=True, response_value=values)

    # Some values are missing from the local cache. Look for them in the DB.
    keys_to_find_db = list(keys_to_find)
    op = util.CheckedOp('looking up keys',
        self.engine.HashMultiGet,
        self.map_key, keys_to_find_db)

    if op.success and op.response_value is not None:
      for key, value in op.response_value.iteritems():
        if value:
          values[key] = value
          keys_to_find.remove(key)

          self.cache[key] = value

    # Fill out any keys not found in the DB with None.
    if len(keys_to_find) != 0:
      for key in keys_to_find:
        values[key] = None

    return util.Operation(success=True, response_value=values)

  def GetAll(self):
    """Retrieve the full {Key => Value} mapping. This does _not_ pull from the
    local cache (but will add the results to the cache).

    Returns:
     Operation object, with the {Key: Value} dict in the response_value.
    """
    op = util.CheckedOp('retrieving entire map',
        self.engine.HashGetAll,
        self.map_key)

    if op.success and op.response_value is not None:
      for k, v in op.response_value.iteritems():
        self.cache[k] = v

    return op

  def SetBatch(self, key_value_map):
    """Update the mapping in the DB from a dict of values.

    Args:
      key_value_map - Dict to update into the DB.

    Returns:
      Operation object.
    """

    # Clear out any old values from the local cache.
    for key in key_value_map.keys():
      if key in self.cache:
        del self.cache[key]

    # Update the DB.
    op = util.CheckedOp('setting batch',
        self.engine.HashMultiPut,
        self.map_key, key_value_map)

    # Refresh the local cache with the new values.
    if op.success:
      for key, value in key_value_map.iteritems():
        self.cache[key] = value

    return op

  def Remove(self, key):
    """Delete a value from the mapping.

    WARNING: Make sure the Value and ID are not references anywhere else in the
    DB before deleting!

    Args:
      value - Value to remove from the mapping.

    Returns:
      Operation object.
    """
    del self.cache[key]

    op = util.CheckedOp('deleting %s' % key,
        self.engine.HashDelete,
        self.map_key, key)

    return op
