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

"""Generic implementation of a double index from string values to numeric IDs.
"""

import logging

from taba.server.storage import util
from taba.util.ttl_heartbeat_cache import TtlHeartbeatCache

LOG = logging.getLogger(__name__)

# Patterns for Redis keys.
_KEY_PATTERN_FORWARD = '%sva-id'
_KEY_PATTERN_REVERSE = '%sid-va'
_KEY_PATTERN_NEXT_ID = '%snxid'

# Number of retries on sensitive DB operations in case of failures.
_DB_ERROR_RETRIES = 3

# Constants for the global write lock.
_LOCK_NAME_PATTERN = '%s_lock'
_LOCK_DURATION = 0.010

class DoubleIndexStorageManager(object):
  """Generic implementation of a double index from string values to numeric IDs.

  Maintains three keys in the DB:
    HASH: {Value => ID}
    HASH: {ID => Value}
    INT: Next ID

  When new values are added, the Next ID is incremented to create an ID for that
  value. Note that IDs are not guaranteed to be contiguous. A global write lock
  is employed to manage concurrent clients and multiple DB shards.

  Each mapping is cached locally for a finite period.
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
    self.forward_key = _KEY_PATTERN_FORWARD % self.key_prefix
    self.reverse_key = _KEY_PATTERN_REVERSE % self.key_prefix
    self.counter_key = _KEY_PATTERN_NEXT_ID % self.key_prefix
    self.write_lock_name = _LOCK_NAME_PATTERN % self.key_prefix

    self.cache_ttl = cache_ttl
    self.value_to_id_cache = TtlHeartbeatCache(self.cache_ttl)
    self.id_to_value_cache = TtlHeartbeatCache(self.cache_ttl)

  def GetIdsForValues(self, values, create_if_new=False, from_cache=True):
    """Retrieve the IDs for a set of Values. If any Values are not known and
    create_if_new is True, new ID will be created.

    Args:
      values - Values to lookup the IDs for.
      create_if_new - If True, any Values for which there are no IDs in the DB
          will be assigned new IDs.
      from_cache - If True, look for the Values in the local cache first.

    Returns:
      Operation object. On success, the response_value contains a mapping of
      {Value: ID} for all the requested Values.
    """
    vids = {}
    values_to_find = set(values)

    # Lookup any values that can be found in the cache.
    if from_cache:
      for value in list(values_to_find):
        if value in self.value_to_id_cache:
          vids[value] = self.value_to_id_cache[value]
          values_to_find.remove(value)

    if len(values_to_find) == 0:
      return util.Operation(success=True, response_value=vids)

    # For any values not found in the cache, look them up in the DB.
    values_to_find_db = list(values_to_find)
    op = util.CheckedOp('looking up IDs',
        self.engine.HashMultiGet,
        self.forward_key, values_to_find_db)

    if not op.success:
      return op

    if op.response_value:
      for value, vid in op.response_value.iteritems():
        if vid:
          vid = int(vid)

          vids[value] = vid
          values_to_find.remove(value)

          self.value_to_id_cache[value] = vid
          self.id_to_value_cache[vid] = value

    if len(values_to_find) == 0:
      return util.Operation(success=True, response_value=vids)

    # For any values not found in the DB, create new IDs for them, if enabled.
    if create_if_new:
      for value in list(values_to_find):
        new_vid = self._CreateIdForValue(value)
        if new_vid is not None:
          vids[value] = new_vid
          values_to_find.remove(value)

    if len(values_to_find) != 0:
      for value in values_to_find:
        vids[value] = None

    return util.Operation(success=True, response_value=vids)

  def GetValuesForIds(self, vids, from_cache=True):
    """Retrieve the Values for a set of IDs.

    Args:
      vids - IDs to retrieve the Values for.
      from_cache - If True, look for in the local cache first.

    Returns:
      Operation object. On success, the response_value contains a mapping of
      {ID: Value} for all the requested IDs.
    """
    values = {}
    vids_to_find = set(map(int, vids))

    # Lookup any values that can be found in the cache.
    if from_cache:
      for vid in list(vids_to_find):
        value = self.id_to_value_cache[vid]
        if value is not None:
          values[vid] = value
          vids_to_find.remove(vid)

    if len(vids_to_find) == 0:
      return util.Operation(success=True, response_value=values)

    # For any values not found in the cache, look them up in the DB.
    vids_to_find_db = list(vids_to_find)
    op = util.CheckedOp('looking up values for IDs',
        self.engine.HashMultiGet,
        self.reverse_key, vids_to_find_db)

    if op.success and op.response_value:
      for vid, value in op.response_value.iteritems():
        if value:
          values[vid] = value
          vids_to_find.remove(vid)

          self.value_to_id_cache[value] = int(vid)
          self.id_to_value_cache[vid] = value

    if len(vids_to_find) != 0:
      for vid in vids_to_find:
        values[vid] = None

    return util.Operation(success=True, response_value=values)

  def GetAllValueIdMap(self):
    """Retrieve the full {Value: ID} mapping. This does _not_ pull from the
    local cache (but will add the results to the cache).

    Returns:
      Operation object, with the {Value: ID} dict in the response_value.
    """
    op = util.CheckedOp('retrieving entire Value <=> ID map',
        self.engine.HashGetAll,
        self.forward_key)

    if op.success:
      # Parse the string IDs into integers.
      op.response_value = dict(
          [(k, int(v)) for k, v in op.response_value.iteritems()])

      # Update the local cache.
      for value, vid in op.response_value.iteritems():
        self.value_to_id_cache[value] = vid
        self.id_to_value_cache[vid] = value

    return op

  def GetAllValues(self):
    """Retrieve the collection of all values. This does _not_ pull from the
    local cache (but will add the results to the cache).

    Returns:
      Operation object, with the collection of values in the response_value.
    """
    op = self.GetAllValueIdMap()
    if op.success and op.response_value:
      op.response_value = op.response_value.keys()

    return op

  def GetAllIds(self):
    """Retrieve the collection of all ids. This does _not_ pull from the
    local cache (but will add the results to the cache).

    Returns:
      Operation object, with the collection of values in the response_value.
    """
    op = self.GetAllValueIdMap()
    if op.success and op.response_value:
      op.response_value = op.response_value.values()

    return op

  def AddValue(self, value, from_cache=True):
    """Add a set of Values to the mapping. If any Values already have IDs, they
    are not modified.

    Args:
      value - Value to create ID for.
      from_cache - If True, checks the local cache for existence of the value.

    Returns:
      Operation object.
    """
    return self.GetIdsForValues(
        [value],
        create_if_new=True,
        from_cache=from_cache)

  def RemoveValue(self, value):
    """Delete a value from the mapping.

    WARNING: Make sure the Value and ID are not references anywhere else in the
    DB before deleting!

    NOTE: IDs are never reused, even after being deleted.

    Args:
      value - Value to remove from the mapping.

    Returns:
      Operation object.
    """
    # Check whether the value exists in the first place.
    op = self.GetIdsForValues([value], create_if_new=False)
    if not op or not op.response_value or not op.response_value.get(value):
      return util.Operation(success=True)

    vid = op.response_value.get(value)

    for _ in xrange(_DB_ERROR_RETRIES):
      # Delete the value from the mapping.
      op1 = self.engine.HashDelete(self.forward_key, value)
      op2 = self.engine.HashDelete(self.reverse_key, vid)

      # Check if the operation succeeded, and if so break out of the loop.
      if all([op1.success, op2.success]):
        op = util.CompoundOperation(op1, op2)
        break

    else:
      # Fell off the loop normally, which means the operations failed.
      LOG.error('Error removing mapping {%s: %s}' % (value, vid))
      op = util.Operation(success=False)

    del self.value_to_id_cache[value]
    del self.id_to_value_cache[vid]

    return op

  def _CreateIdForValue(self, value):
    """Generate and set an ID for a new value. Note that this acquires a global
    lock.

    Args:
      value - New value to generate an ID for and put in the indexes.

    Returns:
      New (or existing) ID for the value.
    """
    # Get the next ID.
    op = util.CheckedOp('creating new ID for value % s' % value,
        self.engine.Increment,
        self.counter_key)

    if not op.success or op.response_value is None or op.response_value == '':
      return None

    vid = int(op.response_value)

    # Acquire the global lock for this table before proceeding, to ensure that
    # no other client modifies the map between checking whether the mapping
    # exists and writing the value. (Note: we use a global lock and not Redis
    # transactions, since this operation reaches across Redis shards).
    with self.engine.Lock(self.write_lock_name, _LOCK_DURATION):

      op = self.engine.HashGet(self.forward_key, value)
      if not op.success:
        raise Exception('Error retrieving ID mapping for "%s"' % value)

      if op.response_value:
        vid = int(op.response_value)

      else:
        op1 = self.engine.HashPut(self.forward_key, value, vid)
        op2 = self.engine.HashPut(self.reverse_key, vid, value)

        if not (op1.success and op2.success):

          # Attempt to remove a partially set mapping.
          if op1.success and not op2.success:
            self.engine.HashDelete(self.forward_key, value)
          elif op2.success and not op1.success:
            self.engine.HashDelete(self.reverse_key, vid)

          raise Exception('Error setting mapping %s <==> %s.' % (value, vid))

    # Put the final ID in the local cache.
    self.value_to_id_cache[value] = vid
    self.id_to_value_cache[vid] = value

    return vid

  def SelfCheck(self, dry=True):
    """Verify the consistency of the forward and reverse mappings, and
    optionally delete any inconsistencies.

    Args:
      dry - Whether this is a dry run. If False, inconsistencies will be
          deleted.
    """
    forward_op = util.StrictOp('looking up IDs',
        self.engine.HashGetAll,
        self.forward_key)
    forward = forward_op.response_value

    reverse_op = util.StrictOp('looking up IDs',
        self.engine.HashGetAll,
        self.reverse_key)
    reverse = reverse_op.response_value

    forward_removals = set()
    reverse_removals = set()

    for fkey, fval in forward.iteritems():
      # Check that the forward mapping has a corresponding entry in the reverse.
      if fval not in reverse:
        LOG.info('Missing reverse mapping F(%s > %s)' % (fkey, fval))
        forward_removals.add(fkey)

      # Check that the reverse mapping is the same as the forward one.
      elif reverse[fval] != fkey:
        LOG.info('Incorrect mapping F(%s > %s), R(%s > %s)' %
            (fkey, fval, fval, reverse[fval]))
        forward_removals.add(fkey)
        reverse_removals.add(fval)

    for rkey, rval in reverse.iteritems():
      # Check that the reverse mapping has a corresponding entry in the forward.
      if rval not in forward:
        LOG.info('Missing forward mapping R(%s > %s)' % (rkey, rval))
        reverse_removals.add(rkey)

    # Report and possibly remove bad mappings.
    if len(forward_removals) > 0:
      LOG.info('Bad Forward Mappings: %s' % ', '.join(forward_removals))
      if not dry:
        op = util.CheckedOp('Removing bad forward mappings',
            self.engine.HashBatchDelete,
            self.forward_key, list(forward_removals))
        LOG.info(str(op))

    if len(reverse_removals) > 0:
      LOG.info('Bad Reverse Mappings: %s' % ', '.join(reverse_removals))
      if not dry:
        op = util.CheckedOp('Removing bad reverse mappings',
            self.engine.HashBatchDelete,
            self.reverse_key, list(reverse_removals))
        LOG.info(str(op))
