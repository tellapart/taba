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

"""Mock Storage Engine that exposes a Redis-like interface, and stores all data
in-memory.
"""

import copy
from contextlib import contextmanager
import logging

from taba.server.storage.util import Operation

# The Get batch size. This controls the number of keys which will be retrieved
# per pipelined Get call on a single shard.
GET_BATCH_SIZE = 1000

LOG = logging.getLogger(__name__)

class MemoryRedisEngine(object):
  """Mock Storage Engine that exposes a Redis-like interface, and stores all
  data in local process memory.
  """

  def __init__(self):
    """"""
    self.data = {}

  def ShutDown(self):
    pass

  ###################################################################
  # SIMPLE / STRING / INTEGER
  ###################################################################

  def Get(self, key):
    """Return the value for a given key from the correct shard.

    Args:
      key - The key to lookup.

    Returns:
      CompoundOperation object with the result of the lookup. The response_value
      field contains the value pertaining to the key.
    """
    # Resubmit the Get operation as as Batch Get.
    op = self.BatchGet([key])

    # Unpack the batch operation results.
    if op.success and op.response_value:
      op.response_value = op.response_value[0]

    return op

  def BatchGet(self, keys, batch_size=GET_BATCH_SIZE):
    """Return the values for a set of keys, from across all shards.

    Args:
      keys - List of keys to lookup.
      batch_size - Get keys per shard in batches of this size.

    Returns:
      Operation with the results of the lookups. The response_value
      field has the form [value1, value2, ...], ordered by the list of keys
      that were looked up.
    """
    response_value = []
    for key in keys:
      response_value.append(self.data.get(key, None))

    return Operation(success=True, response_value=response_value)

  def Put(self, key, value, expiration_time=None):
    """Put a value in a key under the correct shard.

    Args:
      key - The key to put.
      value - The value to put.
      expiration_time - If specified, set the key to expire after this many
                        seconds.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field is set to whether or not the Put was successful.
    """
    op = self.BatchPut([(key, value)], expiration_time=expiration_time)

    if op.success and op.response_value:
      op.response_value = op.response_value[0]

    return op

  def BatchPut(self, key_value_tuples, expiration_time=None):
    """Put a batch of values into keys, each under the correct shard.

    Args:
      key_value_tuples - A list of tuples of the form (key, value)
      expiration_time - If specified, set the keys to expire after this many
                        seconds.

    Returns:
      A CompoundOperation with the results of the queries. The response_value
      field is set to a list of boolean values [success1, success2,...],
      ordered by the list of (key, val) tuples that were Put.
    """
    for key, value in key_value_tuples:
      self.data[key] = value

    return Operation(success=True, response_value=[len(key_value_tuples)])

  def PutNotExist(self, key, value):
    """Put a value into a key, only if the value does not already exist.

    Args:
      key - The key to put.
      value - The value to put.

    Returns:
      Operation object. On success, the response_value will contain the number
      of keys modified (i.e. 1 if the key was set, 0 if it already existed)
    """
    if key in self.data:
      result = 0
    else:
      self.data[key] = value
      result = 1

    return Operation(success=True, response_value=result)

  def Increment(self, key):
    """Atomically increment an integer.

    Args:
      key - The key of the integer to increment.

    Returns:
      An Operation object with the result of the query.
    """
    if key in self.data:
      self.data[key] += 1
    else:
      self.data[key] = 1

    return Operation(success=True, response_value=self.data[key])

  def Delete(self, key):
    """Delete a key.

    Args:
      key - The key to delete.

    Returns:s
      An Operation object with the results of the query. The response value
      field is set to whether or not the key was deleted successfully.
    """
    op = self.BatchDelete([key])

    if op.success and op.response_value:
      op.response_value = op.response_value[0]

    return op

  def BatchDelete(self, keys):
    """Delete a set of keys.

    Args:
      keys - A list of keys to delete.

    Returns:
      A CompoundOperation with the results of the queries. The response_value
      field is set to a list of boolean values [success1, success2,...],
      ordered by the list of keys that were deleted.
    """
    results = []
    for key in keys:
      if key in self.data:
        del self.data[key]
        results.append(1)
      else:
        results.append(0)

    return Operation(success=True, response_value=results)

  def BatchCheckAndMultiSet(self, keys, get_updates_fn):
    """Set a batch of keys transactionally using optimistic locking. Each key is
    locked and retrieved from its shard. The retrieved values are passed, one at
    a time, to the callback function. The return value of the callback is then
    put in the key, under the appropriate shard. If the value changed between
    the time it was locked and when the Put happens, the Put will fail, and the
    whole operation will be retried, until it succeeds.

    Args:
      keys - List of keys to update transactionally.
      get_updates_fn - Callable which, given a key and a value, returns the
          updated value for that key, which will be put back in the DB. If a put
          fails due to a lock error, this function will be called again with the
          updated value.

    Returns:
      A CompountOperation with the result of the queries.
    """
    for key in keys:
      self.data[key] = get_updates_fn(key, self.data.get(key))

  ###################################################################
  # SETS
  ###################################################################

  def SetAdd(self, key, values):
    """Add values to the Set under key.

    Args:
      key - The key of the Set.
      values - List of values to add to the Set.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field is an integer denoting the number of new items added
      to the Set.
    """
    if key not in self.data:
      self.data[key] = set()

    total = 0
    for value in values:
      if value not in self.data[key]:
        total += 1
        self.data[key].add(value)

    return Operation(success=True, response_value=total)

  def SetMembers(self, key):
    """Retrieve all the members of the Set at key.

    Args:
      key - The key of the Set.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field contains a set() object with the members of the Set.
    """
    return Operation(success=True, response_value=self.data.get(key, []))

  def SetIsMember(self, key, value):
    """Test whether a value is a member of the Set at key.

    Args:
      key - The key of the Set.
      value - The String value to test membership in the Set.

    Returns:
      A CompoundOperation object with the result of the query.  The
      response_value field contains a boolean indicating whether the value is
      in the Set.
    """
    is_member = bool(value in self.data.get(key, []))
    return Operation(success=True, response_value=is_member)

  def SetRemove(self, key, values):
    """Remove values from the Set at key.

    Args:
      key - The key of the Set.
      values - List of values to remove from the Set.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field contains an integer denoting the number of
      items removed from the Set.
    """
    num_removed = 0
    for value in values:
      if value in self.data.get(key, []):
        self.data[key].remove(value)
        num_removed += 1

    return Operation(success=True, response_value=num_removed)

  def SetRemoveConditional(self, key, values, conditional_fn):
    """Remove values from the Set at key, but only if an external condition is
    met. The conditional_fn will be called after a transaction is started, but
    before it is committed. If the function returns False, the transaction is
    aborted. This allows arbitrary coordination (e.g. cross-shard conditions).

    Args:
      key - The key of the Set.
      values - List of values to remove from the Set.
      conditional_fn - Callable to be invoked within the transaction. If it
          returns True, the transaction will be committed; otherwise it will be
          aborted.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field contains an integer denoting the number of
      items removed from the Set, or if the transaction is aborted, the value
      False.
    """
    remove = conditional_fn()
    if remove:
      num_removed = 0
      for value in values:
        if value in self.data.get(key, []):
          self.data[key].remove(value)
          num_removed += 1
      return Operation(success=True, response_value=num_removed)

    else:
      return Operation(success=False)

  def SetPop(self, key):
    """Pop (remove and return) a random value from the Set at key.

    Args:
      key - The key of the Set.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field contains the value that was popped from the Set.
    """
    if key not in self.data:
      return Operation(success=False)
    return Operation(success=True, response_value=self.data[key].pop())

  def SetSize(self, key):
    """Returns the size (cardinality) of the set stored at key.

    Args:
      key - The key of the Set.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field contains an integer denoting the size of the set
      stored at key.
    """
    if key not in self.data:
      return Operation(success=False)
    return Operation(success=True, response_value=len(self.data[key]))

  ###################################################################
  # HASH
  ###################################################################

  def HashGet(self, key, field):
    """Retrieve the value stored at a field in a Hash.

    Args:
      key - The key of the Hash.
      field - Field in the Hash to retrieve.

    Returns:
      An Operation object with the result of the query.
    """
    if key not in self.data:
      result = None
    else:
      result = self.data[key].get(field)
    return Operation(success=True, response_value=result)

  def HashMultiGet(self, key, fields):
    """Retrieve a list of fields from a Hash.

    Args:
      key - The key of the Hash.
      fields - List of fields to retrieve.

    Returns:
      An Operation object with the result of the query.
    """
    result = {}

    if key in self.data:
      for field in fields:
        if field in self.data[key]:
          result[field] = self.data[key][field]

    return Operation(success=True, response_value=result)

  def HashFields(self, key):
    """Retrieve the field names of a Hash.

    Args:
      key - The key of the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    fields = []
    if key in self.data:
      fields = self.data[key].keys()

    return Operation(success=True, response_value=fields)

  def HashGetAll(self, key):
    """Retrieve an entire Hash.

    Args:
      key - The key of the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    return Operation(success=True, response_value=self.data.get(key, {}))

  def HashPut(self, key, field, value):
    """Put a value to a field of a Hash.

    Args:
      key - The key of the Hash.
      field - Field in the Hash to set.
      value - Value to set.

    Returns:
      An Operation object with the result of the query.
    """
    return self.HashMultiPut(key, {field: value})

  def HashMultiPut(self, key, mapping):
    """Put a set of fields and values to a Hash.

    Args:
      key - The key of the Hash.
      mapping - Dict of keys and values to set.

    Returns:
      An Operation object with the result of the query.
    """
    if key not in self.data:
      self.data[key] = {}

    self.data[key].update(mapping)
    return Operation(success=True, response_value=True)

  def HashReplace(self, key, mapping):
    """Completely replace the values of a Hash.

    Args:
      key - The key of the Hash.
      mapping - Dictionary of keys and values to set.

    Returns:
      An Operation object with the result of the query.
    """
    self.data[key] = mapping
    return Operation(success=True, response_value=True)

  def HashSize(self, key):
    """Get the number of fields in a Hash.

    Args:
      key - The key of the Hash,

    Returns:
      An Operation object with the result of the query.
    """
    size = 0
    if key in self.data:
      size = len(self.data[key])

    return Operation(success=True, response_value=size)

  def HashDelete(self, key, field):
    """Delete a field from a Hash.

    Args:
      key - The key of the Hash.
      field - Field to delete from the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    return self.HashBatchDelete(key, [field])

  def HashBatchDelete(self, key, fields):
    """Delete a list field from a Hash.

    Args:
      key - The key of the Hash.
      fields - Fields to delete from the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    if key not in self.data:
      return Operation(success=False)

    removed = 0
    for field in fields:
      if field in self.data[key]:
        del self.data[key][field]
        removed += 1

    return Operation(success=True, response_value=removed)

  def HashCheckAndMultiSet(self, key, fields, get_updates_fn):
    """In a transaction, retrieve a (full or partial) Hash, callback to get
    updated values, and set the updates. If the underlying Hash changes in the
    process, the transaction is retried with the new values.

    Args:
      key - the key of the Hash.
      fields - Fields to retrieve from the Hash. if None, all the fields are
          retrieved.
      get_updates_fn - Callable which accepts the current value of the Hash
          fields requested, and returns a Dict of {field: value} updates to
          apply back to the Hash. Note that the results do not have to be
          limited the the Hash fields that were retrieved. If the callback
          returns None, the transaction is aborted / no updates are applied.

    Returns:
      Operation objects with the results of the query.
    """
    current = {}

    if key in self.data:
      if fields is None:
        current = copy.copy(self.data[key])
      else:
        for field in fields:
          current[field] = self.data[key].get(field)

    updates = get_updates_fn(current)
    if key not in self.data:
      self.data[key] = {}
    self.data[key].update(updates)

    return Operation(success=True, response_value=True)

  def HashCheckAndReplace(self, key, get_updates_fn, tx_retries=0):
    """In a transaction, retrieve a full Hash, callback to get updated values,
    and completely replace the Hash with a new Dict. If the underlying Hash
    changes in the process, the transaction is retried with the new values.

    Args:
      key - the key of the Hash.
      fields - Fields to retrieve from the Hash. if None, all the fields are
          retrieved.
      get_updates_fn - Callable which accepts the current value of the Hash
          fields requested, and returns a Dict of {field: value} to replace the
          Hash with. If the callback returns None, the transaction is aborted /
          no replacement is made.
      tx_retries - Maximum number of transactional retries to allow before
        giving up entirely.

    Returns:
      Operation objects with the results of the query.
    """
    current = {}
    if key in self.data:
      current = copy.copy(self.data[key])

    updates = get_updates_fn(current)
    self.data[key] = updates

    return Operation(success=True, response_value=True)

  def HashCheckAndDelete(self, key, get_deletes_fn, tx_retries=0):
    """In a transaction, retrieve a full Hash, callback to get fields to delete,
    and delete those fields from the Hash. If the underlying Hash changes in the
    process, the transaction is retried with the new values.

    Args:
      key - the key of the Hash.
      get_deletes_fn - Callable which accepts the current value of the Hash
          fields requested, and returns an iterable of field names to delete
          from the Hash. If the callback returns None, the transaction is
          aborted / no deletions are made.
      tx_retries - Maximum number of transactional retries to allow before
        giving up entirely.

    Returns:
      Operation objects with the results of the query.
    """
    current = {}
    if key in self.data:
      current = copy.copy(self.data[key])

    deletions = get_deletes_fn(current)
    if deletions is not None:
      for field in deletions:
        if field in self.data[key]:
          del self.data[key][field]

    return Operation(success=True, response_value=True)

  ###################################################################
  # LISTS
  ###################################################################

  def ListLeftPush(self, key, value):
    """Push a value onto the left end of a List.

    Args:
      key - The key of the List.
      value - Value to push onto the List.

    Returns:
      Operation object.
    """
    if key in self.data:
      self.data[key].insert(0, value)
    else:
      self.data[key] = [value]

    return Operation(success=len(self.data[key]))

  def ListGetAndDeleteAtomic(self, key):
    """Retrieve the entire contents of a List, and then delete it, in an atomic
    operation.

    Args:
      key - The key of the List.

    Returns:
      Operation object.
    """
    if key in self.data:
      data = self.data[key]
      del self.data[key]

    return Operation(success=True, response_value=data)

  def ListLenBatch(self, keys):
    """Retrieve the length of multiple Lists.

    Args:
      keys - The keys of the Lists.

    Returns:
      Operation object. On success, the response_value field contains the list
      of List lengths.
    """
    lens = []
    for key in keys:
      lens.append(len(self.data.get(key, [])))

    return Operation(success=True, response_value=lens)

  ###################################################################
  # MISC
  ###################################################################

  @contextmanager
  def Lock(self, lock_name, duration):
    """Context manager for acquiring/releasing a global lock.

    Args:
      lock_name - Name of the lock to obtain.
      duration - Timeut of the lock in seconds.
    """
    yield

  def Keys(self, pattern):
    """Retrieve the set of keys in the database matching a glob-style pattern.

    NOTE: This command runs in O(n), where n is the number of keys in the
    database. It should only be used for maintenance or debugging purposes.

    Args:
      pattern - Glob-style pattern to retrieve keys for.

    Returns:
      An Operation object with the result of the query. The response_value field
      contains a list of keys matching the pattern.
    """
    if pattern != '*':
      raise NotImplementedError()
    return Operation(success=True, response_value=self.data.keys())
