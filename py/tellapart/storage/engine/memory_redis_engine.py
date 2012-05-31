"""
Copyright 2012 TellApart, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

------------------------------------------------------------

Mock Storage Engine that exposes a Redis-like interface, and stores all data
in-memory.
"""

from tellapart.storage.operation import Operation

class MemoryRedisEngine(object):
  def __init__(self, endpoints, num_vbuckets):
    """RedisEngine compatible signature.
    """
    self.val_dict = {}

  def Get(self, key):
    """Return the value for a given key.

    Args:
      key - The key to lookup.

    Returns:
      Operation object with the result of the lookup.
    """
    # Resubmit the Get operation as as Batch Get.
    op = self.BatchGet([key])

    # Unpack the batch operation results.
    if op.success and op.response_value:
      op.response_value = op.response_value[0]

    return op

  def BatchGet(self, keys):
    """Return the values for a set of keys.

    Args:
      keys - List of keys to lookup.

    Returns:
      CompoundOperation with the results of the lookups. The response_value
      field has the form [(key, value)., ...]
    """
    responses = []
    for key in keys:
      if type(self.val_dict.get(key)) == set:
        return Operation(success=False)

      responses.append(self.val_dict.get(key))

    return Operation(success=True, response_value=responses)

  def Put(self, key, value):
    """Put a value in a key.

    Args:
      key - The key to put.
      value - The value to put.

    Returns:
      An Operation object with the result of the query.
    """
    # Resubmit the Put as a Batch Put.
    op = self.BatchPut([(key, value)])

    # Unpack the batch operation result.
    if op.response_value and op.response_value:
      op.response_value = op.response_value[0]

    return op

  def BatchPut(self, key_value_tuples):
    """Put a batch of values into keys.

    Args:
      key_value_tuples - A list of tuples of the form (key, value)

    Returns:
      A CompoundOperation with the results of the queries.
    """
    responses = []
    for key, value in key_value_tuples:
      if type(self.val_dict.get(key)) == set:
        return Operation(success=False)

      self.val_dict[key] = value
      responses.append(True)

    op = Operation(success=True, response_value=responses)
    return op

  def Delete(self, key):
    """Delete a key.

    Args:
      key - The key to delete.

    Returns:
      An Operation object with the results of the query.
    """
    # Resubmit the Delete as a Batch Delete.
    op = self.BatchDelete([key])

    # Unpack the batch operation results.
    if op.success and op.response_value:
      op.response_value = op.response_value[0]

    return op

  def BatchDelete(self, keys):
    """Delete a ket of keys.

    Args:
      keys - A list of keys to delete.

    Returns:
      A CompountOperation with the results of the queries.
    """
    responses = []
    for key in keys:
      if key in self.val_dict:
        del self.val_dict[key]
        responses.append(1)
      else:
        responses.append(0)

    return Operation(success=True, response_value=responses)

  def SetAdd(self, key, *values):
    """Add values to the Set under key.

    Args:
      key - The key of the Set.
      values - List of values to add to the Set.

    Returns:
      An Operation object with the result of the query. The response_value
      field contains a list of integers, the sum of which represents the number
      of new items added to the set.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != set:
        return Operation(success=False)

      before = len(self.val_dict[key])
      self.val_dict[key].update(values)
      added = len(self.val_dict[key]) - before

    else:
      self.val_dict[key] = set(values)
      added = len(self.val_dict[key])

    return Operation(success=True, response_value=added)

  def SetMembers(self, key):
    """Retrieve all the members of the Set at key.

    Args:
      key: - The key of the Set.

    Returns:
      An Operation object with the result of the query. The response_value
      field contains a set() object with the members of the Set.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != set:
        return Operation(success=False)

      val = self.val_dict[key]

    else:
      val = set([])

    return Operation(success=True, response_value=val)

  def SetIsMember(self, key, value):
    """Test whether a value is a member of the Set at key.

    Args:
      key - The key of the Set.
      value - The String value to test membership in the Set.

    Returns:
      A boolean indicating whether the value is in the set.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != set:
        return Operation(success=False)

      val = value in self.val_dict[key]

    else:
      val = False

    return Operation(success=True, response_value=val)

  def SetRemove(self, key, *values):
    """Remove values from the Set at key.

    Args:
      key - The key of the Set.
      values - List of values to remove from the Set.

    Returns:
      An Operation object with the result of the query. The response_value
      field contains a list of integers, the sum of which is the number of
      items removed from the Set.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != set:
        return Operation(success=False)

      before = len(self.val_dict[key])
      self.val_dict[key] = self.val_dict[key] - set(values)
      removed = before - len(self.val_dict[key])

    else:
      removed = 0

    return Operation(success=True, response_value=removed)

  def HashPut(self, key, field, value):
    """Put a value to a field of a Hashtable.

    Args:
      key - The key of the Hashtable
      field - Field in the Hashtable to set.
      value - Value to set.

    Returns:
      An Operation object with the result of the query.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != dict:
        return Operation(success=False)

      self.val_dict[key][field] = value
      result = 0

    else:
      self.val_dict[key] = {field: value}
      result = 1

    return Operation(success=True, response_value=result)

  def HashMultiPut(self, key, mapping):
    """Put a set of fields and values to a Hashtable.

    Args:
      key - The key of the Hashtable.
      mapping - Dictionary of keys and values to set.

    Returns:
      An Operation object with the result of the query.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != dict:
        return Operation(success=False)

      self.val_dict[key].extend(mapping)

    else:
      self.val_dict[key] = mapping

    return Operation(success=True, response_value=True)

  def HashDelete(self, key, field):
    """Delete a field from a Hashtable.

    Args:
      key - The key of the Hashtable.
      field - Field to delete from the Hashtable.

    Returns:
      An Operation object with the result of the query.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != dict:
        return Operation(success=False)

      if field in self.val_dict[key]:
        del self.val_dict[key][field]
        removed = 1
      else:
        removed = 0

    else:
      removed = 0

    return Operation(success=True, response_value=removed)

  def HashGet(self, key, field):
    """Retrieve the value stored at a field in a Hashtable.

    Args:
      key - The key of the Hashtable.
      field - Field in the Hashtable to retrieve.

    Returns:
      An Operation object with the result of the query.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != dict:
        return Operation(success=False)

      result = self.val_dict[key].get(field)

    else:
      result = None

    return Operation(success=True, response_value=result)

  def HashBatchGet(self, keys, field):
    """Retrieve the value of a field from several Hashtables.

    Args:
      keys - The keys of the Hashtables to lookup.
      field - The field to get from each Hashtable.

    Returns:

    """
    result = []
    for key in keys:
      if key in self.val_dict:
        if type(self.val_dict[key]) != dict:
          return Operation(success=False)

        result.append(self.val_dict[key].get(field))

      else:
        result.append(None)

    return Operation(success=True, response_value=result)

  def HashGetAll(self, key):
    """Retrieve an entire Hashtable.

    Args:
      key - The key of the Hashtable.

    Returns:
      An Operation object with the result of the query.
    """
    if key in self.val_dict:
      if type(self.val_dict[key]) != dict:
        return Operation(success=False)

      result = self.val_dict[key]

    else:
      result = None

    return Operation(success=True, response_value=result)

  def BatchCheckAndMultiSet(self, keys, callback_fn):
    """Set a batch of keys transactionally using optimistic locking. Each key
    is locked and retrieved. The retrieved values are passed, one at a time, to
    the callback function. The return value of the callback is then put in the
    key. If the value changed between the time it was locked and when the Put
    happens, the Put will fail, and the whole operation will be retried, until
    it succeeds.

    Args:
      keys - List of keys to update transactionally.
      callback_fn - Callable which, given a key and a value, returns the updated
          value for that key, which will be put back in the DB. If a put fails
          due to a lock error, this function will be called again with the
          updated value.

    Returns:
      A CompountOperation with the result of the queries.
    """
    responses = []

    for key in keys:
      val = self.val_dict.get(key)
      new_kvs = callback_fn(key, val)

      for key, new_val in new_kvs:
        self.val_dict[key] = new_val
        responses.append(True)

    return Operation(success=True, response_value=responses)
