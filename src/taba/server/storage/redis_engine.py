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

"""Storage Engine for interfacing with a cluster of Redis servers.

TODO: Consider limiting batch sizes on things like Hash puts.
"""

import bisect
from collections import defaultdict
from contextlib import contextmanager
import itertools
import logging
import random
import traceback

import gevent
import mmh3
import redis
from redis.exceptions import ConnectionError
from redis.sentinel import Sentinel

from taba import client
from taba.server.storage.redis_connection_pool import (
    BlockingSentinelMasterGeventConnectionPool)
from taba.server.storage.util import CompoundOperation
from taba.server.storage.util import Operation
from taba.util.thread_util import YieldByCount

LOG = logging.getLogger(__name__)

# The number of times to retry a transaction due to a WatchError before giving
# up entirely and raising an exception. This should be set very high, since
# separate shards are independent transactions (i.e. a shard can fail, while
# other shards committed their changes, leaving the DB in an inconsistent
# state)
MAX_TX_RETRIES = 20

# The Check-and-Set batch size. This controls the number of keys which will be
# locked, retrieved, and updated in a single transaction.
CAS_BATCH_SIZE = 20
CAS_PARALLEL_REQUESTS = 5

# The Get batch size. This controls the number of keys which will be retrieved
# per pipelined Get call on a single shard.
GET_BATCH_SIZE = 1000

# The timeout for a connection in seconds.  The current redis client will
# attempt to connect twice before giving up, so the effective timeout will be
# 2x this.
CONNECTION_TIMEOUT = 5.000

# The amount of time to sleep between attempts to acquire a lock.
LOCK_WAIT_SLEEP = 0.010

class RedisServerEndpoint(object):
  """Redis Server end-point specification struct"""

  def __init__(self, shard_name, vbucket_start, vbucket_end):
    """
    Args:
      shard_name - Sentinel name of the Redis shard.
      vbucket_start - First Virtual Bucket in the range the end-point handles.
      vbucket_end - Last Virtual Bucket in the range the end-point handles.
    """
    self.shard_name = shard_name
    self.vbucket_start = vbucket_start
    self.vbucket_end = vbucket_end

class RedisEngine(object):
  """Class for interfacing with a cluster of Redis servers.

  Values are transparently distributed across the cluster using Virtual Buckets
  on the hash of the key. Keys are hashed into one of a large number of Virtual
  Buckets. These buckets are assigned in ranges to specific shards. The cluster
  can be re-sharded by reassigning the Virtual Bucket ranges.
  """

  def __init__(self,
      sentinels,
      endpoints,
      num_vbuckets,
      pool_size=2,
      timeout=CONNECTION_TIMEOUT):
    """
    Args:
      sentinels - List of sentinel connection info.
      endpoints - List of RedisServerEndpoint objects.
      num_vbuckets - The total number of configured Virtual Buckets.
      pool_size - The number of connections in each connection pool for each
          end-point.
      timeout - Socket connection timeout.
    """
    self.num_vbuckets = num_vbuckets
    self.num_endpoints = len(endpoints)

    # Sort the end-points by vbucket start, and make sure all the vbuckets
    # are accounted for.
    endpoints = sorted(endpoints, key=lambda e: e.vbucket_start)
    if endpoints[0].vbucket_start != 0:
      raise ValueError('First Virtual Bucket must be 0')

    if endpoints[-1].vbucket_end != self.num_vbuckets - 1:
      raise ValueError('Last Virtual Bucket does not match total')

    for i in xrange(0, len(endpoints) - 2):
      if endpoints[i].vbucket_end + 1 != endpoints[i + 1].vbucket_start:
        raise ValueError(
            'Virtual Bucket range mismatch between end-points %d and %d' %
            (i, i + 1))

    self.sentinel = Sentinel(sentinels, socket_timeout=timeout)

    self.shards = []
    self.vbucket_starts = []
    for endpoint in endpoints:
      endpoint_client = self.sentinel.master_for(
          endpoint.shard_name,
          connection_pool_class=BlockingSentinelMasterGeventConnectionPool,
          pool_size=pool_size)

      self.shards.append(endpoint_client)
      self.vbucket_starts.append(endpoint.vbucket_start)

  def ShutDown(self):
    """Called when the process is shutting-down. Any necessary clean-up actions
    should be performed here. (Needed to conform to the StorageEngine API).
    """
    # Disconnect each of the redis connections in each of the connection pools.
    for shard in self.shards:
      shard.connection_pool.shutdown()

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
    def _ShardGet(shard, keys, vkeys, values):
      op = CompoundOperation()
      for i in xrange(0, len(vkeys), batch_size):
        response_value = shard.mget([vkey for vkey in vkeys[i:i + batch_size]])
        op.AddOp(Operation(success=True, response_value=response_value))

      op.response_value = list(itertools.chain.from_iterable(
          (sub_op.response_value for sub_op in op.sub_operations)))
      return op

    return self._ShardedOp([(key, None) for key in keys], _ShardGet)

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
    def _ShardPut(shard, keys, vkeys, values):
      if (expiration_time == None) or (expiration_time <= 0):
        successes = [shard.mset(dict(zip(vkeys, values)))]
      else:
        pipe = shard.pipeline()
        map(pipe.setex, vkeys, [expiration_time for _ in vkeys], values)
        successes = pipe.execute()

      return Operation(success=True, response_value=successes)

    return self._ShardedOp(key_value_tuples, _ShardPut)

  def PutNotExist(self, key, value):
    """Put a value into a key, only if the value does not already exist.

    Args:
      key - The key to put.
      value - The value to put.

    Returns:
      Operation object. On success, the response_value will contain the number
      of keys modified (i.e. 1 if the key was set, 0 if it already existed)
    """
    _, shard, vkey = self._GetShardInfo(key)
    result = shard.setnx(vkey, value)

    return Operation(success=True, response_value=result)

  def Increment(self, key):
    """Atomically increment an integer.

    Args:
      key - The key of the integer to increment.

    Returns:
      An Operation object with the result of the query.
    """
    _, shard, vkey = self._GetShardInfo(key)
    result = shard.incr(vkey)

    return Operation(success=True, response_value=result)

  def Delete(self, key):
    """Delete a key.

    Args:
      key - The key to delete.

    Returns:
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
    def _ShardDelete(shard, keys, vkeys, values):
      successes = [shard.delete(*vkeys)]
      return Operation(success=True, response_value=successes)

    return self._ShardedOp([(key, None) for key in keys], _ShardDelete)

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
    _ShardCheckAndSet = _ShardCheckAndSetCallable(self, get_updates_fn)
    op = self._ShardedOp([(key, None) for key in keys], _ShardCheckAndSet)
    return op

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
    _, shard, vkey = self._GetShardInfo(key)
    num_new_values = shard.sadd(vkey, *values)

    return Operation(success=True, response_value=num_new_values)

  def SetMembers(self, key):
    """Retrieve all the members of the Set at key.

    Args:
      key - The key of the Set.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field contains a set() object with the members of the Set.
    """
    _, shard, vkey = self._GetShardInfo(key)
    members = shard.smembers(vkey)

    return Operation(success=True, response_value=set(members))

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
    _, shard, vkey = self._GetShardInfo(key)
    is_member = shard.sismember(vkey, value)

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
    _, shard, vkey = self._GetShardInfo(key)
    num_removed = shard.srem(vkey, *values)

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
    _, shard, vkey = self._GetShardInfo(key)

    def _TransactionBody(pipe):
      pipe.watch(vkey)

      proceed = conditional_fn()

      if proceed:
        pipe.multi()
        pipe.srem(vkey, *values)
        return pipe.execute()

      else:
        return [False]

    op = TransactionSkeleton(shard, _TransactionBody)

    return Operation(
        success=all([op.success, op.response_value, op.response_value[0]]),
        response_value=op.response_value)

  def SetPop(self, key):
    """Pop (remove and return) a random value from the Set at key.

    Args:
      key - The key of the Set.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field contains the value that was popped from the Set.
    """
    _, shard, vkey = self._GetShardInfo(key)
    popped = shard.spop(vkey)

    return Operation(success=True, response_value=popped)

  def SetSize(self, key):
    """Returns the size (cardinality) of the set stored at key.

    Args:
      key - The key of the Set.

    Returns:
      A CompoundOperation object with the result of the query. The
      response_value field contains an integer denoting the size of the set
      stored at key.
    """
    _, shard, vkey = self._GetShardInfo(key)
    size = shard.scard(vkey)

    return Operation(success=True, response_value=size)

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
    _, shard, vkey = self._GetShardInfo(key)
    result = shard.hget(vkey, field)

    return Operation(success=True, response_value=result)

  def HashMultiGet(self, key, fields):
    """Retrieve a list of fields from a Hash.

    Args:
      key - The key of the Hash.
      fields - List of fields to retrieve.

    Returns:
      An Operation object with the result of the query.
    """
    _, shard, vkey = self._GetShardInfo(key)
    values = shard.hmget(vkey, fields)

    hash_values = dict(itertools.izip(fields, values))
    return Operation(success=True, response_value=hash_values)

  def HashFields(self, key):
    """Retrieve the field names of a Hash.

    Args:
      key - The key of the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    _, shard, vkey = self._GetShardInfo(key)
    result = shard.hkeys(vkey)

    return Operation(success=True, response_value=result)

  def HashGetAll(self, key):
    """Retrieve an entire Hash.

    Args:
      key - The key of the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    _, shard, vkey = self._GetShardInfo(key)
    result = shard.hgetall(vkey)

    return Operation(success=True, response_value=result)

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
    _, shard, vkey = self._GetShardInfo(key)
    response = shard.hmset(vkey, mapping)

    return Operation(success=True, response_value=response)

  def HashReplace(self, key, mapping):
    """Completely replace the values of a Hash.

    Args:
      key - The key of the Hash.
      mapping - Dictionary of keys and values to set.

    Returns:
      An Operation object with the result of the query.
    """
    _, shard, vkey = self._GetShardInfo(key)

    pipe = shard.pipeline()
    pipe.delete(vkey)
    pipe.hmset(vkey, mapping)
    response = pipe.execute()

    return Operation(success=True, response_value=response)

  def HashSize(self, key):
    """Get the number of fields in a Hash.

    Args:
      key - The key of the Hash,

    Returns:
      An Operation object with the result of the query.
    """
    _, shard, vkey = self._GetShardInfo(key)
    response = shard.hlen(vkey)

    return Operation(success=True, response_value=response)

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
    _, shard, vkey = self._GetShardInfo(key)
    removed = shard.hdel(vkey, *fields)

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
    _, shard, vkey = self._GetShardInfo(key)

    def _TransactionBody(pipe):
      pipe.watch(vkey)

      if fields is None:
        hash_values = pipe.hgetall(vkey)
      else:
        values = pipe.hmget(vkey, fields)
        hash_values = dict(zip(fields, values))

      # Get the updates from the callback. A return value of None means no
      # updates need to be applied.
      new_hash_values = get_updates_fn(hash_values)
      if new_hash_values is None:
        return [True]

      # Put the new values into the DB.
      pipe.multi()
      pipe.hmset(vkey, new_hash_values)

      return pipe.execute()

    op = TransactionSkeleton(shard, _TransactionBody)

    return Operation(
        success=all([op.success, op.response_value, op.response_value[0]]),
        response_value=op.response_value)

  def HashCheckAndReplace(self, key, get_updates_fn, tx_retries=MAX_TX_RETRIES):
    """In a transaction, retrieve a full Hash, callback to get updated values,
    and completely replace the Hash with a new Dict. If the underlying Hash
    changes in the process, the transaction is retried with the new values.

    Args:
      key - the key of the Hash.
      get_updates_fn - Callable which accepts the current value of the Hash
          fields requested, and returns a Dict of {field: value} to replace the
          Hash with. If the callback returns None, the transaction is aborted /
          no replacement is made.
      tx_retries - Maximum number of transactional retries to allow before
        giving up entirely.

    Returns:
      Operation objects with the results of the query.
    """
    _, shard, vkey = self._GetShardInfo(key)

    def _TransactionBody(pipe):
      pipe.watch(vkey)

      hash_values = pipe.hgetall(vkey)

      # Get the updates from the callback. A return value of None means no
      # updates need to be applied.
      new_hash_values = get_updates_fn(hash_values)
      if new_hash_values is None:
        return [True]

      # Put the new values into the DB.
      pipe.multi()
      pipe.delete(vkey)
      if len(new_hash_values) > 0:

        pipe.hmset(vkey, new_hash_values)

      return pipe.execute()

    op = TransactionSkeleton(shard, _TransactionBody, max_retries=tx_retries)

    return Operation(
        success=all([op.success, op.response_value, op.response_value[0]]),
        response_value=op.response_value)

  def HashCheckAndDelete(self, key, get_deletes_fn, tx_retries=MAX_TX_RETRIES):
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
    _, shard, vkey = self._GetShardInfo(key)

    def _TransactionBody(pipe):
      pipe.watch(vkey)

      hash_values = pipe.hgetall(vkey)

      # Get the updates from the callback. A return value of None means no
      # updates need to be applied.
      field_deletions = get_deletes_fn(hash_values)
      if field_deletions is None:
        return [True]

      # Put the new values into the DB.
      pipe.multi()

      for field in field_deletions:
        pipe.hdel(vkey, field)

      return pipe.execute()

    op = TransactionSkeleton(shard, _TransactionBody, max_retries=tx_retries)

    return Operation(
        success=all([op.success, op.response_value, op.response_value[0]]),
        response_value=op.response_value)

  ###################################################################
  # DISTRIBUTED HASH
  ###################################################################

  def DHashGet(self, key, field):
    """Retrieve the value stored at a field in a Distributed Hash.

    Args:
      key - The key of the Hash.
      field - Field in the Hash to retrieve.

    Returns:
      An Operation object with the result of the query.
    """
    cop = self.DHashMultiGet(key, [field])
    if cop and cop.response_value:
      cop.response_value = cop.response_value[field]

    return cop

  def DHashMultiGet(self, key, fields):
    """Retrieve a list of fields from a Distributed Hash.

    Args:
      key - The key of the Hash.
      fields - List of fields to retrieve.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardDHashMultiGet(shard, h_keys, h_vkeys, values):
      response = shard.hmget(key, h_keys)
      values_dict = dict(itertools.izip(h_keys, response))
      return Operation(success=True, response_value=values_dict)

    cop = self._ShardedOp(
        itertools.izip(fields, [None] * len(fields)),
        _ShardDHashMultiGet)

    if cop and cop.sub_operations:
      cop.response_value = {}
      for op in cop.sub_operations:
        cop.response_value.update(op.response_value)

    return cop

  def DHashGetAll(self, key):
    """Retrieve an entire Distributed Hash.

    Args:
      key - The key of the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardDHashGetAll(shard):
      result = shard.hgetall(key)
      return Operation(success=True, response_value=result)

    cop = self._AllShardsOp(_ShardDHashGetAll)
    if cop and cop.sub_operations:
      cop.response_value = {}
      for op in cop.sub_operations:
        cop.response_value.update(op.response_value)

    return cop

  def DHashFields(self, key):
    """Retrieve the field names of a Distributed Hash.

    Args:
      key - The key of the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardDHashFields(shard):
      result = shard.hkeys(key)
      return Operation(success=True, response_value=result)

    cop = self._AllShardsOp(_ShardDHashFields)
    if cop and cop.sub_operations:
      cop.response_value = [
          e for op in cop.sub_operations for e in op.response_value]

    return cop

  def DHashPut(self, key, field, value):
    """Put a value to a field of a Distributed Hash.

    Args:
      key - The key of the Hash.
      field - Field in the Hash to set.
      value - Value to set.

    Returns:
      An Operation object with the result of the query.
    """
    return self.DHashMultiPut(key, {field: value})

  def DHashMultiPut(self, key, mapping):
    """Put a set of fields and values to a Distributed Hash.

    Args:
      key - The key of the Hash.
      mapping - Dict of keys and values to set.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardDHashMultiPut(shard, h_keys, h_vkeys, values):
      response = shard.hmset(key, dict(zip(h_keys, values)))
      return Operation(success=True, response_value=[response])

    return self._ShardedOp(mapping.items(), _ShardDHashMultiPut)

  def DHashSize(self, key):
    """Get the number of fields in a Distributed Hash.

    Args:
      key - The key of the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardDHashSize(shard):
      response = shard.hlen(key)
      return Operation(success=True, response_value=response)

    cop = self._AllShardsOp(_ShardDHashSize)
    if cop and cop.sub_operations:
      cop.response_value = sum(op.response_value for op in cop.sub_operations)

    return cop

  def DHashDelete(self, key, field):
    """Delete a field from a Distributed Hash.

    Args:
      key - The key of the Hash.
      field - Field to delete from the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    return self.DHashBatchDelete(key, [field])

  def DHashBatchDelete(self, key, fields):
    """Delete a list field from a Distributed Hash.

    Args:
      key - The key of the Hash.
      fields - Fields to delete from the Hash.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardDHashBatchDelete(shard, fields, vfields, values):
      removed = shard.hdel(key, *fields)
      return Operation(success=True, response_value=[removed])

    cop = self._ShardedOp(
        itertools.izip(fields, [None] * len(fields)),
       _ShardDHashBatchDelete)
    if cop and cop.sub_operations:
      cop.response_value = \
          sum(op.response_value[0] for op in cop.sub_operations)

    return cop

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
    _, shard, vkey = self._GetShardInfo(key)
    result = shard.lpush(vkey, value)

    return Operation(success=result)

  def ListGetAndDeleteAtomic(self, key):
    """Retrieve the entire contents of a List, and then delete it, in an atomic
    operation.

    Args:
      key - The key of the List.

    Returns:
      Operation object.
    """
    _, shard, vkey = self._GetShardInfo(key)

    pipe = shard.pipeline()
    pipe.lrange(vkey, 0, -1)
    pipe.delete(vkey)
    result = pipe.execute()

    return Operation(success=True, response_value=result[0])

  def ListLenBatch(self, keys):
    """Retrieve the length of multiple Lists.

    Args:
      keys - The keys of the Lists.

    Returns:
      Operation object. On success, the response_value field contains the list
      of List lengths.
    """
    def _ShardListLenBatch(shard, keys, vkeys, vals):
      pipe = shard.pipeline()
      map(pipe.llen, vkeys)
      result = pipe.execute()
      return Operation(success=True, response_value=result)

    return self._ShardedOp([(k, None) for k in keys], _ShardListLenBatch)

  ###################################################################
  # MISC
  ###################################################################

  @contextmanager
  def Lock(self, lock_name, duration):
    """Context manager for acquiring/releasing a global lock.

    Args:
      lock_name - Name of the lock to obtain.
      duration - Timeout of the lock in seconds.
    """
    (_, shard, vlock_name) = self._GetShardInfo(lock_name)

    with shard.lock(vlock_name, duration, LOCK_WAIT_SLEEP):
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
    keys = set()

    try:
      # Get the keys from each shard for prefixed and non-prefixed keys.
      for shard in self.shards:
        shard_keys = shard.keys(pattern)
        shard_vkeys = shard.keys('*|%s' % pattern)

        keys.update(shard_keys)
        keys.update(['|'.join(vkey.split('|')[1:]) for vkey in shard_vkeys])

      op = Operation(success=True, response_value=keys)

    except ConnectionError:
      client.Counter('redis_connection_error')
      op = Operation(success=False, traceback=traceback.format_exc())
    except Exception:
      op = Operation(success=False, traceback=traceback.format_exc())

    return op

  ###################################################################
  # SHARDING HELPERS
  ###################################################################

  def _GetShardInfo(self, key):
    """For a given client key, calculate the associated shard number and
    virtual key.

    Args:
      key - Client key string.

    Returns:
      A tuple of (shard number, shard, virtual key)
    """
    key_vbucket = self._GetVbucket(key)

    shard_num = bisect.bisect(self.vbucket_starts, key_vbucket) - 1
    shard = self.shards[shard_num]
    vkey = self._MakeVkey(key, key_vbucket)

    return (shard_num, shard, vkey)

  def _GetVbucket(self, key):
    """For a given client key, calculate the associated Virtual Bucket. This is
    the hash of the key string modded by the number of Virtual Buckets.

    Args:
      key - Client key string.

    Returns:
      Virtual bucket number.
    """
    return mmh3.hash(key) % self.num_vbuckets

  def _MakeVkey(self, key, vbucket):
    """For a given client key and Virtual Bucket, generate the virtual key.

    Args:
      key - Client key string.
      vbucket - Associated Virtual Bucket number.

    Returns:
      The virtual key string.
    """
    return "%d|%s" % (vbucket, key)

  def _ShardedOp(self, key_value_tuples, shard_execute_fn):
    """Perform an operation on the necessary shards in the cluster, and
    aggregate the results. The callback will be invoked for each shard, with the
    keys, virtual keys, and and values that belong to that shard.

    Args:
      key_value_tuples - List of tuples of (key, value) for the operation.
      shard_execute_fn - Callback for executing the desired operation in each
          shard. The signature is f(shard, keys, vkeys, values) => Operation

    Returns:
      A CompoundOperation with the results of the queries. The response_value
      field is the combined response_value of each sub-operation (assuming all
      the sub-operations succeeded and returned a response_value)
    """
    # Compute the shard info for all the keys, and group them by the shard in
    # which they reside.
    vkeys_by_shard = defaultdict(list)
    for i, (key, val) in enumerate(key_value_tuples):
      (shard_num, _, vkey) = self._GetShardInfo(key)
      vkeys_by_shard[shard_num].append((i, key, vkey, val))

    # Split the request for each shard into a separate greenlet to allow the
    # requests to happen in parallel.
    op = CompoundOperation()
    responses = []

    def _ShardGreenletWrapper(shard_num, vkey_tuples):
      indices, keys, vkeys, values = [list(i) for i in zip(*vkey_tuples)]

      try:
        sub_op = shard_execute_fn(self.shards[shard_num], keys, vkeys, values)
      except ConnectionError:
        client.Counter('redis_connection_error_shard_%s' % shard_num)
        sub_op = Operation(success=False, traceback=traceback.format_exc())
      except Exception:
        sub_op = Operation(success=False, traceback=traceback.format_exc())

      # Aggregate the results.
      op.AddOp(sub_op)
      if sub_op.response_value:
        responses.extend(zip(indices, sub_op.response_value))

    greenlets = [
        gevent.spawn(_ShardGreenletWrapper, shard_num, vkey_tuples)
        for shard_num, vkey_tuples in vkeys_by_shard.iteritems()]

    gevent.joinall(greenlets)

    # Sort the combined responses back into the request order.
    responses = sorted(responses, key=lambda r: r[0])
    op.response_value = [r[1] for r in responses]

    return op

  def _AllShardsOp(self, shard_execute_fn):
    """Perform an operation on the necessary shards in the cluster, and
    aggregate the results. The callback will be invoked for each shard, with the
    keys, virtual keys, and and values that belong to that shard.

    Args:
      key_value_tuples - List of tuples of (key, value) for the operation.
      shard_execute_fn - Callback for executing the desired operation in each
          shard. The signature is f(shard) => Operation

    Returns:
      A CompoundOperation with the results of the queries. The response_value
      field is the combined response_value of each sub-operation (assuming all
      the sub-operations succeeded and returned a response_value)
    """
    # Split the request for each shard into a separate greenlet to allow the
    # requests to happen in parallel.
    op = CompoundOperation()

    def _ShardGreenletWrapper(shard):
      try:
        sub_op = shard_execute_fn(shard)
      except ConnectionError:
        client.Counter('redis_connection_error')
        sub_op = Operation(success=False, traceback=traceback.format_exc())
      except Exception:
        sub_op = Operation(success=False, traceback=traceback.format_exc())

      # Aggregate the results.
      op.AddOp(sub_op)

    greenlets = [
        gevent.spawn(_ShardGreenletWrapper, shard)
        for shard in self.shards]
    gevent.joinall(greenlets)

    return op

class _ShardCheckAndSetCallable(object):
  """Callable class to implement the per-shard logic for BatchCheckAndMultiSet.
  This is implemented as a class instead of a closure to reduce memory use and
  avoid leaks.
  """
  def __init__(self, engine, get_updates_fn):
    """
    Args:
      engine - RedisEngine object to make calls through.
      get_updates_fn - See RedisEngine.BatchCheckAndMultiSet.
    """
    self.engine = engine
    self.get_updates_fn = get_updates_fn

  def __call__(self, *args, **kwargs):
    return self._ShardCheckAndSet(*args, **kwargs)

  def _ShardCheckAndSet(self, shard, keys, vkeys, values):
    op = CompoundOperation()

    # Split the request into batches to avoid locking too many keys at once.
    greenlet_args = [(
            shard,
            keys[i:i + CAS_BATCH_SIZE],
            vkeys[i:i + CAS_BATCH_SIZE],
            values[i:i + CAS_BATCH_SIZE])
        for i in xrange(0, len(keys), CAS_BATCH_SIZE)]

    greenlets = []
    for j in xrange(0, len(greenlet_args), CAS_PARALLEL_REQUESTS):
      greenlet_batch = [
          gevent.spawn(self._ShardCheckAndSetBatch, *args)
          for args in greenlet_args[j:j + CAS_PARALLEL_REQUESTS]]

      greenlets.extend(greenlet_batch)
      gevent.joinall(greenlet_batch)

    # Get the results and combine them into a single Operation.
    [op.AddOp(g.get()) for g in greenlets]
    op.response_value = []
    for sub_op in op.sub_operations:
      if sub_op.success:
        op.response_value.extend(sub_op.response_value)

    return op

  def _ShardCheckAndSetBatch(self, shard, keys, vkeys, values):
    retries = 0
    while True:
      # Open a transactional pipeline.
      pipe = shard.pipeline(True)

      try:
        # Lock the keys to start the operation.
        pipe.watch(*vkeys)

        # Batch Get the keys to be updated.
        values = pipe.mget(vkeys)

        # Get the new values from the client.
        put_kv_tuples = []
        for key, value in YieldByCount(itertools.izip(keys, values)):
          put_kv_tuples.extend(self.get_updates_fn(key, value))

        # Put the new values into the DB.
        pipe.multi()

        val_dict = {}
        for key, value in put_kv_tuples:
          vbucket = self.engine._GetVbucket(key)
          vkey = self.engine._MakeVkey(key, vbucket)
          val_dict[vkey] = value

        pipe.mset(val_dict)

        response = pipe.execute()

      except redis.WatchError:
        # Lock error occurred. Try the operation again.
        gevent.sleep(0.1 * retries + 0.1 * random.random())

        retries += 1
        if retries > MAX_TX_RETRIES:
          raise

        continue

      finally:
        # Make sure we always reset the pipe.
        pipe.reset()

      # If we make it here without a WatchError, the operation succeeded.
      op = Operation(success=True, response_value=response, retries=retries)
      break

    return op

def TransactionSkeleton(client, body_fn, max_retries=MAX_TX_RETRIES):
  """Basic skeleton implementation of a Redis transaction, handling retries and
  resets. The body of the transaction is left to a callback.

  Args:
    client - Redis object to open a pipeline on.
    body_fn - Callback to execute the body of the transaction (i.e. watch,
        multi, exec, ...). The signature of the function should be:
          body_fn(pipeline) -> Operation
    max_retries - Maximum number of transactional retries to allow before
        giving up entirely.

  Returns:
    Operation object.
  """
  retries = 0

  while True:
    # Open a transactional pipeline.
    pipe = client.pipeline(True)

    try:
      response = body_fn(pipe)

    except (redis.WatchError, redis.ConnectionError):
      # Lock error or connection error occurred. Try the operation again.
      gevent.sleep(0.1 * retries + 0.1 * random.random())

      retries += 1
      if retries > max_retries:
        raise

      continue

    finally:
      # Make sure we always reset the pipe.
      pipe.reset()

    # If we make it here without a WatchError, the operation succeeded.
    op = Operation(success=True, response_value=response, retries=retries)
    break

  return op
