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

Storage Engine for interfacing with a cluster of Redis servers.
"""

import bisect
from collections import defaultdict
import itertools
import random
import traceback

import gevent
from gevent.queue import LifoQueue
from tellapart.third_party import redis
from tellapart.third_party.redis.connection import DefaultParser

from tellapart.storage.operation import Operation, CompoundOperation

# The number of times to retry a transaction due to a WatchError before giving
# up entirely and raising an exception. This should be set very high, since
# separate shards are independent transactions (i.e. a shard can fail, while
# other shards committed their changes, leaving the DB in an inconsistent
# state)
MAX_TRANSACTION_RETRIES = 15

# The Check-and-Set batch size. This controls the number of keys which will be
# locked, retrieved, and updated in a single transaction.
CAS_BATCH_SIZE = 50

class RedisServerEndpoint(object):
  """Redis Server end-point specification struct"""
  def __init__(self, host, port, vbucket_start, vbucket_end):
    """
    Args:
      host - Host name string of end-point.
      port - Port number on the host for the end-point.
      vbucket_start - First Virtual Bucket in the range the end-point handles.
      vbucket_end - Last Virtual Bucket in the range the end-point handles.
    """
    self.host = host
    self.port = port
    self.vbucket_start = vbucket_start
    self.vbucket_end = vbucket_end

class RedisConnectionPool(object):
  """Pool of Redis Connections that uses a gevent LifoQueue to block when a
  resource is not available.
  """

  def __init__(self, size, host, port, db=0, passwd=None, socket_timeout=None):
    """
    Args:
      size - Number of connections to maintain in the pool.
      host - The hostname to use for making connections.
      port - The port to use for making connections.
      db - The database number to connect to.
      passwd - The password to use for accessing the database.
      socket_timeout - The socket timeout value for connections.
    """
    self.size = size

    self.all = set()
    self.pool = LifoQueue(maxsize=self.size)

    for _ in xrange(self.size):
      connection = redis.Connection(
          host, port, db, passwd,
          socket_timeout,
          encoding='utf-8',
          encoding_errors='strict',
          parser_class=DefaultParser)
      self.all.add(connection)
      self.pool.put(connection)

  def get_connection(self, command_name, *keys, **options):
    """Get a connection from the pool. If no connection is available, this call
    will block.
    """
    return self.pool.get(timeout=60)

  def release(self, connection):
    """Return a connection to the pool.
    """
    if connection not in self.all:
      raise ValueError()

    self.pool.put(connection)

  def disconnect(self):
    """Close all the connections managed by this pool.
    """
    for connection in self.all:
      connection.disconnect()

class RedisEngine(object):
  """Storage Engine class for interfacing with a cluster of Redis servers.
  Supports Single/Batch Get/Put operations (i.e. key-value data), and Set
  Add/Get/Remove operations (i.e. as in a set() of objects).

  Values are transparently distributed across the cluster using Virtual Buckets
  on the hash of the key. Keys are hashed into one of a large number of Virtual
  Buckets. These buckets are assigned in ranges to specific shards. The cluster
  can be re-sharded by reassigning the Virtual Bucket ranges.

  Also supports transactions via optimistic locking. A transaction is initiated
  by specifying a set of keys to lock, and a callback function that will perform
  the work. See the documentation of BatchCheckAndMultiSet() for more details.

  NOTE: Each Set resides on a single end-point, so very large or high load Sets
  are not advisable.
  """

  def __init__(self, endpoints, num_vbuckets):
    """
    Args:
      endpoints - List of RedisServerEndpoint objects.
      num_vbuckets - The total number of configured Virtual Buckets.
    """
    self.num_vbuckets = num_vbuckets

    # Sort the endpoints by vbucket start, and make sure all the vbuckets
    # are accounted for.
    endpoints = sorted(endpoints, key=lambda e: e.vbucket_start)
    if endpoints[0].vbucket_start != 0:
      raise ValueError('First Virtual Bucket must be 0')

    if endpoints[-1].vbucket_end != self.num_vbuckets - 1:
      raise ValueError('Last Virtual Bucket does not match total')

    for i in xrange(0, len(endpoints) - 2):
      if endpoints[i].vbucket_end + 1 != endpoints[i + 1].vbucket_start:
        raise ValueError('Virtual Bucket range mismatch between end-points ' + \
            '%d and %d' % (i, i + 1))

    # Generate a map of vbucket start to redis client.
    self.shards = []
    self.vbucket_starts = []
    for endpoint in endpoints:
      pool = RedisConnectionPool(64, endpoint.host, endpoint.port)
      r = redis.StrictRedis(
          host=endpoint.host,
          port=endpoint.port,
          connection_pool=pool)
      self.shards.append(r)
      self.vbucket_starts.append(endpoint.vbucket_start)

  def _GetShardInfo(self, key):
    """For a given client key, calculate the associated shard number and
    virtual key.

    Args:
      key - Client key string.

    Returns:
      A tuple of (shard number, virtual key)
    """
    key_vbucket = self._GetVbucket(key)

    shard_num = bisect.bisect(self.vbucket_starts, key_vbucket) - 1
    vkey = self._MakeVkey(key, key_vbucket)

    return (shard_num, vkey)

  def _GetVbucket(self, key):
    """For a given client key, calculate the associated Virtual Bucket. This is
    the hash of the key string modded by the number of Virtual Buckets.

    Args:
      key - Client key string.

    Returns:
      Virtual bucket number.
    """
    return hash(key) % self.num_vbuckets

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
    """Perform an operation on each shard in the cluster, and aggregate the
    results. The callback will be invoked for each shard, with the keys, virtual
    keys, and values the belong to that shard.

    Args:
      key_value_tuples - List of tuples of (key, value) for the operation.
      shard_execute_fn - Callback for executing the desired operation in each
          shard. The signature is f(shard, keys, vkeys, values) => Operation

    Returns:
      A CompountOperation with the results of the queries. The response_value
      field is the combined response_value of each sub-operation (assuming all
      the sub-operations succeeded and returned a response_value)
    """
    # Compute the shard info for all the keys, and group them by the shard in
    # which they reside.
    vkeys_by_shard = defaultdict(list)
    for i, (key, val) in enumerate(key_value_tuples):
      (shard_num, vkey) = self._GetShardInfo(key)
      vkeys_by_shard[shard_num].append((i, key, vkey, val))

    # Split the request for each shard into a separate greenlet to allow the
    # requests to happen in parallel.
    op = CompoundOperation()
    responses = []

    def _ShardGreenletWraper(shard_num, vkey_tuples):
      indices, keys, vkeys, values = [list(i) for i in zip(*vkey_tuples)]

      try:
        sub_op = shard_execute_fn(self.shards[shard_num], keys, vkeys, values)
      except Exception:
        sub_op = Operation(success=False, traceback=traceback.format_exc())

      # Aggregate the results.
      op.AddOp(sub_op)
      if sub_op.response_value:
        responses.extend(zip(indices, sub_op.response_value))

    greenlets = [
        gevent.spawn(_ShardGreenletWraper, shard_num, vkey_tuples)
        for shard_num, vkey_tuples in vkeys_by_shard.iteritems()]

    gevent.joinall(greenlets)

    # Sort the combined responses back into the request order.
    responses = sorted(responses, key=lambda r: r[0])
    op.response_value = [r[1] for r in responses]

    return op

  def Get(self, key):
    """Return the value for a given key from the correct shard.

    Args:
      key - The key to lookup.

    Returns:
      Operation object with the result of the lookup.
    """
    # resubmit the Get operation as as Batch Get.
    op = self.BatchGet([key])

    # Unpack the batch operation results.
    if op.success and op.response_value:
      op.response_value = op.response_value[0]

    return op

  def BatchGet(self, keys):
    """Return the values for a set of keys, from across all shards.

    Args:
      keys - List of keys to lookup.

    Returns:
      CompoundOperation with the results of the lookups. The response_value
      field has the form [(key, value)., ...]
    """
    def _ShardGet(shard, keys, vkeys, values):
      pipe = shard.pipeline()
      map(pipe.get, vkeys)
      values = pipe.execute()
      return Operation(success=True, response_value=values)

    return self._ShardedOp([(key, None) for key in keys], _ShardGet)

  def Put(self, key, value):
    """Put a value in a key under the correct shard.

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
    """Put a batch of values into keys, each under the correct shard.

    Args:
      key_value_tuples - A list of tuples of the form (key, value).

    Returns:
      A CompoundOperation with the results of the queries.
    """
    def _ShardPut(shard, keys, vkeys, values):
      pipe = shard.pipeline()
      map(pipe.set, vkeys, values)
      successes = pipe.execute()
      return Operation(success=True, response_value=successes)

    return self._ShardedOp(key_value_tuples, _ShardPut)

  def Delete(self, key):
    """Delete a key from the correct shard.

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
    """Delete a set of keys, each from the correct shard.

    Args:
      keys - A list of keys to delete.

    Returns:
      A CompountOperation with the results of the queries.
    """
    def _ShardDelete(shard, keys, vkeys, values):
      pipe = shard.pipeline()
      map(pipe.delete, vkeys)
      successes = pipe.execute()
      return Operation(success=True, response_value=successes)

    return self._ShardedOp([(key, None) for key in keys], _ShardDelete)

  def SetAdd(self, key, *values):
    """Add values to the Set under key, in the correct shard.

    Args:
      key - The key of the Set.
      values - List of values to add to the Set.

    Returns:
      An Operation object with the result of the query. The response_value
      field contains a list of integers, the sum of which represents the number
      of new items added to the Set.
    """
    def _ShardSetAdd(shard, keys, vkeys, vals):
      pipe = shard.pipeline()
      map(pipe.sadd, vkeys, vals)
      added = pipe.execute()
      return Operation(success=True, response_value=added)

    return self._ShardedOp([(key, val) for val in values], _ShardSetAdd)

  def SetMembers(self, key):
    """Retrieve all the members of the Set at key.

    Args:
      key: - The key of the Set.

    Returns:
      An Operation object with the result of the query. The response_value
      field contains a set() object with the members of the Set.
    """
    def _ShardSetMembers(shard, keys, vkeys, values):
      members = shard.smembers(vkeys[0])
      return Operation(success=True, response_value=[members])

    op = self._ShardedOp([(key, None)], _ShardSetMembers)

    if op.success and op.response_value:
      op.response_value = set(op.response_value[0])

    return op

  def SetIsMember(self, key, value):
    """Test whether a value is a member of the Set at key.

    Args:
      key - The key of the Set.
      value - The String value to test membership in the Set.

    Returns:
      A boolean indicating whether the value is in the Set.
    """
    def _ShardSetMembers(shard, keys, vkeys, values):
      is_member = shard.sismember(vkeys[0], values[0])
      return Operation(success=True, response_value=[is_member])

    op = self._ShardedOp([(key, value)], _ShardSetMembers)

    if op.success and op.response_value:
      op.response_value = bool(op.response_value[0])

    return op

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
    keys = []

    try:
      # Modify the pattern to account for the vbucket prefix.
      shard_pattern = '*|%s' % pattern

      # Get the keys from each shard.
      for shard in self.shards:
        keys.extend(shard.keys(shard_pattern))

      # Split out the vbucket info from the returned keys.
      op = Operation(
          success=True,
          response_value=['|'.join(key.split('|')[1:]) for key in keys])

    except Exception:
      op = Operation(success=False, traceback=traceback.format_exc())

    return op

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
    def _ShardSetRemove(shard, keys, vkeys, values):
      pipe = shard.pipeline()
      map(pipe.srem, vkeys, values)
      removed = pipe.execute()
      return Operation(success=True, response_value=removed)

    return self._ShardedOp([(key, val) for val in values], _ShardSetRemove)

  def HashPut(self, key, field, value):
    """Put a value to a field of a Hashtable.

    Args:
      key - The key of the Hashtable
      field - Field in the Hashtable to set.
      value - Value to set.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardHashPut(shard, keys, vkeys, vals):
      added = shard.hset(vkeys[0], field, value)
      return Operation(success=True, response_value=[added])

    return self._ShardedOp([(key, None)], _ShardHashPut)

  def HashMultiPut(self, key, mapping):
    """Put a set of fields and values to a Hashtable.

    Args:
      key - The key of the Hashtable.
      mapping - Dictionaryt of keys and values to set.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardHashMultiPut(shard, keys, vkeys, vals):
      response = shard.hmset(vkeys[0], mapping)
      return Operation(success=True, response_value=[response])

    return self._ShardedOp([(key, mapping)], _ShardHashMultiPut)

  def HashDelete(self, key, field):
    """Delete a field from a Hashtable.

    Args:
      key - The key of the Hashtable.
      field - Field to delete from the Hashtable.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardHashDelete(shard, keys, vkeys, vals):
      added = shard.hdel(vkeys[0], field)
      return Operation(success=True, response_value=[added])

    return self._ShardedOp([(key, None)], _ShardHashDelete)

  def HashGet(self, key, field):
    """Retrieve the value stored at a field in a Hashtable.

    Args:
      key - The key of the Hashtable.
      field - Field in the Hashtable to retrieve.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardHashGet(shard, keys, vkeys, vals):
      result = shard.hget(vkeys[0], field)
      return Operation(success=True, response_value=[result])

    op = self._ShardedOp([(key, None)], _ShardHashGet)

    if op.success and op.response_value:
      op.response_value = op.response_value[0]

    return op

  def HashBatchGet(self, keys, field):
    """Retrieve the value of a field from several Hashtables.

    Args:
      keys - The keys of the Hashtables to lookup.
      field - The field to get from each Hashtable.

    Returns:

    """
    def _ShardHashBatchGet(shard, keys, vkeys, vals):
      pipe = shard.pipeline()
      for vkey in vkeys:
        pipe.hget(vkey, field)
      result = pipe.execute()
      return Operation(success=True, response_value=result)

    op = self._ShardedOp([(key, None) for key in keys], _ShardHashBatchGet)

    return op

  def HashGetAll(self, key):
    """Retrieve an entire Hashtable.

    Args:
      key - The key of the Hashtable.

    Returns:
      An Operation object with the result of the query.
    """
    def _ShardHashGetAll(shard, keys, vkeys, vals):
      result = shard.hgetall(vkeys[0])
      return Operation(success=True, response_value=[result])

    op = self._ShardedOp([(key, None)], _ShardHashGetAll)

    if op.success and op.response_value:
      op.response_value = op.response_value[0]

    return op

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

class _ShardCheckAndSetCallable(object):
  """Callable class to implement the per-shard logic for BatchCheckAnsMultiSet.
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
    greenlets = [
        gevent.spawn(
            self._ShardCheckAndSetBatch,
            shard,
            keys[i:i+CAS_BATCH_SIZE],
            vkeys[i:i+CAS_BATCH_SIZE],
            values[i:i+CAS_BATCH_SIZE])
        for i in xrange(0, len(keys), CAS_BATCH_SIZE)]

    gevent.joinall(greenlets)

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
        sub_pipe = pipe.pipeline(False)
        map(sub_pipe.get, vkeys)
        values = sub_pipe.execute()

        # Get the new values from the client.
        put_kv_tuples = []
        for i, (key, value) in enumerate(itertools.izip(keys, values)):
          put_kv_tuples.extend(self.get_updates_fn(key, value))

          # Yield periodically.
          if i % 1000 == 0:
            gevent.sleep(0)

        # Put the new values into the DB.
        pipe.multi()
        for key, value in put_kv_tuples:
          vbucket = self.engine._GetVbucket(key)
          vkey = self.engine._MakeVkey(key, vbucket)
          pipe.set(vkey, value)

        response = pipe.execute()

      except redis.WatchError:
        # Lock error occurred. Try the operation again.
        gevent.sleep(0.1 * retries + 0.1 * random.random())

        retries += 1
        if retries > MAX_TRANSACTION_RETRIES:
          raise

        continue

      finally:
        # Make sure we always reset the pipe.
        pipe.reset()

      # If we make it here without a WatchError, the operation succeeded.
      op = Operation(success=True, response_value=response, retries=retries)
      break

    return op
