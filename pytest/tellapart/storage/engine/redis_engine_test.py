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
Unit tests for RedisEngine.
"""

import mox
import unittest

from tellapart.storage.engine import redis_engine
from tellapart.storage.operation import Operation

class RedisTestCase(mox.MoxTestBase):
  """Test case for tellapart.storage.engine.redis_engine.
  """

  def setUp(self):
    """Set up common unit test state.
    """
    mox.MoxTestBase.setUp(self)

    self.mox.StubOutWithMock(redis_engine.redis, 'StrictRedis', True)
    self.mox.StubOutClassWithMocks(redis_engine, 'RedisConnectionPool')

    self.vbuckets = 4
    self.endpoints = [
      redis_engine.RedisServerEndpoint('h1', 1111, 0, 1),
      redis_engine.RedisServerEndpoint('h1', 2222, 2, 3)]

    self.shard0 = self.mox.CreateMockAnything()
    self.shard1 = self.mox.CreateMockAnything()

    pool = redis_engine.RedisConnectionPool(64, 'h1', 1111)
    redis_engine.redis.StrictRedis(
        connection_pool=pool, host='h1', port=1111).AndReturn(self.shard0)

    pool = redis_engine.RedisConnectionPool(64, 'h1', 2222)
    redis_engine.redis.StrictRedis(
        connection_pool=pool, host='h1', port=2222).AndReturn(self.shard1)

  def tearDown(self):
    mox.MoxTestBase.tearDown(self)

  def testShardedOp(self):
    """Basic test for _ShardedOp"""

    kv_tuples = [('k1', 'v1'), ('k2', 'v2'), ('k3', None)]

    returns = [[True, True], [True]]
    callbacks = []
    def _callback(shard, keys, vkeys, values):
      callbacks.append((shard, keys, vkeys, values))
      return Operation(True, returns.pop(0))

    self.mox.ReplayAll()

    self.engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = self.engine._ShardedOp(kv_tuples, _callback)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, [True, True, True])

    self.assertTrue(callbacks[1][0] is self.shard1)
    self.assertEqual(callbacks[1][1], ['k1'])
    self.assertEqual(callbacks[1][2], ['2|k1'])
    self.assertEqual(callbacks[1][3], ['v1'])

    self.assertTrue(callbacks[0][0] is self.shard0)
    self.assertEqual(callbacks[0][1], ['k2', 'k3'])
    self.assertEqual(callbacks[0][2], ['1|k2', '0|k3'])
    self.assertEqual(callbacks[0][3], ['v2', None])

    self.mox.VerifyAll()

  def testShardedOpSingleFailure(self):
    """Test for _ShardedOp where a single operatin is not successful"""

    kv_tuples = [('k1', 'v1'), ('k2', 'v2'), ('k3', None)]

    results = [True, False]
    returns = [[True, True], None]
    callbacks = []
    def _callback(shard, keys, vkeys, values):
      callbacks.append((shard, keys, vkeys, values))
      return Operation(results.pop(0), returns.pop(0))

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine._ShardedOp(kv_tuples, _callback)

    self.assertFalse(op.success)
    self.assertEqual(op.response_value, [True, True])

    self.mox.VerifyAll()

  def testBatchGet(self):
    """Test BstchGet()"""
    keys = ['k1', 'k2', 'k3']

    mock_pipe0 = self.mox.CreateMockAnything()
    self.shard0.pipeline().AndReturn(mock_pipe0)
    mock_pipe0.get('1|k2')
    mock_pipe0.get('0|k3')
    mock_pipe0.execute().AndReturn(['v2', 'v3'])

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.get('2|k1')
    mock_pipe1.execute().AndReturn(['v1'])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.BatchGet(keys)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, ['v1', 'v2', 'v3'])

    self.mox.VerifyAll()

  def testGet(self):
    """Test Get()"""
    key = 'k1'

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.get('2|k1')
    mock_pipe1.execute().AndReturn(['v1'])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.Get(key)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, 'v1')

    self.mox.VerifyAll()

  def testBatchPut(self):
    """Test BatchPut()"""
    kv_tuples = [('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')]

    mock_pipe0 = self.mox.CreateMockAnything()
    self.shard0.pipeline().AndReturn(mock_pipe0)
    mock_pipe0.set('1|k2', 'v2')
    mock_pipe0.set('0|k3', 'v3')
    mock_pipe0.execute().AndReturn([True, True])

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.set('2|k1', 'v1')
    mock_pipe1.execute().AndReturn([True])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.BatchPut(kv_tuples)

    self.assertTrue(op.success)
    self.assertEqual(
        op.response_value,
        [True, True, True])

    self.mox.VerifyAll()

  def testPut(self):
    """Test Put()"""
    key = 'k1'
    val = 'v1'

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.set('2|k1', 'v1')
    mock_pipe1.execute().AndReturn([True])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.Put(key, val)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, True)

    self.mox.VerifyAll()

  def testBatchDelete(self):
    """Test BatchDelete()"""
    keys = ['k1', 'k2', 'k3']

    mock_pipe0 = self.mox.CreateMockAnything()
    self.shard0.pipeline().AndReturn(mock_pipe0)
    mock_pipe0.delete('1|k2')
    mock_pipe0.delete('0|k3')
    mock_pipe0.execute().AndReturn([True, True])

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.delete('2|k1')
    mock_pipe1.execute().AndReturn([True])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.BatchDelete(keys)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, [True, True, True])

    self.mox.VerifyAll()

  def testDelete(self):
    """Test Delete()"""
    key = 'k1'

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.delete('2|k1')
    mock_pipe1.execute().AndReturn([True])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.Delete(key)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, True)

    self.mox.VerifyAll()

  def testSetAdd(self):
    """Test SetAdd()"""
    key = 's1'
    values = ['v1', 'v2']

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.sadd('2|s1', 'v1')
    mock_pipe1.sadd('2|s1', 'v2')
    mock_pipe1.execute().AndReturn([1, 1])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.SetAdd(key, *values)

    self.assertTrue(op.success)

    self.mox.VerifyAll()

  def testSetMembers(self):
    """Test SetMembers()"""
    key = 's1'

    self.shard1.smembers('2|s1').AndReturn(['v1', 'v2', 'v3'])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.SetMembers(key)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['v1', 'v2', 'v3']))

    self.mox.VerifyAll()

  def testSetIsMember(self):
    """Test SetMembers()"""
    key = 's1'
    val1 = 'v1'
    val2 = 'v2'

    self.shard1.sismember('2|s1', val1).AndReturn(0)
    self.shard1.sismember('2|s1', val2).AndReturn(1)

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)

    op = engine.SetIsMember(key, val1)
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, False)

    op = engine.SetIsMember(key, val2)
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, True)

    self.mox.VerifyAll()

  def testSetRemove(self):
    """Test SetRemove()"""
    key = 's1'
    values = ['v1', 'v2']

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.srem('2|s1', 'v1')
    mock_pipe1.srem('2|s1', 'v2')
    mock_pipe1.execute().AndReturn([1, 1])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.SetRemove(key, *values)

    self.assertTrue(op.success)

    self.mox.VerifyAll()

  def testHashPut(self):
    """Test HashPut()"""
    key = 'h1'
    field = 'f1'
    value = 'v1'

    self.shard1.hset('3|h1', 'f1', 'v1').AndReturn(1)

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.HashPut(key, field, value)

    self.assertTrue(op.success)

    self.mox.VerifyAll()

  def testHashGet(self):
    """Test HashGet()"""
    key = 'h1'
    field = 'f1'
    value = 'v1'

    self.shard1.hget('3|h1', 'f1').AndReturn('v1')

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.HashGet(key, field)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, value)

    self.mox.VerifyAll()

  def testHashMultiPut(self):
    """Test HashMultiPut()"""
    key = 'h1'
    mapping = {'f1': 'v1', 'f2': 'v2'}

    self.shard1.hmset('3|h1', {'f1': 'v1', 'f2': 'v2'}).AndReturn(3)

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.HashMultiPut(key, mapping)

    self.assertTrue(op.success)

    self.mox.VerifyAll()

  def testHashDelete(self):
    """Test HashDelete()"""
    key = 'h1'
    field = 'f1'

    self.shard1.hdel('3|h1', 'f1').AndReturn(True)

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.HashDelete(key, field)

    self.assertTrue(op.success)

    self.mox.VerifyAll()

  def testHashBatchGet(self):
    """Test HashBatchGet()"""
    keys = ['h1', 'h2']
    field = 'f1'

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline().AndReturn(mock_pipe1)
    mock_pipe1.hget('3|h1', 'f1')
    mock_pipe1.execute().AndReturn(['v1'])

    mock_pipe0 = self.mox.CreateMockAnything()
    self.shard0.pipeline().AndReturn(mock_pipe0)
    mock_pipe0.hget('0|h2', 'f1')
    mock_pipe0.execute().AndReturn(['v2'])

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.HashBatchGet(keys, field)

    self.assertTrue(op.success)
    self.assertEquals(op.response_value, ['v1', 'v2'])

    self.mox.VerifyAll()

  def testHashGetAll(self):
    """Test HashGetAll()"""
    key = 'h1'
    mapping = {'f1': 'v1'}

    self.shard1.hgetall('3|h1').AndReturn({'f1': 'v1'})

    self.mox.ReplayAll()

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.HashGetAll(key)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, mapping)

    self.mox.VerifyAll()

  def testBatchCheckAndMultiSetSimple(self):
    """Test BatchCheckAndMultiSet() under normal conditions"""
    kv_dict = {'k1': 'v11', 'k2': 'v21', 'k3': 'v31'}

    mock_pipe0 = self.mox.CreateMockAnything()
    self.shard0.pipeline(True).AndReturn(mock_pipe0)
    mock_pipe0.watch('1|k2', '0|k3')

    mock_sub_pipe0 = self.mox.CreateMockAnything()
    mock_pipe0.pipeline(False).AndReturn(mock_sub_pipe0)
    mock_sub_pipe0.get('1|k2')
    mock_sub_pipe0.get('0|k3')
    mock_sub_pipe0.execute().AndReturn(['v20', 'v30'])

    mock_pipe0.multi()
    mock_pipe0.set('1|k2', 'v21')
    mock_pipe0.set('0|k3', 'v31')
    mock_pipe0.execute().AndReturn([True, True])
    mock_pipe0.reset()

    mock_pipe1 = self.mox.CreateMockAnything()
    self.shard1.pipeline(True).AndReturn(mock_pipe1)
    mock_pipe1.watch('2|k1')

    mock_sub_pipe1 = self.mox.CreateMockAnything()
    mock_pipe1.pipeline(False).AndReturn(mock_sub_pipe1)
    mock_sub_pipe1.get('2|k1')
    mock_sub_pipe1.execute().AndReturn(['v10'])

    mock_pipe1.multi()
    mock_pipe1.set('2|k1', 'v11')
    mock_pipe1.execute().AndReturn([True])
    mock_pipe1.reset()

    self.mox.ReplayAll()

    old_vals = []
    def callback(key, old_value):
      old_vals.append(old_value)
      return [(key, kv_dict[key])]

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.BatchCheckAndMultiSet(sorted(kv_dict.keys()), callback)
    return op

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, [True, True, True])
    self.assertEqual(len(old_vals), 3)

    self.mox.VerifyAll()

  def testBatchCheckAndMultiSetLockError(self):
    """Test BatchCheckAndSet() when a lock error occurs"""
    kv_dict = {'k1': 'v11', 'k2': 'v21', 'k3': 'v31'}

    # Shard 0, first attempt.
    mock_pipe00 = self.mox.CreateMockAnything('pipe00')
    self.shard0.pipeline(True).AndReturn(mock_pipe00)
    mock_pipe00.watch('1|k2', '0|k3')

    mock_sub_pipe00 = self.mox.CreateMockAnything('sub-pipe00')
    mock_pipe00.pipeline(False).AndReturn(mock_sub_pipe00)
    mock_sub_pipe00.get('1|k2')
    mock_sub_pipe00.get('0|k3')
    mock_sub_pipe00.execute().AndReturn(['v20', 'v30'])

    mock_pipe00.multi()
    mock_pipe00.set('1|k2', 'v21')
    mock_pipe00.set('0|k3', 'v31')
    mock_pipe00.execute().AndRaise(redis_engine.redis.WatchError)
    mock_pipe00.reset()

    # Shard 0, second attempt.
    mock_pipe01 = self.mox.CreateMockAnything('pipe01')
    self.shard0.pipeline(True).AndReturn(mock_pipe01)
    mock_pipe01.watch('1|k2', '0|k3')

    mock_sub_pipe01 = self.mox.CreateMockAnything('sub-pipe01')
    mock_pipe01.pipeline(False).AndReturn(mock_sub_pipe01)
    mock_sub_pipe01.get('1|k2')
    mock_sub_pipe01.get('0|k3')
    mock_sub_pipe01.execute().AndReturn(['v20', 'v30'])

    mock_pipe01.multi()
    mock_pipe01.set('1|k2', 'v21')
    mock_pipe01.set('0|k3', 'v31')
    mock_pipe01.execute().AndReturn([True, True])
    mock_pipe01.reset()

    # Shard 1.
    mock_pipe1 = self.mox.CreateMockAnything('pipe1')
    self.shard1.pipeline(True).AndReturn(mock_pipe1)
    mock_pipe1.watch('2|k1')

    mock_sub_pipe1 = self.mox.CreateMockAnything('sub-pipe1')
    mock_pipe1.pipeline(False).AndReturn(mock_sub_pipe1)
    mock_sub_pipe1.get('2|k1')
    mock_sub_pipe1.execute().AndReturn(['v10'])

    mock_pipe1.multi()
    mock_pipe1.set('2|k1', 'v11')
    mock_pipe1.execute().AndReturn([True])
    mock_pipe1.reset()

    self.mox.ReplayAll()

    old_vals = []
    def callback(key, old_value):
      old_vals.append(old_value)
      return [(key, kv_dict[key])]

    engine = redis_engine.RedisEngine(self.endpoints, self.vbuckets)
    op = engine.BatchCheckAndMultiSet(sorted(kv_dict.keys()), callback)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, [True, True, True])
    self.assertEqual(len(old_vals), 5)

    self.mox.VerifyAll()

if __name__ == '__main__':
  unittest.main()
