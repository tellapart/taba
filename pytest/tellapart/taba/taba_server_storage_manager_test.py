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

Unit tests for TabaServerStorageManager.
"""

import mox
import unittest

from tellapart.storage.engine.memory_redis_engine import MemoryRedisEngine
from tellapart.storage.operation import Operation
from tellapart.taba.server import taba_server_storage_manager
from tellapart.taba.server import taba_state
from tellapart.taba.server.taba_server_storage_manager import \
    TabaServerStorageManager
from tellapart.taba.server.taba_state import StateStruct

S1_PACKED = "N\x00\x00\x00\x04 \x06\xf7x^k`J.I\xcd\xc9I,H,*\xd1+ILJ\xd4+N-*K-" \
    "\x02\xb3\xe3\x8bK\x12KR\xb9\x82AdpIQir\tW!\xa3fc!Sm!\xb3F(;PYqf~^!\x8b7C" \
    "({AbeN~bJ!k(S\xb1a![i\x92\x1e\x00\xe5f\x1f\x17"

S2_PACKED = "N\x00\x00\x00\xfb\x1fi\xf6x^k`J.I\xcd\xc9I,H,*\xd1+ILJ\xd4+N-*K-" \
    "\x02\xb3\xe3\x8bK\x12KR\xb9\x82AdpIQir\tW!\xa3fc!Sm!\xb3F(;PYqf~^!\x8b7C" \
    "({AbeN~bJ!k(S\xb1Q![i\x92\x1e\x00\xe5l\x1f\x18"

S3_PACKED = "N\x00\x00\x00\" \xdc\xf7x^k`J.I\xcd\xc9I,H,*\xd1+ILJ\xd4+N-*K-" \
    "\x02\xb3\xe3\x8bK\x12KR\xb9\x82AdpIQir\tW!\xa3fc!Sm!\xb3F(;PYqf~^!\x8b7C" \
    "({AbeN~bJ!k(S\xb1q![i\x92\x1e\x00\xe5r\x1f\x19"

class TabaServerStorageManagerTestCase(mox.MoxTestBase):
  """Test case for tellapart.taba.server.taba_server_storage_manager.
  """

  def setUp(self):
    """Set up common unit test state.
    """
    mox.MoxTestBase.setUp(self)

    self.engine = MemoryRedisEngine(None, None)
    self.vssm = TabaServerStorageManager(self.engine)

  def tearDown(self):
    mox.MoxTestBase.tearDown(self)

  def testStateSerde(self):
    """Test State Serialization and Deserialization"""
    STATE = "123456789012345678901234567890"
    VERSION = 1
    state_struct = taba_state.StateStruct(payload=STATE, version=VERSION)

    state_packed = taba_state.PackState(state_struct)
    state_unpacked = taba_state.UnpackState(state_packed)

    self.assertEqual(state_unpacked.payload, STATE)
    self.assertEqual(state_unpacked.version, VERSION)

  def testStateSerdeFailure(self):
    """Test that the checksum detects errors"""
    STATE = "123456789012345678901234567890"
    VERSION = 1

    state_struct = taba_state.StateStruct(payload=STATE, version=VERSION)
    state_packed = taba_state.PackState(state_struct)

    # Simulate an error.
    state_packed = ''.join([state_packed[:20], '\x00', state_packed[21:]])

    try:
      taba_state.UnpackState(state_packed)
      self.fail("Unpacking should have failed")

    except ValueError:
      pass

  def testStatePutSimple(self):
    """Test State Put under normal conditions"""

    TUPLES = [
        (('c1', 'n1'), StateStruct('s1', 0)),
        (('c2', 'n2'), StateStruct('s2', 0)),]

    # Batch Put
    op = self.vssm.StatePutBatch(TUPLES)

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, [True, True])

    self.assertEqual(self.engine.val_dict['state:c1:n1'], S1_PACKED)
    self.assertEqual(self.engine.val_dict['state:c2:n2'], S2_PACKED)

    self.assertEqual(self.engine.val_dict['clients'], set(['c1', 'c2']))

    self.assertEqual(self.engine.val_dict['names:all'], set(['n1', 'n2']))
    self.assertEqual(self.engine.val_dict['names:c1'], set(['n1']))
    self.assertEqual(self.engine.val_dict['names:c2'], set(['n2']))

    # Single Put
    op = self.vssm.StatePut('c1', 'n3', StateStruct('s3', 0))

    self.assertTrue(op.success)
    self.assertEqual(op.response_value, True)

    self.assertEqual(self.engine.val_dict['state:c1:n1'], S1_PACKED)
    self.assertEqual(self.engine.val_dict['state:c2:n2'], S2_PACKED)
    self.assertEqual(self.engine.val_dict['state:c1:n3'], S3_PACKED)
    self.assertEqual(self.engine.val_dict['clients'], set(['c1', 'c2']))
    self.assertEqual(self.engine.val_dict['names:all'], set(['n1', 'n2', 'n3']))
    self.assertEqual(self.engine.val_dict['names:c1'], set(['n1', 'n3']))
    self.assertEqual(self.engine.val_dict['names:c2'], set(['n2']))

  def testStatePutFail(self):
    """Test State Put when the operation fails"""

    TUPLES = [
        (('c1', 'n1'), StateStruct('s1', 0)),
        (('c2', 'n2'), StateStruct('s2', 0)),]

    enc_tuples = [
        ('state:c1:n1', S1_PACKED),
        ('state:c2:n2', S2_PACKED),]

    self.mox.StubOutWithMock(taba_server_storage_manager, 'LOG')
    taba_server_storage_manager.LOG.error(mox.IgnoreArg()).MultipleTimes()

    self.mox.StubOutWithMock(self.engine, 'BatchPut')
    self.engine.BatchPut(enc_tuples).AndReturn(Operation(False))

    self.mox.ReplayAll()

    op = self.vssm.StatePutBatch(TUPLES)

    self.assertFalse(op.success)
    self.assertTrue(op.response_value is None)

    self.assertEqual(self.engine.val_dict.get('state:c1:n1'), None)
    self.assertEqual(self.engine.val_dict.get('state:c2:n2'), None)
    self.assertEqual(self.engine.val_dict.get('clients'), None)
    self.assertEqual(self.engine.val_dict.get('names:all'), None)
    self.assertEqual(self.engine.val_dict.get('names:c1'), None)
    self.assertEqual(self.engine.val_dict.get('names:c2'), None)

    self.mox.VerifyAll()

  def testStateGet(self):
    """Test State Get"""
    TUPLES = [
        (('c1', 'n1'), StateStruct('s1', 0)),
        (('c2', 'n2'), StateStruct('s2', 0)),]

    self.vssm.StatePutBatch(TUPLES)

    op = self.vssm.StateGetBatch([('c1', 'n1'), ('c2', 'n2')])
    self.assertTrue(op.success)
    self.assertEqual(
        op.response_value,
        [(('c1', 'n1'), StateStruct('s1', 0)),
         (('c2', 'n2'), StateStruct('s2', 0))])

    op = self.vssm.StateGetBatch([('c1', 'n1'), ('c2', 'n2'), ('c3', 'n3')])
    self.assertTrue(op.success)
    self.assertEqual(
        op.response_value,
        [(('c1', 'n1'), StateStruct('s1', 0)),
         (('c2', 'n2'), StateStruct('s2', 0)),
         (('c3', 'n3'), None)])

    op = self.vssm.StateGet('c2', 'n2')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, (('c2', 'n2'), StateStruct('s2', 0)))

    op = self.vssm.StateGet('c3', 'n2')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, (('c3', 'n2'), None))

  def testStateDelete(self):
    """Test State Delete"""
    tuples = [
        (('c1', 'n1'), StateStruct('s1', 0)),
        (('c2', 'n2'), StateStruct('s2', 0)),
        (('c3', 'n3'), StateStruct('s3', 0)),]

    self.vssm.StatePutBatch(tuples)

    op = self.vssm.StateGetBatch([('c1', 'n1'), ('c2', 'n2'), ('c3', 'n3')])
    self.assertTrue(op.success)
    self.assertEqual(
        op.response_value,
        [(('c1', 'n1'), StateStruct('s1', 0)),
         (('c2', 'n2'), StateStruct('s2', 0)),
         (('c3', 'n3'), StateStruct('s3', 0)),])

    op = self.vssm.StateDeleteBatch([('c1', 'n1'), ('c2', 'n2')])
    self.assertTrue(op.success)

    op = self.vssm.StateGetBatch([('c1', 'n1'), ('c2', 'n2'), ('c3', 'n3')])
    self.assertTrue(op.success)
    self.assertEqual(
        op.response_value,
        [(('c1', 'n1'), None),
         (('c2', 'n2'), None),
         (('c3', 'n3'), StateStruct('s3', 0))])

    op = self.vssm.StateDelete('c3', 'n3')
    self.assertTrue(op.success)

    op = self.vssm.StateGetBatch([('c1', 'n1'), ('c2', 'n2'), ('c3', 'n3')])
    self.assertTrue(op.success)
    self.assertEqual(
        op.response_value,
        [(('c1', 'n1'), None), (('c2', 'n2'), None), (('c3', 'n3'), None)])

  def testStateAndProjectionUpdateBatchTx(self):
    """Test for StateUpdateBatchTx()"""

    # Put some values to start.
    tuples_put = [
        (('c1', 'n1'), StateStruct('s1', 0)),
        (('c2', 'n2'), StateStruct('s2', 0)),]

    self.vssm.StatePutBatch(tuples_put)

    # Update a portion of the values.
    updates = {
        ('c2', 'n2'): StateStruct('s2u', 0),
        ('c2', 'n3'): StateStruct('s3u', 0),}

    old_states = {}
    def _callback(client_id, name, old_state):
      old_states[(client_id, name)] = old_state
      return (updates[(client_id, name)])

    op = self.vssm.StateUpdateBatchTx(updates.keys(), _callback)

    # Check that everything lines-up.
    self.assertTrue(op.success)
    self.assertEqual(len(old_states), 2)
    self.assertEqual(
        old_states,
        {('c2', 'n2'): StateStruct('s2', 0), ('c2', 'n3'): None})

    op = self.vssm.StateGetBatch([('c1', 'n1'), ('c2', 'n2'), ('c2', 'n3')])
    self.assertTrue(op.success)
    self.assertEqual(
        op.response_value,
        [(('c1', 'n1'), StateStruct('s1', 0)),
         (('c2', 'n2'), StateStruct('s2u', 0)),
         (('c2', 'n3'), StateStruct('s3u', 0))])

    op = self.vssm.ClientIdsGet()
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['c1', 'c2']))

    op = self.vssm.TabaNamesForAllGet()
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n1', 'n2', 'n3']))

    op = self.vssm.TabaNamesForClientGet('c1')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n1']))

    op = self.vssm.TabaNamesForClientGet('c2')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n2', 'n3']))

  def testStateIterators(self):
    """Test for State Iterators"""

    tuples_put = [
        (('c1', 'n1'), StateStruct('s11', 0)),
        (('c1', 'n2'), StateStruct('s12', 0)),
        (('c2', 'n1'), StateStruct('s21', 0)),
        (('c2', 'n2'), StateStruct('s22', 0)),]

    self.vssm.StatePutBatch(tuples_put)

    vals = [v for v in self.vssm.StateIterator()]
    self.assertEqual(len(vals), len(tuples_put))
    for tuple in tuples_put:
      self.assertTrue(tuple in vals)

    vals = [v for v in self.vssm.StateIteratorForClient('c1')]
    self.assertEqual(len(vals), 2)
    for tuple in tuples_put[:2]:
      self.assertTrue(tuple in vals)

    vals = [v for v in self.vssm.StateIteratorForName('n1')]
    self.assertEqual(len(vals), 2)
    for tuple in tuples_put[:2:2]:
      self.assertTrue(tuple in vals)

  def testTabaNames(self):
    """Test for working with Taba Names"""
    op = self.vssm.TabaNamesForClientAdd('c1', ['n1', 'n2'])
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, 2)

    op = self.vssm.TabaNamesForClientGet('c1')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n1', 'n2']))

    op = self.vssm.TabaNamesForClientAdd('c1', ['n1', 'n3'])
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, 1)

    op = self.vssm.TabaNamesForClientGet('c1')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n1', 'n2', 'n3']))

    op = self.vssm.TabaNamesForClientAdd('c2', ['n1', 'n2', 'n3'])
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, 3)

    op = self.vssm.TabaNamesForClientGet('c2')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n1', 'n2', 'n3']))

    op = self.vssm.TabaNamesForClientRemove('c1', ['n1', 'n4'])
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, 1)

    op = self.vssm.TabaNamesForClientGet('c1')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n2', 'n3']))

    op = self.vssm.TabaNamesForAllAdd(['n1', 'n2'])
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, 2)

    op = self.vssm.TabaNamesForAllGet()
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n1', 'n2']))

    op = self.vssm.TabaNamesForAllAdd(['n1', 'n3'])
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, 1)

    op = self.vssm.TabaNamesForAllGet()
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n1', 'n2', 'n3']))

    op = self.vssm.TabaNamesForAllRemove(['n1', 'n4'])
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, 1)

    op = self.vssm.TabaNamesForAllGet()
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, set(['n2', 'n3']))

  def testTabaType(self):
    """Test for working with Taba Types"""
    tuples = [
        ('n1', 't1'),
        ('n2', 't2'),]

    op = self.vssm.TabaTypePutBatch(tuples)
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, [True, True])

    op = self.vssm.TabaTypePut('n3', 't3')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, True)

    op = self.vssm.TabaTypeGetBatch(['n1', 'n2', 'n3', 'n4'])
    self.assertTrue(op.success)
    self.assertEqual(
        op.response_value,
        [('n1', 't1'), ('n2', 't2'), ('n3', 't3'), ('n4', None)])

    op = self.vssm.TabaTypeGet('n1')
    self.assertTrue(op.success)
    self.assertEqual(op.response_value, ('n1', 't1'))

if __name__ == '__main__':
  unittest.main()
