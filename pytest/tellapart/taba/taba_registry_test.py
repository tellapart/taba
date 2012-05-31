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

Tests for the Taba Registry and Taba Handlers.
"""

import time
import unittest
import mox

# Compile Cython modules before importing them.
from tellapart.taba.util import misc_util
misc_util.BootstrapCython('unittest')

from tellapart.taba.handlers import percentile_counter_native #@UnresolvedImport
from tellapart.taba.handlers import moving_interval_counter
from tellapart.taba.handlers import percentile_counter
from tellapart.taba.handlers import buffer
from tellapart.taba.handlers.common_prefix_counter_group import \
    CommonPrefixCounterGroup
from tellapart.taba.handlers.expiry_string_taba import ExpiryStringTaba
from tellapart.taba.handlers.expiry_string_taba import ExpiryStringState
from tellapart.taba.handlers.moving_interval_counter import \
    MovingIntervalCounter
from tellapart.taba.handlers.moving_interval_counter_group import \
    MovingIntervalCounterGroup
from tellapart.taba.handlers.string_taba import StringTaba
from tellapart.taba.handlers.totals_counter import TotalsCounter

class TabaRegistryTestCase(mox.MoxTestBase):

  def testBufferTaba(self):
    """Test for BufferTaba"""
    client = 'c1'
    name = 'n1'
    events = [
      ('n1', 't1', ('v1', 23), 20),
      ('n1', 't1', ('v2', 25), 21),
      ('n1', 't1', ('v3', 26), 21),]

    expt_rend = ["n1\t[('v3', 21), ('v2', 21), ('v3', 21), ('v2', 21)]"]

    self.mox.StubOutWithMock(buffer, 'time')
    buffer.time.time().AndReturn(22)
    buffer.time.time().AndReturn(24)

    self.mox.ReplayAll()

    handler = buffer.BufferTaba()

    new = handler.NewState(client, name)
    state = handler.FoldEvents(new, events)
    proj = handler.ProjectState(state)
    agg = handler.Aggregate([proj, proj])
    rend = handler.Render(name, [agg])

    self.assertEqual(rend, expt_rend)

    self.mox.VerifyAll()

  def testStringTaba(self):
    """Test for StringTaba"""
    client = 'c1'
    name = 'n1'
    events = [
      ('n1', 't1', ['v1'], 20),
      ('n1', 't1', ['v2'], 21),]

    expt_rend = ["n1\t{(string: v2, frequency: 3)}"]

    handler = StringTaba()

    new = handler.NewState(client, name)
    state = handler.FoldEvents(new, events)
    proj = handler.ProjectState(state)
    agg = handler.Aggregate([proj, proj])
    agg = handler.Aggregate([agg, proj])
    rend = handler.Render(name, [agg])

    self.assertEqual(rend, expt_rend)

  def testTotalsHandler(self):
    """Test for TotalsCounter"""
    client = 'c1'
    name = 'n1'
    events = [
      ('n1', 't1', [5], 20),
      ('n1', 't1', [10], 21),]

    expt_rend = ["n1\t45.00\t6"]

    handler = TotalsCounter()

    new = handler.NewState(client, name)
    state = handler.FoldEvents(new, events)
    proj = handler.ProjectState(state)
    agg = handler.Aggregate([proj, proj])
    agg = handler.Aggregate([agg, proj])
    rend = handler.Render(name, [agg])

    self.assertEqual(rend, expt_rend)

  def testPercentileTotalsHandler(self):
    """Test for PercentileCounter"""
    client = 'c1'
    name = 'n1'
    events = [('n1', 't1', [i], i) for i in xrange(1, 201)]

    expt_rend = ['n1\t60300.00\t600\t[(0.25, 51.00), (0.50, 101.00), '
        '(0.75, 151.00), (0.90, 181.00), (0.95, 191.00), (0.99, 199.00)]']

    handler = percentile_counter.PercentileCounter()
    handler.server = misc_util.Bunch(GetAverageAppliedLatency=lambda: 0.0)

    new = handler.NewState(client, name)
    state = handler.FoldEvents(new, events)
    proj = handler.ProjectState(state)
    agg = handler.Aggregate([proj, proj])
    agg = handler.Aggregate([agg, proj])
    rend = handler.Render(name, [agg])

    self.assertEqual(rend, expt_rend)

  def testMovingIntervalCounter(self):
    """Test for MovingIntervalCounter"""
    client = 'c1'
    name = 'n1'
    events = [('n1', 't1', [i], i) for i in xrange(1, 501)]

    expt_rend = ["n1\t84690.00\t180"]

    self.mox.StubOutWithMock(moving_interval_counter.time, 'time')
    moving_interval_counter.time.time().AndReturn(1)
    moving_interval_counter.time.time().AndReturn(500)
    moving_interval_counter.time.time().AndReturn(500)

    self.mox.ReplayAll()

    handler = MovingIntervalCounter()
    handler.server = misc_util.Bunch(GetAverageAppliedLatency=lambda: 0.0)

    new = handler.NewState(client, name)
    state = handler.FoldEvents(new, events)
    proj = handler.ProjectState(state)
    agg = handler.Aggregate([proj, proj])
    agg = handler.Aggregate([agg, proj])
    rend = handler.Render(name, [agg])

    self.assertEqual(rend, expt_rend)

    self.mox.VerifyAll()

  def testMovingIntervalCounterGroup(self):
    """Test for MovingIntervalCounterGroup"""
    # Define basic input parameters.
    client = 'c1'
    name = 'n1'
    events = [('n1', 't1', [i], i) for i in xrange(1, 86401, 50)]

    expt_rend = [
        'n1_1m\t259053.00\t3',
        'n1_10m\t3098736.00\t36',
        'n1_1h\t18268416.00\t216',
        'n1_1d\t223824384.00\t5184',
        'n1_pct\t223824384.00\t5184\t[(0.25, 2501.00), (0.50, 5001.00), '
            '(0.75, 7501.00), (0.90, 9001.00), (0.95, 9501.00), '
            '(0.99, 9901.00)]']

    # Mock the time and random methods to make the test predictable.
    #
    # Note that this intentionally breaks percentile counter sampling so that
    # the test is deterministic. The sampling is tested throughly in the
    # the PercentileCounter unit test.
    self.mox.StubOutWithMock(percentile_counter_native.random, 'random')
    for _ in xrange(0, 86400/50 - 200):
      percentile_counter_native.random.random().AndReturn(2)

    self.mox.StubOutWithMock(moving_interval_counter.time, 'time')
    for _ in xrange(0, 4):
      moving_interval_counter.time.time().AndReturn(1)
    for _ in xrange(0, 8):
      moving_interval_counter.time.time().AndReturn(86400)

    # Execute the test pathway.
    self.mox.ReplayAll()

    handler = MovingIntervalCounterGroup()
    handler.server = misc_util.Bunch(GetAverageAppliedLatency=lambda: 0.0)

    new = handler.NewState(client, name)
    state = handler.FoldEvents(new, events)
    proj = handler.ProjectState(state)
    agg = handler.Aggregate([proj, proj])
    agg = handler.Aggregate([agg, proj])
    rend = handler.Render(name, [agg])

    for i in range(len(expt_rend)):
      self.assertEqual(rend[i], expt_rend[i])

    self.mox.VerifyAll()

  def testCommonPrefixCounterGroup(self):
    """Test for CommonPrefixCounterGroup"""
    client = 'c1'
    name = 'n1'
    events = [
      ('n1', 't1', ['s1', 1], 100),
      ('n1', 't1', ['s2', 1], 100)]

    expt_rend = [
      'n1_s2_1m\t3.00\t3',
      'n1_s2_10m\t3.00\t3',
      'n1_s2_1h\t3.00\t3',
      'n1_s2_1d\t3.00\t3',
      'n1_s2_pct\t3.00\t3\t[(0.25, 1.00), (0.50, 1.00), (0.75, 1.00), '
          '(0.90, 1.00), (0.95, 1.00), (0.99, 1.00)]',
      'n1_s1_1m\t3.00\t3',
      'n1_s1_10m\t3.00\t3',
      'n1_s1_1h\t3.00\t3',
      'n1_s1_1d\t3.00\t3',
      'n1_s1_pct\t3.00\t3\t[(0.25, 1.00), (0.50, 1.00), (0.75, 1.00), '
          '(0.90, 1.00), (0.95, 1.00), (0.99, 1.00)]',]

    # Mock the time and random methods to make the test predictable.
    #
    # Note that this intentionally breaks percentile counter sampling so that
    # the test is deterministic. The sampling is tested throughly in the
    # the PercentileCounter unit test.
    self.mox.StubOutWithMock(moving_interval_counter.time, 'time')
    for _ in xrange(0, 8):
      moving_interval_counter.time.time().AndReturn(1)
    for _ in xrange(0, 16):
      moving_interval_counter.time.time().AndReturn(101)

    self.mox.ReplayAll()

    handler = CommonPrefixCounterGroup()
    handler.server = misc_util.Bunch(GetAverageAppliedLatency=lambda: 0.0)

    new = handler.NewState(client, name)
    state = handler.FoldEvents(new, events)
    proj = handler.ProjectState(state)
    agg = handler.Aggregate([proj, proj])
    agg = handler.Aggregate([agg, proj])
    rend = handler.Render(name, [agg])

    for i in range(len(expt_rend)):
      self.assertEqual(rend[i], expt_rend[i])

    self.mox.VerifyAll()

  def testExpiryStringTaba(self):
    """Simple test for ExpiryStringTaba"""
    client = 'c1'
    name = 'n1'
    events = [
      ('n1', 't1', ['v1', 30], 20),
      ('n1', 't1', ['v2', 30], 21),]

    expt_rend = ["n1\t{(string: v2, frequency: 3)}"]

    self.mox.StubOutWithMock(time, 'time')
    time.time().MultipleTimes().AndReturn(25)

    self.mox.ReplayAll()

    handler = ExpiryStringTaba()

    new = handler.NewState(client, name)
    state = handler.FoldEvents(new, events)
    proj = handler.ProjectState(state)
    agg = handler.Aggregate([proj, proj])
    agg = handler.Aggregate([agg, proj])
    rend = handler.Render(name, [agg])

    self.assertEqual(rend, expt_rend)

    self.mox.VerifyAll()

  def testFoldingOfOldData(self):
    """Test that expired events will be ignored"""
    state = ExpiryStringState('foo', 300)

    # Don't ignore this one.
    input_event = ('foo', 'ExpiryStringTaba', ('foo', 500), 400)
    self.mox.StubOutWithMock(time, 'time')
    time.time().MultipleTimes().AndReturn(300)

    self.mox.ReplayAll()

    handler = ExpiryStringTaba()
    new_state = handler.FoldEvents(state, [input_event])

    self.assertEquals(new_state.expiry, 500)
    self.assertEquals(new_state.value, 'foo')

    self.mox.ResetAll()
    self.mox.StubOutWithMock(time, 'time')
    time.time().MultipleTimes().AndReturn(800)
    self.mox.ReplayAll()

    state = ExpiryStringState('bar', 300)

    new_state = handler.FoldEvents(state, [input_event])

    self.assertEquals(new_state.expiry, 300)
    self.assertEquals('bar', new_state.value)

if __name__ == '__main__':
  unittest.main()
