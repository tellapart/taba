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

"""Classes and methods for managing State objects.
"""

import cPickle as pickle
import ctypes
import itertools
import logging
import operator
import re
import traceback
import zlib

import gevent
import redis

from taba.server.model import model_provider
from taba.server.model import tab_registry
from taba.server.storage import util
from taba.util import misc_util
from taba.util import thread_util
from taba.util.thread_util import YieldByCount

LOG = logging.getLogger(__name__)

# Redis Key patterns for State objects.
KEY_PATTERN_STATE = 'St%d'

KEY_PATTERN_CID_NID_INDEX = 'InC%s'

# Special Hash field for the all-CID aggregate.
ALL_CID_FIELD = '__all__'

# Marker constants for Serialized State types.
STATE_MARK_LIGHT = 21
STATE_MARK_ZIP = 78

class StateManager(object):
  """Manager class for manipulating and accessing States.
  """

  def __init__(self, engine):
    """
    Args:
      engine - RedisEngine compatible storage manager.
    """
    self.engine = engine

  def GetGroupLatency(self):
    """Retrieve the current end-to-end Taba Server cluster event latency.

    Returns:
      Event latency in seconds.
    """
    return model_provider.GetLatency().GetClusterLatency()

  ###################################################################
  # In-bound Data Methods
  ###################################################################

  def ConvertNewEvents(self, client, cid, name, nid, tab_type, events):
    """Create a partial Serialized State binary string for set of Events.

    Args:
       client - Client Name the Events came from.
       cid - CID for the Client Name.
       name - Tab Name of the Events.
       nid - NID of the Tab Name.
       tab_type - TabType for the Tab Name.
       events - List of Events.

    Returns:
      Serialized State binary string.
    """
    # Retrieve the Handler for the Tab Type.
    handler = tab_registry.GetHandler(tab_type)
    if handler is None:
      LOG.error("Unknown Tab Type '%s'" % tab_type)
      return
    handler.server = self

    # Create the State Buffer with the Events folded in.
    state_buffer = handler.NewState(client, name)
    state_buffer = handler.FoldEvents(state_buffer, events)

    # Serialize the State Buffer.
    state_struct = StateStruct(state_buffer, handler.CURRENT_VERSION, tab_type)
    serialized_state = PackState(state_struct)

    return serialized_state

  def ReduceInto(self, nid, cid_to_sstate, tab_type):
    """For a group of Serialized States for an NID, reduce them into the main
    State in the database.

    Args:
      nid - NID of the State.
      cid_to_sstate - Dict of {CID: [Serialized States]}
      tab_type - Tab Type of the NID.
    """
    # Retrieve the Handler for the Tab Type.
    handler = tab_registry.GetHandler(tab_type)
    if handler is None:
      LOG.error("Unknown Tab Type '%s'" % tab_type)
      return
    handler.server = self

    # Deserialize the buffered state parts.
    cid_to_state_parts = {}
    all_state_parts = []
    for cid, serialized_state_parts in cid_to_sstate.iteritems():
      cid_state_parts = [
          UnpackAndUpgrade(serialized_state_part, handler)
          for serialized_state_part in YieldByCount(serialized_state_parts, 10)]

      cid_to_state_parts[cid] = cid_state_parts
      all_state_parts.extend(cid_state_parts)

    # Callback which updates the values of the State Hash from the DB.
    def _update_fn(current_vals):
      updated_vals = {}

      for cid, serialized_state in YieldByCount(current_vals.iteritems(), 10):
        if cid == ALL_CID_FIELD:
          new_state_parts = all_state_parts
        else:
          new_state_parts = cid_to_state_parts[cid]

        if not new_state_parts:
          continue

        if serialized_state is not None:
          state = UnpackAndUpgrade(serialized_state, handler)
        else:
          state = new_state_parts.pop()

        if state.tab_type != tab_type:
          LOG.error('Type mismatch for N%s C%s (Expecting %s, Got %s)!' %
              (nid, cid, tab_type, state.tab_type))
          continue

        # Create a list of the main state and all the parts to reduce together.
        all_parts = [state.payload]
        all_parts.extend(s.payload for s in new_state_parts)

        # Call the Handler to perform the Reduction.
        state.payload = handler.Reduce(all_parts)
        updated_vals[cid] = PackState(state)

      return updated_vals

    fields = list(cid_to_sstate.keys())
    fields.append(ALL_CID_FIELD)

    op = self.engine.HashCheckAndMultiSet(
        KEY_PATTERN_STATE % nid,
        fields,
        _update_fn)

    # Update the CID -> NID index
    if op.success:
      for cid in cid_to_sstate.keys():
        self.AddNidForCid(cid, nid)

  def Delete(self, nid):
    """Delete the State for an NID.

    Args:
      nid - NID of the State to delete.
    """
    op = util.StrictOp('getting fields for hash to delete',
        self.engine.HashFields,
        KEY_PATTERN_STATE % nid)

    # Remove the main State Hash.
    util.StrictOp('deleting State Hash for %d' % nid,
        self.engine.Delete,
        KEY_PATTERN_STATE % nid)

    # Remove the NId from all the CID indexes it was member of.
    for cid in op.response_value:
      util.CheckedOp('removing NID %d from CID index %d' % (nid, cid),
          self.engine.SetRemove,
          KEY_PATTERN_CID_NID_INDEX % cid, [nid])

  ###################################################################
  # CID -> NIDs Index Methods
  ###################################################################

  def AddNidForCid(self, cid, nid):
    """Add an NID to the CID -> NID index.

    Args:
      cid - CID to add to.
      nid - NID to add.
    """
    util.CheckedOp('adding NID(%d) for CID(%d)' % (nid, cid),
        self.engine.SetAdd,
        KEY_PATTERN_CID_NID_INDEX % cid, [nid])

  def GetNidsForCid(self, cid):
    """Retrieve all the NIDs for a CID.

    Args:
      cid - CID to retrieve NIDs for.

    Returns:
      List of NIDs.
    """
    op = util.StrictOp('retrieving all NIDs for CID(%d)' % cid,
        self.engine.SetMembers,
        KEY_PATTERN_CID_NID_INDEX % cid)

    return map(int, op.response_value)

  def RemoveNidForCid(self, cid, nid):
    """Remove an NID from the CID -> NIDs index. This method checks whether a
    State exists for (NID, CID) before removing the entry from the index (and
    will refuse to remove the mapping is said State exists).

    Args:
      cid - CID index to update.
      nid - NID to remove from the index.

    Returns:
      Operation object.
    """
    # Callback which returns whether a State entry for (NID, CID) exists. If
    # it does, the conditional SetRemove will not proceed.
    #
    # NOTE: this approach is used instead of a pure Redis transaction since the
    # CID -> NIDs index and the NID State Hash can reside on separate shards.
    def _check_for_update():
      op = util.StrictOp('Checking CID %s in NID %d state' % (cid, nid),
          self.engine.HashGet,
          KEY_PATTERN_STATE % nid, cid)

      return not bool(op.response_value)

    op = self.engine.SetRemoveConditional(
        KEY_PATTERN_CID_NID_INDEX % cid,
        [nid],
        _check_for_update)

    return op

  ###################################################################
  # Maintenance Methods - Garbage Collector
  ###################################################################

  def RunGarbageCollector(self):
    """Traverse the States in the database, removing any that are Prunable.

    NOTE: This is a preliminary implementation, designed to save memory. A full
    implementation will also need to be able to fully delete / de-activate CIDs
    and NIDs.
    """
    LOG.info('GC: Starting')

    nids = sorted(list(model_provider.GetNames().GetAllNids()))

    for nid_slice in misc_util.Slicer(nids, 10):
      gevent.joinall([
          gevent.spawn(self._CollectSingleNid, nid)
          for nid in nid_slice])

    LOG.info('GC: Finished')

  def _CollectSingleNid(self, nid):
    """Run garbage collection on a single NID.

    Args:
      nid - NID to collect.
    """
    cids_removed = set()
    expected_type = model_provider.GetNames().GetTypes([nid])[nid]

    # Callback to perform garbage collection on a single state Hash.
    def _gc_transaction(current_vals):
      update = False
      cids_removed.clear()

      for cid, packed_state in YieldByCount(current_vals.iteritems(), 10):
        if packed_state is None:
          continue

        # Deserialize the state pulled from the DB.
        state_struct = UnpackAndUpgrade(packed_state)

        # Retrieve the Handler for the Tab Type.
        handler = tab_registry.GetHandler(state_struct.tab_type)
        if handler is None:
          LOG.error("Unknown Tab Type '%s'" % state_struct.tab_type)
          continue
        handler.server = self

        # Determine whether the State should be pruned or not.
        if state_struct.tab_type != expected_type:
          LOG.error("Type mismatch for N%s 'E: %s, A: %s'" % (
              str(nid), expected_type, state_struct.tab_type))
          update = True
          cids_removed.add(cid)

        if handler.ShouldPrune(state_struct.payload):
          update = True
          cids_removed.add(cid)

      return list(cids_removed) if update else None

    try:
      self.engine.HashCheckAndDelete(
          KEY_PATTERN_STATE % nid,
          _gc_transaction,
          tx_retries=0)

      # Update the CID -> NIDs index for any CIDs removed from this NID.
      if cids_removed:
        LOG.info('GC: removed %d states for %d' % (len(cids_removed), nid))

        for cid in cids_removed:
          self.RemoveNidForCid(cid, nid)

    except redis.WatchError:
      # Ignore watch errors, since a change to a state usually means it isn't
      # collectible.
      pass

    except Exception:
      LOG.error('Caught exception while running GC on %d' % nid)
      LOG.error(traceback.format_exc())

  ###################################################################
  # Maintenance Methods - Upgrade
  ###################################################################

  def UpgradeAll(self):
    """Traverse all states in the database, and attempt to upgrade them to the
    latest version of the handler (where applicable).
    """
    LOG.info('Upgrade: Starting')

    # Callback function to upgrade a {CID: State} Hash for an NID.
    def _upgrade(current_vals):
      updates = {}

      for cid, packed_state in YieldByCount(current_vals.iteritems(), 10):
        if packed_state is None:
          continue

        # Deserialize the state pulled from the DB.
        state_struct = UnpackState(packed_state)

        # Retrieve the Handler for the Tab Type.
        handler = tab_registry.GetHandler(state_struct.tab_type)
        if handler is None:
          LOG.error("Unknown Tab Type '%s'" % state_struct.tab_type)
          continue
        handler.server = self

        # Upgrade the retrieved State if necessary.
        if state_struct.version != handler.CURRENT_VERSION:
          state_struct.payload = handler.Upgrade(
              state_struct.payload,
              state_struct.version)
          state_struct.version = handler.CURRENT_VERSION
          updates[cid] = PackState(state_struct)

      return updates if updates else None

    # Retrieve the list of all NIDs. We can do this without checking for updates
    # later since any new NIDs will be created with the latest Handler versions.
    nids = model_provider.GetNames().GetAllNids()
    for i, nid in enumerate(nids):
      if i % 1000 == 0:
        LOG.info('Upgrade: processing %d (%d of %d)' % (nid, i, len(nids)))

      self.engine.HashCheckAndMultiSet(KEY_PATTERN_STATE % nid, None, _upgrade)

    LOG.info('Upgrade: Finished')

  ###################################################################
  # Maintenance Methods - Self-check
  ###################################################################

  def SelfCheck(self, dry=True):
    """Check the internal consistency of the State objects.

    Args:
      dry - Whether this is a dry run. IF False, inconsistencies will be
          deleted.
    """
    nid_to_type = model_provider.GetNames().GetAllTypes()
    nids = sorted(model_provider.GetNames().GetAllNids(), reverse=True)

    total = [0]
    def _process_nid(nid):
      total[0] += 1
      LOG.info('Processing %d (%d of %d)' % (nid, total[0], len(nids)))
      self._SelfCheckNid(nid, nid_to_type.get(nid, None), dry)

    processor = thread_util.AsyncProcessor(
        tasks=nids,
        process_fn=_process_nid,
        result_fn=lambda x: x,
        workers=10)

    processor.Process()

  def _SelfCheckNid(self, nid, expected_type, dry=True):
    """Check the consistency of the Tab Types for all CIDs of a single NID.

    Args:
      nid - NID of the State.
      expected_type - Expected Tab Type.
      dry - Whether this is a dry run. If False, inconsistencies will be
          deleted.
    """
    deletions = set()

    def _check_nid(current_vals):
      deletions.clear()

      for cid, packed_state in YieldByCount(current_vals.iteritems(), 10):
        if packed_state is None:
          continue

        # Deserialize the state pulled from the DB.
        state_struct = UnpackAndUpgrade(packed_state)

        # Retrieve the Handler for the Tab Type.
        if state_struct.tab_type != expected_type:
          LOG.info('Self-check: type mismatch N%d C%d (Exp.: %s, Got: %s)' %
              (nid, cid, expected_type, state_struct.tab_type))
          if not dry:
            deletions.add(cid)

      return list(deletions) if len(deletions) > 0 else None

    # Check the Hash for fields that need to be deleted.
    self.engine.HashCheckAndDelete(KEY_PATTERN_STATE % nid, _check_nid)

    if deletions:
      LOG.info('Self-check: removed %d for NID %d' % (len(deletions), nid))
      for cid in deletions:
        self.RemoveNidForCid(cid, nid)

  def DeepClean(self, dry=True):
    """Iterate through all the State database keys, delete any that are for
    NIDs that are not in the NID index, and remove and Name <-> NID mappings
    that have no associated States.

    Args:
      dry - Whether this is a dry run. If False, delete orphaned records from
          the database.
    """
    pattern = re.compile('^St([0-9]*)')
    orphaned_nids = set()
    used_nids = set()

    # Get the set of all DB keys matching the State pattern.
    op = util.StrictOp('Getting all State DB keys',
        self.engine.Keys,
        'St*')

    for i, state_key in enumerate(op.response_value):
      if i % 1000 == 0:
        LOG.info('Processing %d of %d' % (i, len(op.response_value)))

      # Extract the NID from the key.
      m = pattern.search(state_key)
      nid = int(m.groups()[0])

      used_nids.add(nid)

      # Check whether the NID exists in the Name storage index.
      name = model_provider.GetNames().GetNames([nid])[nid]
      if name is None:
        LOG.info('Found orphaned State record "%d"' % nid)
        orphaned_nids.add(nid)

    # Delete any orphaned State records.
    orphaned_nids = list(orphaned_nids)
    for i in xrange(0, len(orphaned_nids), 1000):
      LOG.info('Removing orphaned States (%d of %d)' % (i, len(orphaned_nids)))
      if not dry:
        op = util.CheckedOp(
            'Removing orphaned States (%d of %d)' % (i, len(orphaned_nids)),
            self.engine.BatchDelete,
            [KEY_PATTERN_STATE % nid for nid in orphaned_nids[i:i+1000]])

    # Delete any unused NIDs.
    all_nids = set(model_provider.GetNames().GetAllNids())
    stale_nids = all_nids - used_nids

    stale_nid_to_name = model_provider.GetNames().GetNames(list(stale_nids))
    LOG.info('Removing %d stale Names' % len(stale_nids))
    for nid, name in stale_nid_to_name.iteritems():
      LOG.info('Deleting N(%s, %s)' % (nid, name))

      if not dry:
        model_provider.GetNames().RemoveName(name)

  ###################################################################
  # Retrieval Methods
  ###################################################################

  def StatesIterator(self, nids=None, cids=None, render=True, accept=None):
    """Get a _non-aggregated_ State iterator.

    Args:
      nids - List of NIDs to restrict results to. If None, all NIDs will be
          included.
      cids - List of CIDs to restrict results to. If None, all CIDs will be
          included.
      render - If True, the returned 'State String' values will be rendered by
          the Handler for the NID. Otherwise, they will be the raw binary
          State Buffer.
      accept - MIME type to render to (if render is True).

    Returns:
      Stream of ((NID, CID), State String)
    """
    # Get the underlying raw iterator.
    state_iterator = self._IteratorWithCids(nids, cids)

    # Process the raw results according to the arguments.
    for nid, cid_to_sstate in state_iterator:
      for cid, sstate in cid_to_sstate.iteritems():
        if sstate is None:
          continue

        state_struct = UnpackAndUpgrade(sstate)
        state_str = state_struct.payload

        if render:
          handler = tab_registry.GetHandler(state_struct.tab_type)
          if handler is None:
            LOG.error("Unknown Tab Type '%s'" % state_struct.tab_type)
            continue
          handler.server = self

          state_str = handler.Render(state_str, accept)

        yield ((nid, cid), state_str)

  def AggregatesIterator(self, nids=None, cids=None, render=True, accept=None):
    """Get an aggregated State iterator. Results be aggregated over all
    applicable CIDs.

    Args:
      nids - List of NIDs to restrict results to. If None, all NIDs will be
          included.
      cids - List of CIDs to restrict results to. If None, all CIDs will be
          included.
      render - If True, the returned 'State String' values will be rendered by
          the Handler for the NID. Otherwise, they will be the raw binary
          State Buffer.
      accept - MIME type to render to (if render is True).

    Returns:
      Stream of (NID, State String)
    """
    if cids is not None:
      # We're getting Aggregates on a subset of CIDs. Get the applicable
      # underlying iterator, and reduce all the CID parts together.
      state_iterator = self._IteratorWithCids(nids, cids)

      for nid, cid_to_sstate in state_iterator:
        state_structs = [
            UnpackAndUpgrade(sstate)
            for sstate in cid_to_sstate.values()
            if sstate is not None]

        if not state_structs:
          continue

        states = [s.payload for s in state_structs]

        handler = tab_registry.GetHandler(state_structs[0].tab_type)
        if handler is None:
          LOG.error("Unknown Tab Type '%s'" % state_structs[0].tab_type)
          continue
        handler.server = self

        state_str = handler.Reduce(states)
        if render:
          state_str = handler.Render(state_str, accept)

        yield (nid, state_str)

    else:
      # If we're getting Aggregates AND specific CID(s) are not requested, then
      # we can use the cached 'All CID' State, which saves us from having to
      # download each Client's State and reduce them together.
      state_iterator = self._IteratorWithoutCids(nids)

      for nid, sstate in state_iterator:
        state_struct = UnpackAndUpgrade(sstate) if sstate is not None else None
        if not state_struct:
          continue

        state_str = state_struct.payload

        if render:
          handler = tab_registry.GetHandler(state_struct.tab_type)
          if handler is None:
            LOG.error("Unknown Tab Type '%s'" % state_struct.tab_type)
            continue
          handler.server = self

          state_str = handler.Render(state_str, accept)

        yield (nid, state_str)

  def SuperAggregate(self, nids=None, cids=None, render=True, accept=None):
    """Get a "super aggregated" State, which is where multiple different Tabs
    of the same Tab Type are aggregated together.

    Args:
      nids - List of NIDs to restrict results to. If None, all NIDs will be
          included.
      cids - List of CIDs to restrict results to. If None, all CIDs will be
          included.
      render - If True, the returned 'State String' values will be rendered by
          the Handler for the NID. Otherwise, they will be the raw binary
          State Buffer.
      accept - MIME type to render to (if render is True).

    Returns:
      A single State String
    """
    if cids is not None:
      # We're getting Super-Aggregates on a subset of CIDs. Get the applicable
      # underlying iterator, and extract the StateStruct for each NID and CID.
      serialized_state_iterator = self._IteratorWithCids(nids, cids)

      # [SState, ...] => [StateStruct, ...]
      state_struct_iterator = itertools.ifilter(None, itertools.imap(
          UnpackAndUpgrade,

          # [[SState, ...], ...] => [SState, ...]
          itertools.chain.from_iterable(

              # [{CID: SState}, ...] => [[SState, ...], ...]
              itertools.imap(
                  operator.methodcaller('values'),

                      # [(NID, {CID: SState}), ...] => [{CID: SState}, ...]
                      itertools.imap(
                          operator.itemgetter(1),
                          serialized_state_iterator)))))

    else:
      # If we're getting Super-Aggregates AND specific CID(s) are not requested,
      # then we can use the cached 'All CID' State, which saves us from having
      # to download each Client's State and reduce them together.
      serialized_state_iterator = self._IteratorWithoutCids(nids)

      state_struct_iterator = itertools.ifilter(None, itertools.imap(
          UnpackAndUpgrade,
          itertools.imap(
              operator.itemgetter(1),
              serialized_state_iterator)))

    # Pull the first state from the iterator to initialize with.
    try:
      state_struct = state_struct_iterator.next()
    except StopIteration:
      return None

    # Initialize the Tab Type and Handler.
    tab_type = state_struct.tab_type
    handler = tab_registry.GetHandler(tab_type)
    if handler is None:
      LOG.error("Unknown Tab Type '%s'" % tab_type)
      return None
    handler.server = self

    # Initialize the 'reduced' aggregate.
    reduced = state_struct.payload

    # Run through slices of States, reducing them into the common aggregate.
    for state_struct_slice in misc_util.Slicer(state_struct_iterator, 25):
      state_struct_slice = list(state_struct_slice)

      # Verify that all the states are of the same Tab Type.
      for state_struct in state_struct_slice:
        if state_struct.tab_type != tab_type:
          raise ValueError('Cannot super-aggregate heterogeneous types')

      states = [s.payload for s in state_struct_slice]
      states.append(reduced)

      reduced = handler.Reduce(states)

    if render:
      reduced = handler.Render(reduced, accept)

    return reduced

  ###################################################################
  # Internal State Iterators
  ###################################################################

  def _IteratorWithCids(self, nids=None, cids=None):
    """Return an iterator of (NID, {CID: Serialized State}).

    Args:
      nids - List of NIDs to restrict results to. If None, all NIDs will be
          included.
      cids - List of CIDs to restrict results to. If None, all CIDs will be
          included.

    Returns:
      Iterator which generates a stream of (NID, {CID: Serialized State}).
    """
    if nids is not None and cids is not None:
      # Iterate over a requested set of NIDs, restricted to a set of CIDs.
      it = self._IteratorByNidsWithCids(nids, cids)

    elif nids is not None:
      # Iterate over a requested set of NIDs, _not_ restricted to a set of CIDs.
      it = self._IteratorByNidsWithCids(nids)

    elif cids is not None:
      # Iterate over all NIDs for a requested set of CIDs. We have to use the
      # CID -> NIDs index to know what NIDs to pull from.

      all_nids = set()
      def _get_nids_for_cid(cid):
        all_nids.update(self.GetNidsForCid(cid))
      gevent.joinall([gevent.spawn(_get_nids_for_cid, cid) for cid in cids])

      it = self._IteratorByNidsWithCids(all_nids, cids)

    else:
      # Iterate over all NIDs and CIDs.
      nids = model_provider.GetNames().GetAllNids()
      it = self._IteratorByNidsWithCids(nids)

    return it

  def _IteratorWithoutCids(self, nids=None):
    """Return an iterator of (NID, Serialized State). This uses the pre-cached
    'All CID' aggregate state, which is much more efficient to retrieve and
    process for cases where the individual CIDs aren't needed.

    Args:
      nids - List of NIDs to restrict results to. If None, all NIDs will be
        included.

    Returns:
      Iterator which generates a stream of (NID, Serialized State).
    """
    if nids is not None:
      it = self._IteratorByNids(nids)

    else:
      nids = model_provider.GetNames().GetAllNids()
      it = self._IteratorByNids(nids)

    return it

  def _IteratorByNidsWithCids(self, nids, cids=None):
    """For a list of NIDs, generates a sequence of Tuples like:
      (NID, {CID: Serialized State})

    Args:
      nids - List of NIDs to retrieve states for.
      cids - CIDs to pull states for.
    """

    def _get_batch(nid):
      if cids is not None:
        state_op = self.engine.HashMultiGet(KEY_PATTERN_STATE % nid, cids)
      else:
        state_op = self.engine.HashGetAll(KEY_PATTERN_STATE % nid)

      if not state_op.success:
        raise Exception('Error retrieving State batch\n%s' % state_op)

      if state_op.response_value:
        state_op.response_value = dict(
            (int(k), v) for k, v in state_op.response_value.iteritems()
            if k != ALL_CID_FIELD)

      return state_op

    def _format_output(nid, state_op):
      return (nid, state_op.response_value)

    return thread_util.BatchedAsyncIterator(
        nids, _get_batch, _format_output, workers=64, buffer_len=16)

  def _IteratorByNids(self, nids):
    """For a list of NIDs, generates a sequence of Tuples like:
      (NID, Serialized State)

    Args:
      nids - List of NIDs to retrieve states for.
    """

    def _get_batch(nid):
      state_op = self.engine.HashGet(KEY_PATTERN_STATE % nid, ALL_CID_FIELD)
      if not state_op.success:
        raise Exception('Error retrieving State batch\n%s' % state_op)

      return state_op

    def _format_output(nid, state_op):
      return (nid, state_op.response_value)

    return thread_util.BatchedAsyncIterator(
        nids, _get_batch, _format_output, workers=64, buffer_len=16)

#####################################################################
# Serialized State binary conversion methods
#####################################################################

class StateStruct(object):
  """Object for holding a State object and associated meta-data.
  """

  def __init__(self, payload, version, tab_type):
    """
    Args:
      payload - A binary serialized State object.
      version - Handler specific State version number.
      tab_type - Tab Type name string.
    """
    self.payload = payload
    self.version = version
    self.tab_type = tab_type

  def __repr__(self):
    return '%s(t=%s, v=%s, p=%s)' % (
        self.__class__.__name__, self.tab_type, self.version, self.payload)

  def __eq__(self, other):
    return \
        hasattr(other, 'payload') and other.payload == self.payload and \
        hasattr(other, 'version') and other.version == self.version and \
        hasattr(other, 'tab_type') and other.tab_type == self.tab_type

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
  meta-data.

  Args:
    state_struct - A populated StateStruct object.

  Returns:
    Binary string of the packed state ready for persisting in the database.
  """
  state_serialized = pickle.dumps(state_struct, protocol=2)

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

def UnpackAndUpgrade(packed_state, handler=None):
  """Unpack a binary Serialized State (see UnpackState()), and checks whether
  the results needs to be upgraded according to the current version of the
  Handler for its Tab Type.

  Args:
    packed_state - Binary string of the type produces by PackState.
    handler - If specified, use this TabHandler. Otherwise, the correct Handler
        is looked-up based on the Tab Type in the resulting StateStruct.

  Returns:
    Populated StateStruct object.
  """
  if packed_state is None:
    return None

  # Deserialize the state pulled from the DB.
  state_stct = UnpackState(packed_state)

  # Retrieve the Handler for the Tab Type.
  if handler is None:
    handler = tab_registry.GetHandler(state_stct.tab_type)
    if handler is None:
      LOG.error("Unknown Tab Type '%s'" % state_stct.tab_type)
      return state_stct

  # Upgrade the retrieved State if necessary.
  if state_stct.version != handler.CURRENT_VERSION:
    state_stct.payload = handler.Upgrade(state_stct.payload, state_stct.version)
    state_stct.version = handler.CURRENT_VERSION

  return state_stct
