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

Storage Manager for the Taba Server to persist data to a Redis-like engine.
"""

from collections import defaultdict
import itertools
import traceback

import gevent
from gevent.queue import Queue

from tellapart.storage.operation import CompoundOperation
from tellapart.storage.operation import Operation
from tellapart.taba.server import taba_state
from tellapart.taba.util import misc_util

KEY_STATE = 'state'
KEY_CLIENTS = 'clients'
KEY_NAMES = 'names'
KEY_TYPE = 'type'

ALL_CLIENTS = 'all'
NUM_NAME_BLOCKS = 128

BATCH_LOOKUP_SIZE = 20

LOG = misc_util.MakeStreamLogger(__name__)

class TabaServerStorageManager(object):
  """Storage Manager for the Taba Server that persists data to a Redis-like
  engine.
  """

  def __init__(self, engine):
    """
    Args:
      engine - Storage engine compatible with RedisEngine.
    """
    self.engine = engine
    self.num_name_blocks = NUM_NAME_BLOCKS

  #----------------------------------------------------------------------------
  #  STATE
  #----------------------------------------------------------------------------

  def StateGetBatch(self, id_tuples):
    """Retrieve a batch of State objects.

    Args:
      id_tuples - List of tuples of the form (client_id, name) of the state
          objects to retrieve.

    Returns:
      Operation object with the query results. If successful, the response_value
      field contains a List of tuples of the form ((client_id, name), state)
    """
    # Convert the ID tuples to keys and retrieve from the DB.
    keys = [
        self._MakeKey(KEY_STATE, client, name)
        for client, name in id_tuples]

    op = self._GetBatch(keys)

    # Unpack the StateStruct objects and join the retrieved values to their
    # original ID tuples.
    if op.response_value:
      try:
        states_unpacked = map(taba_state.UnpackState, op.response_value)
        op.response_value = zip(id_tuples, states_unpacked)

      except Exception:
        LOG.error("Error unpacking State objects")
        LOG.error(traceback.format_exc())
        op = Operation(success=False, traceback=traceback.format_exc())

    return op

  def StateGetBatchGenerator(self, id_tuples):
    """Retrieve a batch of State objects.

    Args:
      id_tuples - List of tuples of the form (client_id, name) of the State
          objects to retrieve.

    Returns:
      Operation object with the query results. If successful, the response_value
      field contains a List of tuples of the form ((client_id, name), state)
    """
    # Convert the ID tuples to keys and retrieve from the DB.
    keys = [
        self._MakeKey(KEY_STATE, client, name)
        for client, name in id_tuples]

    op = self._GetBatch(keys)

    # Unpack the StateStruct objects and join the retrieved values to their
    # original ID tuples.
    if op.response_value:
      try:
        states_unpacked = itertools.imap(taba_state.UnpackState, op.response_value)
        op.response_value = itertools.izip(id_tuples, states_unpacked)

      except Exception:
        LOG.error("Error unpacking State objects")
        LOG.error(traceback.format_exc())
        op = Operation(success=False, traceback=traceback.format_exc())

    return op

  def StateGet(self, client_id, name):
    """Retrieve the State object for a specific Client ID and Name.

    Args:
      client_id - Client ID string to lookup the State object for.
      name - Taba Name string to lookup the State object for.

    Returns:
      Operation object with the query results. If successful, the response_value
      field contains a tuple of the form ((client_id, name), state)
    """
    op = self.StateGetBatch([(client_id, name)])

    if op.response_value:
      op.response_value = op.response_value[0]

    return op

  def StatePutBatch(self, id_value_tuples):
    """Store a batch of State objects, and update the corresponding Client IDs
    and Taba Names Sets.

    Args:
      id_value_tuples - List of tuples of the form ((client_id, name), state) to
          be stored. The state objects must be JSON serializable.

    Returns:
      Operation object with the query results.
    """
    # Make the States keys and put the values in the DB.
    key_value_tuples = [
        (self._MakeKey(KEY_STATE, client_id, name), taba_state.PackState(state))
        for (client_id, name), state in id_value_tuples]

    op = self._PutBatch(key_value_tuples)

    # Update the sets of all Client IDs and Taba Names.
    if op.success:
      self._UpdateIdSets([id for id, _ in id_value_tuples])

    return op

  def StatePut(self, client_id, name, state):
    """Store a State object for a specific Client ID and Taba Name, and update
    the corresponding Client ID and Taba Name Sets.

    Args:
      client_id - Client ID string to store the State object for.
      name - Taba Name string to store the State object for.
      state - The state object to store. The object must be JSON serializable.

    Returns:
      Operation object with the query results.
    """
    op = self.StatePutBatch([((client_id, name), state)])

    if op.response_value:
      op.response_value = op.response_value[0]

    return op

  def StateUpdateBatchTx(self, id_tuples, update_fn):
    """For a given set of (client_id, name) ID tuples, retrieve the State object
    and pass to the update_fn callback. The updated State object returned from
    the callback will then be stored. Finally, the Client ID and Taba Names Sets
    will be updated.

    Args:
      id_tuples - List of tuples of the form (client_id, name) to update.
      update_fn - Callable whose signature is f(client_id, name, state), and
          returns an updated state. This function will be called for each State
          object to be updated. The State object can be None, if it doesn't
          exist in the DB. The function may be called multiple times for the
          same key in the event of a lock error.

    Returns:
      CompoundOperation object with the query results.
    """
    # Generate a map from DB keys to (client_id, name) tuples.
    state_keys_map = {}
    for c, n in id_tuples:
      state_keys_map[self._MakeKey(KEY_STATE, c, n)] = (c, n)

    _Callback = _StateUpdateBatchTxCallable(state_keys_map, update_fn)
    op = self.engine.BatchCheckAndMultiSet(state_keys_map.keys(), _Callback)

    if op.retries > 0:
      LOG.warning("Transactional retries: %d" % op.retries)

    # Update the Client and Names Sets.
    self._UpdateIdSets(id_tuples)

    return op

  def StateDelete(self, client_id, name):
    """Delete a State object for a specific Client ID and Taba Name.

    Args:
      client_id - Client ID string to store the State object for.
      name - Taba Name string to store the State object for.

    Returns:
      Operation object with the query results.
    """
    return self.StateDeleteBatch([(client_id, name)])

  def StateDeleteBatch(self, id_tuples):
    """Delete a batch of State objects.

    Args:
      id_tuples - List of tuples of the form (client_id, name) of the State
          objects to delete.

    Returns:
      Operation object with the query results.
    """
    keys = [
        self._MakeKey(KEY_STATE, client, name)
        for client, name in id_tuples]

    try:
      op = self.engine.BatchDelete(keys)
      if not op.success:
        LOG.error("Batch Delete operation failed")
        LOG.error(op)

    except Exception:
      LOG.error("Exception in Batch Delete operation")
      LOG.error(traceback.format_exc())
      op = Operation(success=False, traceback=traceback.format_exc())

    return op

  def StateIteratorForClient(self, client_id):
    """Generator which iterates over all State objects for a Client ID.

    Args:
      client_id - Client ID string to retrieve State objects for.

    Returns:
      Generator that yields values of the form ((client_id, name), state)
    """
    op = self.TabaNamesForClientGet(client_id)
    if not op.success:
      raise Exception('Error retrieving list of Taba Names\n%s' % op)
    names = op.response_value

    state_ids = self._IdsIterator([client_id], names)

    return self._StateIteratorForIdTuples(state_ids)

  def StateIteratorForName(self, name):
    """Generator which iterates over all state objects for a Taba Name.

    Args:
      name - Taba Name string to retrieve State object for.

    Returns:
      Generator that yields values of the form ((client_id, name), state)
    """
    # Get the set of all clients.
    op = self.ClientIdsGet()
    if not op.success:
      raise Exception('Error retrieving list of Client IDs\n%s' % op)
    clients = op.response_value

    state_ids = self._IdsIterator(clients, [name])

    return self._StateIteratorForIdTuples(state_ids)

  def StateIteratorForNameList(self, names, client_id=None, by_name=False):
    """Generator which iterates over all state objects for a list of Taba Name.

    Args:
      names - List of Taba Name string to retrieve State objects for.
      client_id - Client ID string to retrieve State objects for.
      by_name - Group generated States by Taba Name (instead of Client ID, which
          id the default)

    Returns:
      Generator that yields values of the form ((client_id, name), state)
    """
    # Get the set of clients to retrieve.
    if client_id:
      clients = [client_id]

    else:
      op = self.ClientIdsGet()
      if not op.success:
        raise Exception('Error retrieving list of Client IDs\n%s' % op)
      clients = op.response_value

    # Make the list of IDs, grouped by name.
    state_ids = self._IdsIterator(clients, names, by_name=by_name)

    return self._StateIteratorForIdTuples(state_ids)

  def StateIteratorForNameBlocks(self, name_blocks, by_name=False):
    """Generator which iterates over all state objects for a Taba Name Block.

    Args:
      name_block - List of Taba Name Block strings to retrieve State objects.
      by_name - Group generated States by Taba Name (instead of Client ID, which
          id the default)

    Returns:
      Generator that yields values of the form ((client_id, name), state)
    """
    # Get the set of all clients.
    cli_op = self.ClientIdsGet()
    if not cli_op.success:
      raise Exception('Error retrieving list of Client IDs\n%s' % cli_op)

    clients = set(cli_op.response_value)

    # Get the set of names to process.
    names_op = self.TabaNamesForAllGet()
    if not names_op.success:
      raise Exception('Error retrieving list of Taba Names\n%s' % names_op)

    names = [
        name for name in names_op.response_value
        if str(hash(name) % self.num_name_blocks) in name_blocks]

    # Build the list of State IDs to retrieve.
    state_ids = self._IdsIterator(clients, names, by_name=by_name)

    return self._StateIteratorForIdTuples(state_ids)

  def StateIterator(self, by_name=False):
    """Generator which iterates over all State objects.

    Returns:
      Generator that yields values of the form ((client_id, name), state)
    """
    clients_op = self.ClientIdsGet()
    if not clients_op.success:
      raise Exception('Error retrieving list of Client IDs\n%s' % clients_op)
    clients = clients_op.response_value

    names_op = self.TabaNamesForAllGet()
    if not names_op.success:
      raise Exception('Error retrieving list of Taba Names\n%s' % names_op)
    names = names_op.response_value

    # Generate the list of IDs to lookup
    state_ids = self._IdsIterator(clients, names, by_name=by_name)

    return self._StateIteratorForIdTuples(state_ids)

  def _IdsIterator(self, clients, names, by_name=False,
                   batch_size=BATCH_LOOKUP_SIZE):
    """Returns an iterator which will generate (Client ID, Taba Name) tuples for
    all combinations of Client ID and Taba Name from the lists given. If by_name
    is True, the results will be grouped by Taba Name; otherwise they will be
    grouped by Client ID (default). Client IDs and Taba Names will be returned
    in lexographically sorted order.

    Args:
      clients - List of Client IDs.
      names - List of Taba Names.
      by_name - If True, group the produced ID tuples by Taba Name, instead of
          by Client ID.
      batch_size - Maximum number of ID tuples to return per slice (if not using
          by-name mode)

    Returns:
      A generator which will generate (client, name) tuples for all combinations
      of Client ID and Taba Name from the lists given.
    """
    clients = sorted(clients)
    names = sorted(names)

    if by_name:
      def next_slice_by_name():
        for name in names:
          yield [(client, name) for client in clients]

        raise StopIteration

      return next_slice_by_name()

    else:
      iter = itertools.product(clients, names)
      def next_slice_by_client():
        while True:
          slice = [e for e in itertools.islice(iter, batch_size)]
          if not slice:
            raise StopIteration
          yield slice

      return next_slice_by_client()

  def _StateIteratorForIdTuples(self, slice_gen):
    """Generator which iterates over State buffers for a given list of ID
    tuples. Separated the list of tuples into batches of a maximum size.

    Args:
      slice_gen - Generator which returns the next list of ID tuples of the
          form (Client ID, Taba Name) to lookup.
    """
    # Split the lookups into batches, and start a background greenlet to
    # retrieve the batches. Use a queue to retrieve results so that they can be
    # processes as soon as they are available, and limit the size of the queue
    # to control memory usage.
    result_queue = Queue(8)

    def _GetBatchWorker():
      while True:

        try:
          id_slice = slice_gen.next()

        except StopIteration:
          result_queue.put(None)
          return

        state_op = self.StateGetBatchGenerator(id_slice)
        if not state_op.success:
          LOG.error('Error retrieving State batch\n%s' % state_op)
          result_queue.put(Exception)
          return

        else:
          result_queue.put(state_op)

    workers = [gevent.spawn(_GetBatchWorker) for _ in xrange(8)]

    # Extract the results as long as there are unprocessed slices or there are
    # results available.
    while not all([w.ready() for w in workers]) or not result_queue.empty():
      state_op = result_queue.get()

      # Yield the results from this batch.
      if state_op:
        for i, ((client_id, name), state) in enumerate(state_op.response_value):
          if state is not None:
            yield ((client_id, name), state)

          # Yield to other greenlets periodically.
          if i % 5000 == 0:
            gevent.sleep(0)

  #----------------------------------------------------------------------------
  #  CLIENT IDS
  #----------------------------------------------------------------------------

  def ClientIdsGet(self):
    """Return the Set of all Client IDs.

    Returns:
      Operation object with the query results. If successful, the response_value
      field contains a set of all Client IDs.
    """
    key = KEY_CLIENTS
    op = self._CheckedOp("retrieving Client IDs Set",
        self.engine.SetMembers, key)

    if op.response_value:
      op.response_value = set(op.response_value)

    return op

  def ClientIdsAdd(self, client_ids):
    """Add Client IDs to the Set of all Client IDs.

    Args:
      client_ids - List of Client ID strings to add.

    Returns:
      Operation object with the query results.
    """
    key = KEY_CLIENTS
    op = self._CheckedOp("adding Client IDs to Set",
        self.engine.SetAdd, key, *client_ids)

    return op

  def ClientIdsRemove(self, client_ids):
    """Remove Client IDs from the Set of all Client IDs.

    Args:
      client_ids - List of Client ID strings to remove.

    Returns:
      Operation object with the query results.
    """
    key = KEY_CLIENTS
    op = self._CheckedOp("removing Client IDs from Set",
        self.engine.SetRemove, key, *client_ids)

    return op

  #----------------------------------------------------------------------------
  #  TABA NAMES
  #----------------------------------------------------------------------------

  def TabaNamesForClientGet(self, client_id):
    """Retrieve the set of all Taba Names for a Client ID.

    Args:
      client_id - The Client ID string to retrieve Taba Names for.

    Returns:
      Operation object with the query results. If successful, the response_value
      field contains the set of Taba Names for the Client ID.
    """
    key = self._MakeKey(KEY_NAMES, client_id)
    op = self._CheckedOp("retrieving Taba Names for Client %s" % client_id,
        self.engine.SetMembers, key)

    if op.response_value:
      op.response_value = set(op.response_value)

    return op

  def TabaNamesForClientAdd(self, client_id, names):
    """Add Taba Names to the Set of Taba Names for a Client ID.

    Args:
      client_id - Client ID string to add Taba Names for.
      names - List of Taba Name strings to add.

    Returns:
      Operation object with the query results.
    """
    key = self._MakeKey(KEY_NAMES, client_id)
    op = self._CheckedOp("adding Taba Names to Set for Client %s" % client_id,
        self.engine.SetAdd, key, *names)

    return op

  def TabaNamesForClientRemove(self, client_id, names):
    """Remove Taba Names from the Set of Taba Names for a Client ID.

    Args:
      client_id - Client ID string to remove Taba Names from.
      names - List of Taba Name strings to remove.

    Returns:
      Operation object with the query results.
    """
    key = self._MakeKey(KEY_NAMES, client_id)
    op = self._CheckedOp("removing Taba Names to Set for Client %s" % client_id,
        self.engine.SetRemove, key, *names)

    return op

  def TabaNamesForClientDelete(self, client_id):
    """Remove Taba Names Set for a Client ID.

    Args:
      client_id - Client ID string to remove Taba Names Set.

    Returns:
      Operation object with the query results.
    """
    key = self._MakeKey(KEY_NAMES, client_id)
    op = self._CheckedOp("removing Taba Names to Set for Client %s" % client_id,
        self.engine.Delete, key)

    return op

  def TabaNamesForAllGet(self):
    """Retrieve the set of all Taba Names across all Client IDs.

    Returns:
      Operation object with the query results. If successful, the response_value
      field contains the set of Taba Names across all Client ID.
    """
    return self.TabaNamesForClientGet(ALL_CLIENTS)

  def TabaNamesForAllAdd(self, names):
    """Add Taba Names to the Set of Taba Names across all Client IDs.

    Args:
      names - List of Taba Name strings to add.

    Returns:
      Operation object with the query results.
    """
    return self.TabaNamesForClientAdd(ALL_CLIENTS, names)

  def TabaNamesForAllRemove(self, names):
    """Remove Taba Names from the Set of Taba Names across all Client IDs.

    Args:
      names - List of Taba Name strings to remove.

    Returns:
      Operation object with the query results.
    """
    return self.TabaNamesForClientRemove(ALL_CLIENTS, names)

  #----------------------------------------------------------------------------
  #  TABA TYPES
  #----------------------------------------------------------------------------

  def TabaTypeGetBatch(self, names):
    """Retrieve a batch of Taba Type strings.

    Args:
      names - List of Taba Names to lookup Taba Types for.

    Returns:
      Operation object with the query results. If successful, the response_value
      field is a List of tuples of the form (name, type).
    """
    keys = [self._MakeKey(KEY_TYPE, name) for name in names]

    op = self._GetBatch(keys)

    # Decode the compressed values
    if op.response_value:
      vals = [val or None for val in op.response_value]
      op.response_value = zip(names, vals)

    return op

  def TabaTypeGet(self, name):
    """Retrieve the Taba Type string for a specific Taba Name.

    Args:
      name - The Taba Name to lookup the Taba Type for.

    Returns:
      Operation object with the query results. If successful, the response_value
      field is a tuple of the form (name, type)
    """
    op = self.TabaTypeGetBatch([name])

    if op.response_value:
      op.response_value = op.response_value[0]

    return op

  def TabaTypePutBatch(self, update_tuples):
    """Store a batch of Taba Type strings.

    Args:
      update_tuples - List of tuples of the form (name, type) to update.

    Returns:
      Operation object with the query results.
    """
    key_value_tuples = [
        (self._MakeKey(KEY_TYPE, name), value)
        for name, value in update_tuples]

    op = self._PutBatch(key_value_tuples)

    return op

  def TabaTypePut(self, name, type):
    """Store the Taba Type for a specific Taba Name.

    Args:
      name - The Taba Name to put the Taba Type for.
      type - The Taba Type string.

    Returns:
      Operation object with the query results.
    """
    op = self.TabaTypePutBatch([(name, type)])

    if op.response_value:
      op.response_value = op.response_value[0]

    return op

  def TabaTypeDelete(self, name):
    """Delete the Type entry for a Name

    Args:
      name - The Taba Name to delete the type record for.

    Returns:
      Operation object with the query results.
    """
    key = self._MakeKey(KEY_TYPE, name)

    op = self.engine.Delete(key)
    if not op.success:
      LOG.error('Error deleting type for %s' % name)
      LOG.error(op)

    return op

  def AllKeys(self, pattern):
    """Retrieve the set of keys in the database matching a glob-style pattern.

    Note: This command runs in O(n), where n is the number of keys in the
    database. It should only be used for maintenance or debugging purposes.

    Args:
      pattern - Glob-style pattern to retrieve keys for.

    Returns:
      An Operation object with the result of the query. The response_value field
      contains a list of keys matching the pattern.
    """
    return self.engine.Keys(pattern)

  #----------------------------------------------------------------------------
  #  GENERIC HELPERS
  #----------------------------------------------------------------------------

  def _GetBatch(self, keys):
    """Retrieve several keys in a single operation. Values will be decoded
    before returning.

    Args:
      keys - List of keys to retrieve.

    Returns:
      Operation object with the query results.
    """
    try:
      # Retrieve the encoded valued from the storage engine.
      op = self.engine.BatchGet(keys)
      if not op.success:
        LOG.error("Batch Get operation failed")
        LOG.error(op)
        return op

      op.response_value = [v if v else None for v in op.response_value]

    except Exception:
      LOG.error("Exception in Batch Get operation")
      LOG.error(traceback.format_exc())
      op = Operation(success=False, traceback=traceback.format_exc())

    return op

  def _PutBatch(self, key_value_tuples):
    """Store several keys in a single operation. Values will be encoded before
    storing.

    Args:
      key_value_tuples - List of tuples of (key, value) to store.

    Returns:
      Operation object with the result of the query.
    """
    try:

      # Put the values to the storege engine.
      op = self.engine.BatchPut(key_value_tuples)
      if not op.success:
        LOG.error("Batch Put operation failed")
        LOG.error(op)
        return op

    except Exception:
      LOG.error("Exception in Batch Get operation")
      LOG.error(traceback.format_exc())
      op = Operation(success=False, traceback=traceback.format_exc())

    return op

  def _UpdateIdSets(self, id_tuples):
    """Update the sets of Client IDs and Taba Names.

    Args:
      id_tuples - List of tuples of the form (client_id, name).

    Returns:
      Operation object with the query results.
    """
    op = CompoundOperation()

    # Build a map of Client ID to Taba Names
    client_id_names_map = defaultdict(list)
    for client_id, name in id_tuples:
      client_id_names_map[client_id].append(name)

    try:
      # Add the Client IDs to the Set of all Client IDs.
      op_clients = self.ClientIdsAdd(client_id_names_map.keys())
      op.AddOp(op_clients)

      # Add all the Taba Names to the Set of all names across all Client IDs.
      all_names = set()
      all_names.update(*client_id_names_map.values())
      op_names_all = self.TabaNamesForAllAdd(all_names)
      op.AddOp(op_names_all)

      # Add the Taba Names for each Client ID.
      for client_id, names in client_id_names_map.iteritems():
        op_names = self.TabaNamesForClientAdd(client_id, names)
        op.AddOp(op_names)

    except Exception:
      LOG.error("Error updating Client/Name sets")
      LOG.error(traceback.format_exc())
      op.AddOp(Operation(success=False, traceback=traceback.format_exc()))

    return op

  def _CheckedOp(self, description, engine_fn, *engine_fn_args):
    """Execute an operation which returns an Operation object, and check it for
    success or exceptions, with logging in the case of an error.

    Args:
      description - String describing the operation which will be appended to
          error messages.
      engine_fn - Callback to execute, which return an Operation object.
      enging_fn_args - List of arguments to bass to engine_fn

    Returns:
      Operation object with the query results.
    """
    try:
      op = engine_fn(*engine_fn_args)

      if not op.success:
        LOG.error("Error %s" % description)
        LOG.error(op)
        return op

    except Exception:
      LOG.error("Exception %s" % description)
      LOG.error(traceback.format_exc())
      op = Operation(success=False, traceback=traceback.format_exc())

    return op

  def _MakeKey(self, *args):
    """Convert a list of key parts into a single key.

    Args:
      *args - A list of strings.

    Returns:
      A single string composed of the arguments.
    """
    return ':'.join(args)

class _StateUpdateBatchTxCallable(object):
  """Callable class for implementing the callback logic in StateUpdateBatchTx.
  This is implenented as a callable class instead of a closure to improve
  memory performance and to avoid leaks.
  """
  def __init__(self, state_keys_map, update_fn):
    """
    Args:
      state_keys_map - Map of {Key: (Client ID, Taba Name)}
      update_fn - See TabaServerStorageManager.StateUpdateBatchTx
    """
    self.state_keys_map = state_keys_map
    self.update_fn = update_fn

  def __call__(self, *args, **kwargs):
    return self._Callback(*args, **kwargs)

  def _Callback(self, state_key, packed_state):
    """Callback function that takes a key and value retrieved from the DB,
    and returns a list of (key, value) tuples to put into the DB.
    """
    client_id, name = self.state_keys_map[state_key]

    # Unpack, get the update, and Pack the StateStruct object.
    state = taba_state.UnpackState(packed_state)

    new_state = self.update_fn(client_id, name, state)

    new_packed_state = taba_state.PackState(new_state)

    # Return the (key, value) tuples to put in the DB.
    return[(state_key, new_packed_state)]
