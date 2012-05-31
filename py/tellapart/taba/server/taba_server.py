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

Core Taba Server logic.
"""

from collections import defaultdict
import itertools
import math
import random
import time
import traceback

import cjson
import gevent
from gevent.event import Event
from gevent.pool import Pool
from gevent.queue import Queue

from tellapart.taba import taba_client
from tellapart.taba.handlers import taba_registry
from tellapart.taba.server.taba_state import StateStruct
from tellapart.taba.util import misc_util
from tellapart.taba.util import thread_util
from tellapart.taba.taba_event import TABA_EVENT_IDX_NAME
from tellapart.taba.taba_event import TABA_EVENT_IDX_TIME
from tellapart.taba.taba_event import TABA_EVENT_IDX_TYPE

LOG = misc_util.MakeStreamLogger(__name__)

# Number of 1-minute wide buckets to track applied latency. Only the latest
# bucket gets updated; the rest are used to maintain historical state in a way
# that allows expiring old data. The buckets will be rotated every minute.
_APPLIED_LATENCY_BUCKETS = 60

class TabaServer(object):
  def __init__(self, taba_server_storage_manager, endpoints, prune=False):
    """
    Args:
      taba_server_storage_manager - Initialized TabaServerStorageManager object.
      endpoints - List of Taba Server end-points used for splitting requests
          into parts. If not specified, request splitting is disabled.
      prune - If True, start a periodic pruning service.
    """
    self.dao = taba_server_storage_manager

    # Generate all the Taba Server end-point URLs.
    if endpoints:
      self.redistribute = True
      self.server_endpoints = [
          '%s:%d' % (e['host'], e['port'])
          for e in endpoints]
    else:
      self.redistribute = False

    # Initialize the applied latency tracker.
    self.latency_count_buckets = [0] * _APPLIED_LATENCY_BUCKETS
    self.latency_total_buckets = [0.0] * _APPLIED_LATENCY_BUCKETS
    thread_util.ScheduleOperationWithPeriod(60, self._RotateLatencyBuckets)

    # Initialize the Pruning service to run every 4 hours.
    if prune:
      thread_util.ScheduleOperationWithPeriod(3600, self.PruneAll)

  def GetAverageAppliedLatency(self):
    """Return the average observed difference between the time-stamps on Events
    and the actual time they were received ("applied latency"), over the last
    hour.

    Returns:
      Average applied latency in seconds (float).
    """
    count = sum(self.latency_count_buckets)
    if count == 0:
      return 0.0

    return sum(self.latency_total_buckets) / count

  def _UpdateAppliedLatency(self, client_id, events):
    """Update the average observed difference between the time-stamps on Events
    and the current time ("applied latency") for a list of events.

    Args:
      client_id - The client the events are being processed for.
      events - List of Taba Event Tuples being processed.
    """
    now = time.time()

    self.latency_count_buckets[0] += len(events)
    self.latency_total_buckets[0] += \
        sum([now - e[TABA_EVENT_IDX_TIME] for e in events])

    # Update the Tabas for this request. Have to exclude Tabas from the Taba
    # Server itself, otherwise a continuous loop of events gets established.
    if client_id != 'taba_server':
      taba_client.RecordValue('taba_events_processed', (len(events),))

  def _RotateLatencyBuckets(self):
    """Discard the oldest applied latency bucket, and start a new one.
    """
    taba_client.RecordValue(
        'taba_average_applied_latency',
        (self.GetAverageAppliedLatency(),))

    self.latency_count_buckets = [0] + self.latency_count_buckets[:-1]
    self.latency_total_buckets = [0.0] + self.latency_total_buckets[:-1]

  def ReceiveEvents(self, client_id, events):
    """Receive a list of Taba Events for a Client ID, folding them into the
    current State objects.

    Args:
      client_id - Client ID string to fold events into.
      events - List of Taba Event tuples.
    """
    # Regroup the events by Taba Name
    taba_map = defaultdict(list)
    names_map = {}
    for event in events:
      taba_map[(client_id, event[TABA_EVENT_IDX_NAME])].append(event)
      names_map[event[TABA_EVENT_IDX_NAME]] = event[TABA_EVENT_IDX_TYPE]

    # Retrieve the Handlers.
    handlers = self._GetOrUpdateHandlers(names_map)

    # Execute the transaction state update.
    _UpdateState = _UpdateStateCallable(handlers, taba_map)
    op = self.dao.StateUpdateBatchTx(
        taba_map.keys(),
        _UpdateState)

    if not op.success:
      LOG.error('Error updating events.')
      raise Exception(op)

    self._UpdateAppliedLatency(client_id, events)

  def GetStates(self, client_id=None, names=None, name_blocks=None):
    """Retrieve the raw State object(s). If the client_id or name arguments are
    None or empty then the State(s) are retrieved for all clients or names
    respectively.

    Args:
      client_id - Client ID string to retrieve state objects for. If None or
          empty, then all clients are retrieved.
      names - List of Taba Names to retrieve state objects for. If None or
          empty, then all Taba Names will be retrieved.
      name_block - Taba Name Block to retrieve state objects for. If None or
          empty, then all blocks will be retrieved.

    Returns:
      A list of ((client_id, name), state) tuples.
    """
    # Specific State object - retrieve directly.
    if names and len(names) == 1 and client_id:
      op = self.dao.StateGet(client_id, names[0])
      if op.success and op.response_value[1] is not None:
        states = [op.response_value]
      else:
        states = []

    # Specific Taba Names and Client ID - don't bother distributing.
    elif names and client_id:
      states = self.dao.StateIteratorForNameList(names, client_id=client_id)

    # Specific Taba Names - don't bother distributing.
    elif names:
      states = self.dao.StateIteratorForNameList(names)

    # Specific Client ID - don't bother distributing.
    elif client_id:
      states = self.dao.StateIteratorForClient(client_id)

    # Specific Taba Name Blocks - retrieve all States in the Block.
    elif name_blocks is not None:
      states = self.dao.StateIteratorForNameBlocks(name_blocks)

    # Non-specific request.
    else:
      states = self.dao.StateIterator()

    return states

  def GetProjections(self, client_id=None, names=None, name_blocks=None,
                     handlers=None):
    """Retrieve the raw Projection object(s). If the client_id or name arguments
    are None or empty then the Projection(s) are retrieved for all clients or
    names respectively.

    Args:
      client_id - Client ID string to retrieve projection objects for. If None
          or empty, then all clients are retrieved.
      names - List of Taba Name to retrieve projection objects for. if None or
          empty, then all Taba Names will be retrieved.
      name_block - Taba Name Block to retrieve state objects for. If None or
          empty, then all blocks will be retrieved.
      handlers - Map of {Taba Name: Handler} of pre-fetched handlers. If set,
          this set of handlers will be used instead of looking them up
          explicitly.

    Returns:
      A list of ((client_id, name), projection) tuples.
    """
    if not (names or client_id) and name_blocks is None and self.redistribute:
      # Non-specific request - redistribute the request.
      projections = self._DistributeRequestByTabaNameBlock('projection')
      for projection in projections.iteritems():
        yield projection

    else:
      states = self.GetStates(client_id, names, name_blocks)

      # Get the required Handlers.
      if not handlers:
        all_names = self.GetNames()
        handlers = self._GetHandlers(all_names)

      for (client_id, name), state in states:
        if name in handlers:
          try:
            handler = handlers[name]

            if state.version != handler.CURRENT_VERSION:
              state.payload = handler.Upgrade(state.payload, state.version)

            projection = handler.ProjectState(state.payload)

          except Exception as e:
            LOG.error("Error projecting %s (%s)" % (name, e))
            LOG.error(traceback.format_exc())

          else:
            yield ((client_id, name), projection)

  def GetAggregates(self, names=None, blocks=None, handlers=None):
    """Retrieve Aggregate Projections. If name is None or blank, retrieve for
    all Taba Names.

    Args:
      names - List of Taba Name to retrieve Aggregate Projections for. If None
          or blank, retrieve for all Taba Names.
      blocks - Taba Name Block to retrieve state objects for. If None or
          empty, then all blocks will be retrieved.
      handlers - Map of {Taba Name: Handler} of pre-fetched handlers. If set,
          this set of handlers will be used instead of looking them up
          explicitly.

    Returns:
      List of (name, Aggregate Projection) tuples.
    """
    if not names and blocks is None and self.redistribute:
      # Non-specific request - redistribute the request.
      aggregates = self._DistributeRequestByTabaNameBlock('aggregate')
      for aggregate in aggregates.iteritems():
        yield aggregate

    else:
      # Get the State iterator based on the request parameters.
      #
      # Note: iterators are requested with Name priority, so that all States for
      # the same Taba Name are grouped together.
      if names:
        state_iter = self.dao.StateIteratorForNameList(names, by_name=True)

      elif blocks:
        state_iter = self.dao.StateIteratorForNameBlocks(blocks, by_name=True)

      else:
        state_iter = self.dao.StateIterator(by_name=True)

      # Get the required Handlers.
      if not handlers:
        all_names = self.GetNames()
        handlers = self._GetHandlers(all_names)

      # Iterate through the states, and aggregate all the states for each name.
      last_name = None
      aggregate = None

      for (_, name), state in state_iter:
        # Check if we finished a group of Taba Names.
        if name != last_name:
          if aggregate:
            yield (last_name, aggregate)

          last_name = name
          aggregate = None

        # Fold the current State into the Aggregate.
        try:
          handler = handlers[name]

          if state.version != handler.CURRENT_VERSION:
            state.payload = handler.Upgrade(state.payload, state.version)

          projection = handler.ProjectState(state.payload)

          if aggregate:
            input = [aggregate, projection]
          else:
            input = [projection]

          aggregate = handler.Aggregate(input)

        except Exception as e:
          LOG.error("Error projecting or aggregating %s (%s)" % (last_name, e))
          LOG.error(traceback.format_exc())

      # Finish emitting the final group.
      if aggregate:
        yield (last_name, aggregate)

  def GetRendered(self, client_id=None, names=None, name_blocks=None):
    """Retrieve Rendered Tabas. If client_id is specified, render Projections,
    otherwise, render Aggregates.

    Args:
      client_id - Client ID string to product Rendered Tabas for. If specified
          Projections will be rendered, otherwise Aggregates across all Clients
          will be rendered.
      names - List of Taba Name strings to render. If not specified, then all
          Taba Names will be rendered.
      name_block - Taba Name Block to render. If None or empty, then all blocks
          will be rendered.

    Returns:
      List of strings of Rendered Tabas.
    """
    # Retrieve the handlers.
    all_names = self.GetNames()
    handlers = self._GetHandlers(all_names)

    if client_id:
      to_renders = self.GetProjections(client_id, names, name_blocks, handlers)
      to_renders = [(nm, proj) for (_, nm), proj in to_renders]

    else:
      to_renders = self.GetAggregates(names, name_blocks, handlers)
      to_renders = [(nm, agg) for nm, agg in to_renders]

    # Sort the Tabas to render by Taba Name
    to_renders = sorted(to_renders, key=lambda e: e[0])

    # Perform the rendering.
    renders = []
    for nm, to_render in to_renders:
      if nm in handlers and to_render:
        try:
          handler = handlers[nm]
          rendered = handler.Render(nm, [to_render])
          renders.extend(rendered)

        except Exception as e:
          LOG.error("Error rendering %s (%s)" % (nm, e))
          LOG.error(traceback.format_exc())

    return renders

  def GetClients(self):
    """Retrieve a list of all the known Client IDs.

    Returns:
      A list of Client ID strings.
    """
    op = self.dao.ClientIdsGet()
    if not op.success:
      raise Exception(op)

    return op.response_value

  def GetNames(self):
    """Retrieve a list of all known Taba Names.

    Returns:
      A list of all known Taba Names across all Clients
    """
    op = self.dao.TabaNamesForAllGet()
    if not op.success:
      raise Exception(op)

    return op.response_value

  def GetNamesForClient(self, client_id):
    """Retrieve a list of Taba Names associated with a given Client.

    Args:
      client_id - Client ID string to lookup Taba Names for.

    Returns:
      List of Taba Names associated with the given Client ID.
    """
    op = self.dao.TabaNamesForClientGet(client_id)
    if not op.success:
      raise Exception(op)

    return op.response_value

  def GetTabaTypes(self, names=None):
    """Retrieve the Taba Type for the given Name.

    Args:
      name - List of Taba Names to lookup the Type for.

    Returns:
      List of associated Taba Type identifier string.
    """
    if not names:
      op = self.dao.TabaNamesForAllGet()
      if not op.success:
        raise Exception(op)
      names = op.response_value

    op = self.dao.TabaTypeGetBatch(names)
    if not op.success:
      raise Exception(op)

    return op.response_value

  def PruneAll(self):
    """Traverse all States in the database, and remove the ones that should be
    pruned according to their handler.
    """
    LOG.info("Starting Prune")
    try:
      # Get a list of all Clients and Names, and get Handler.
      clients = self.GetClients()
      names = self.GetNames()

      handlers = self._GetHandlers(names)

      # Get an iterator over all state objects.
      states = self.dao.StateIterator()

      # Make lists to track which Clients and Names are still active.
      inactive_clients = dict([(c, True) for c in clients])
      inactive_names = dict([(n, True) for n in names])

      to_delete_batch = []
      last_client = None
      for (client_id, name), state in states:
        try:

          # Flush at Client boundaries (mainly for logging purposes)
          if client_id != last_client and to_delete_batch:
            LOG.info('Pruning %d States for %s' % \
                (len(to_delete_batch), last_client))

            self.dao.StateDeleteBatch(to_delete_batch)
            to_delete_batch = []
            last_client = client_id

          # Check if this State should be pruned. If yes, buffer the deletion,
          # otherwise, mark this Client and Name as active.
          handler = handlers[name]
          if state.version != handler.CURRENT_VERSION:
            state.payload = handler.Upgrade(state.payload, state.version)

          if handler.ShouldPrune(state.payload):
            to_delete_batch.append((client_id, name))

            if len(to_delete_batch) >= 100:
              LOG.info('Pruning %d States for %s' % \
                  (len(to_delete_batch), last_client))

              self.dao.StateDeleteBatch(to_delete_batch)
              to_delete_batch = []

          else:
            inactive_clients[client_id] = False
            inactive_names[name] = False

        except Exception as e:
          LOG.error("Error calling ShouldPrune() for %s:\n%s" % (name, e))
          LOG.error(traceback.format_exc())

        # Yield frequently, since this is a background operation.
        gevent.sleep(0)

      # Flush the buffer of States to delete.
      if to_delete_batch:
        self.dao.StateDeleteBatch(to_delete_batch)

      # Build lists of Clients and Names that are totally inactive.
      clients_to_remove = sorted([
          client
          for client, clear in inactive_clients.iteritems()
          if clear == True])

      names_to_remove = sorted([
          name
          for name, clear in inactive_names.iteritems()
          if clear == True])

      LOG.info('Completely removing clients: %s' % ', '.join(clients_to_remove))
      LOG.info('Completely removing names: %s' % ', '.join(names_to_remove))

      # Delete meta-data entries for the inactive Clients and Names.
      self.dao.ClientIdsRemove(clients_to_remove)
      self.dao.TabaNamesForAllRemove(names_to_remove)
      for client in clients_to_remove:
        self.dao.TabaNamesForClientDelete(client)

      LOG.info("Finished Prune")

    except Exception as e:
      LOG.error("Error Pruning: %s" % e)

  def DeleteTaba(self, name, client_id=None):
    """Delete the Taba State objects for a Taba Name, and optionally Client ID.

    Args:
      name - Taba Name to delete States of.
      client_id - Client ID for which to delete States for (or all Client IDs
          if not specified).
    """
    if not name:
      raise ValueError('Taba Name must be specified')

    # Get the iterator over existing States.
    states = self.GetStates(client_id, [name])

    # Iterate and delete the States.
    for (cli, name), _ in states:
      self.dao.StateDelete(cli, name)

    if client_id:
      self.dao.TabaNamesForClientRemove(client_id, [name])

    else:
      self.dao.TabaNamesForAllRemove([name])
      self.dao.TabaTypeDelete(name)

  def Upgrade(self, force_rewrite=False):
    """Iterate through all State objects, and Upgrade any that have an out of
    date version.

    Args:
      force_rewrite - If True, write back all state objects, regardless of
          whether they were upgraded.
    """
    # Retrieve all the Handlers by Taba Name.
    names = self.GetNames()
    handlers = self._GetHandlers(names)

    # Get an iterator over all States.
    states = self.GetStates()

    # Track and specify a method for puting updates back in the DB in batches.
    def _FlushUpdates(updates):
      if not updates:
        return

      op = self.dao.StatePutBatch(updates)
      if not op.success:
        LOG.error("Error putting Upgraded State ojects in the DB")
        LOG.error(op)

    updates = []

    # Iterate through the States, and upgrade the ones that need upgrading.
    for (client_id, name), state in states:
      handler = handlers[name]

      if state.version != handler.CURRENT_VERSION:
        state.payload = handler.Upgrade(state.payload, state.version)
        state.version = handler.CURRENT_VERSION

        updates.append(((client_id, name), state))

      elif force_rewrite:
        updates.append(((client_id, name), state))

      if len(updates) > 500:
        LOG.info("Flushing Upgrades (@ %s %s)" % (client_id, name))
        _FlushUpdates(updates)
        updates = []

    _FlushUpdates(updates)

  def Status(self):
    """Get the status of this Taba Server

    Returns:
      Dictonary of status variables
    """
    return {
        'applied_latency' : self.GetAverageAppliedLatency()}

  def _GetOrUpdateHandlers(self, name_guess_map):
    """Retrieve Handlers for Taba Names, and update Taba Types with guesses if
    for Taba Names that don't currently have an entry.

    Args:
      name_guess_map - Map of {Taba Name: Taba Type Guess}. The Taba Types for
          each Taba Name will be retrieved, and for any where the Taba Type is
          not known, the guess will be used in its place, and stored.

    Returns:
      Map of {Taba Name: Handler}.
    """
    # Retrieve the Taba Types for the Taba Names.
    op = self.dao.TabaTypeGetBatch(name_guess_map.keys())
    if not op.success:
      LOG.error('Error retrieving Taba Types')
      LOG.error(op)
      raise Exception(op)

    # Parse the response into a map, and build a list of updates.
    types = []
    updates = []
    for name, type in op.response_value:
      if type:
        types.append((name, type))
      else:
        guess = name_guess_map[name]
        types.append((name, guess))
        updates.append((name, guess))

    # If there are updated, apply them.
    if updates:
      op = self.dao.TabaTypePutBatch(updates)
      if not op.success:
        LOG.error('Error updating Taba Types')
        LOG.error(op)

    # Retrieve the Handlers for the Taba Types.
    handlers = {}
    for name, type in types:
      if type is None:
        LOG.error("Null type for name %s" % name)
        handlers[name] = None
        continue

      handler = taba_registry.GetHandler(type)
      if handler is None:
        LOG.error("Unknown Taba Type '%s'" % type)

      handler.server = self
      handlers[name] = handler

    return handlers

  def _GetHandlers(self, names):
    """Get a map of Taba Names to Handlers.

    Args:
      names - List of Taba Names to retrieve Handlers for.

    Returns:
      Map of {Taba Name: Handler}. If the Taba Type of Handler could not be
      found for a Taba Name, the entry in the map will be None.
    """
    op = self.dao.TabaTypeGetBatch(names)
    if not op.success:
      LOG.error('Error retrieving Taba Types')
      raise Exception(op)

    handlers = dict(itertools.izip(names, [None] * len(names)))
    for name, type in op.response_value:
      if type is None:
        LOG.error("Null type for name %s" % name)
        continue

      handler = taba_registry.GetHandler(type)
      if handler is None:
        LOG.error("Unknown Taba Type '%s'" % type)

      handler.server = self
      handlers[name] = handler

    return handlers

  def _DistributeRequestByTabaNameBlock(self, method, client_id=None):
    """Split a request into parts by Taba Name Block, and send each Block
    request to other Taba Server workers.

    Args:
      method - Method name string to make the request to (i.e. raw, projection,
          aggregate, etc.)

    Returns:
      List of response objects combined from all sub-requests.
    """
    # Calculate the block ranges to split the request into.
    total_blocks = self.dao.num_name_blocks
    group_size = 1

    block_ranges = [
        range(
            group * group_size,
            min(group * group_size + group_size, total_blocks))
        for group in xrange(math.ceil(total_blocks / group_size))]

    redistributor = RequestRedistributor(
        endpoints=self.server_endpoints,
        method=method,
        block_ranges=block_ranges,
        client_id=client_id)

    return redistributor.Run()

class RequestRedistributor(object):
  """Class for handling the redistribution of a request to multiple end-points,
  based on Taba Name Blocks. Each end-point has a pool of workers, which pull
  from the pool of all block ranges to process. Each time a request finishes,
  that end-point starts the next available block range. Requests have a soft
  timeout, which will put the block range back onto the queue, but still checks
  if the initial request finishes (the first result returned gets used).
  """

  def __init__(self,
      endpoints,
      method,
      block_ranges,
      client_id=None,
      soft_timeout=60,
      soft_timeout_max_parallel=2,
      endpoint_parallel_requests=16,
      max_retries=5):
    """
    Args:
      endpoints - List of available end-point DNS names.
      method - The Taba Server method to use (e.g. 'raw')
      block_ranges - List of lists of blocks to retrieve.
      client_id - Client ID to append to requests, if any.
      soft_timeout - Seconds to wait before the request soft timeout.
      soft_timeout_max_parallel - Maximum number of simultaneous parallel
          attempts of a task due to the soft timeout.
      endpoint_parallel_requests - Number of requests to execute in parallel
          to each end-point.
      max_retries - Maximum number of times to retry requests that return a
          recoverable failure.
    """
    self.endpoints = endpoints
    self.method = method
    self.block_ranges = block_ranges
    self.client_id = client_id

    self.soft_timeout = soft_timeout
    self.queue_size = endpoint_parallel_requests
    self.soft_timeout_max_parallel = soft_timeout_max_parallel
    self.max_retries = max_retries

    self.block_pool = None
    self.result_pool = None
    self.task_pools = None
    self.results_pending = None
    self.failures = None
    self.parallels = None

    self.task_done = None
    self.stop = False

  def Run(self):
    """Execute a redistribution request.

    Returns:
      Combined results dict.
    """
    self.block_pool = Queue()
    self.result_pool = Queue()
    self.results_pending = set()
    self.failures = defaultdict(int)
    self.parallels = defaultdict(int)
    self.task_done = Event()

    results = {}

    # Convert the block ranges into strings, and put them in the pool.
    for block_nums in self.block_ranges:
      blocks = ','.join(map(str, block_nums))

      self.block_pool.put(blocks)
      self.results_pending.add(blocks)

    # Start the requester loop.
    gevent.spawn(self._StartEndpointRequests)

    # Wait for the results to come it.
    while len(self.results_pending) > 0:

      # Wait for the next result to be ready.
      result_blocks, result = self.result_pool.get()

      # In case this block was attempted more than once, check if we already
      # have the result, and if so, ignore this one.
      if result_blocks not in self.results_pending:
        continue

      self.results_pending.remove(result_blocks)
      results[result_blocks] = result

    # We have all the results we need; tell the workers to stop.
    self.stop = True
    self.block_pool.put(None)
    self.task_done.set()

    # Re-combine the results.
    response_data = {}
    for blocks, response in results.iteritems():

      if not response.status_code == 200:
        LOG.error('Error in sub-query for method %s (%s)' % \
            (self.method, response.final_url))
        LOG.error(response)
        raise Exception(response)

      # Decode and merge the result (with yielding to be friendly)
      data = cjson.decode(response.body)
      gevent.sleep(0)

      response_data.update(data)
      gevent.sleep(0)

    return response_data

  def _StartEndpointRequests(self):
    """Manage the distribution of block ranges from the pool to individual
    end-point task pools. Blocks will be pulled from the block pool until the
    maximum requests per end-point are reached. When a block range task
    completes, the next block range is pulled from the pool.
    """
    self.task_pools = [(e, Pool(size=self.queue_size)) for e in self.endpoints]

    while not self.stop:

      # Find an end-point to send the next request to. Use the one with the
      # fewest active requests. If all the end-point pools are full, wait until
      # a task completes.
      endpoint, pool = sorted(
          self.task_pools, key=lambda e: e[1].free_count(), reverse=True)[0]

      # If the emptiest pool is full, then they are all full. Wait until at
      # least one task completes and try again.
      if pool.full():
        self.task_done.clear()
        self.task_done.wait()
        continue

      # Get the next block range from the queue.
      blocks = self.block_pool.get()

      self._WriteLog()

      # If the block value is None, this indicates that processing is complete.
      # Skip the value, and let the loop exit.
      if blocks is None:
        continue

      # Put the task into the pool.
      pool.spawn(self._RequestSingleTaskWithSoftTimeout, endpoint, blocks)

  def _RequestSingleTaskWithSoftTimeout(self, endpoint, blocks):
    """Make a single end-point request for a range of blocks, with a timeout. If
    the timeout expires, the request will continue, but the blocks will be put
    back into the pool for another worker to complete. The first results
    returned will be used.

    Args:
      endpoint - End-point public DNS name.
      blocks - Block range specification string (e.g. '1,2,3')
    """
    try:
      # Check whether another greenlet picked up this task between the time it
      # was scheduled and when it was able to run.
      if blocks not in self.results_pending:
        return

      # Attempt to execute the task normally, within the timeout.
      headers = {'Accept': 'text/json'}
      task = gevent.spawn(
          misc_util.GenericFetchFromUrlToString,
          self._MakeUrl(endpoint, blocks),
          headers)

      task.join(timeout=self.soft_timeout)

      # Check whether another greenlet picked up this task and completed it
      # first.
      if blocks not in self.results_pending:
        return

      if task.ready():
        # Task finished within the timeout. Submit the results.
        self._ProcessResult(blocks, task.get())

      else:
        # Task did not finish within the timeout. Leave the greenlet running,
        # but also put the blocks back into the pool to give another worker the
        # chance to finish the work faster.
        LOG.warning("Soft timeout for distributed request (%s %s)" % \
            (self.method, blocks))

        # Check whether we've reached the maximum number of parallel retries
        # before adding the block back to the pool.
        if self.parallels[blocks] < self.soft_timeout_max_parallel - 1:
          self.parallels[blocks] += 1
          self.block_pool.put(blocks)

        task.join()
        self._ProcessResult(blocks, task.get())

    finally:
      # Notify that a task just completed.
      self.parallels[blocks] = max(self.parallels[blocks] - 1, 0)
      self.task_done.set()

  def _ProcessResult(self, blocks, response):
    """Given a result from a single request, decide how to handle it. On
    success, put the result into the results pool. For certain recoverable
    failures, wait and retry the request.

    Args:
      blocks - Block range string of the request.
      response - Response object returned from the request.
    """
    if response.status_code in (500, 502, 503, 504) \
        and self.failures[blocks] < self.max_retries:
      # Caught a recoverable failure. Wait and retry the request.
      LOG.warning('Recoverable error for redistributed request (%s %s)' % \
          (self.method, blocks))

      self.failures[blocks] += 1
      gevent.sleep(self.failures[blocks] * (1 + random.random()))

      self.block_pool.put(blocks)

    else:
      # On success, or non-recoverable failure, just post the result.
      self.result_pool.put((blocks, response))

  def _MakeUrl(self, endpoint, blocks):
    """Generate the URL to request for a single task.

    Args:
      endpoint - End-point public DNS name.
      blocks - Block range specification string (e.g. '1,2,3')

    Returns:
      The URL string to request.
    """
    pattern = 'http://%(endpoint)s/%(method)s?block=%(blocks)s'
    if self.client_id:
      pattern += '&client=%s' % self.client_id

    url = pattern % {
        'endpoint': endpoint,
        'method': self.method,
        'blocks': blocks}

    return url

  def _WriteLog(self):
    """Write some statistics to the log.
    """
    LOG.info("Redistribute %s: Pending %s; Queues %s" % \
        (self.method,
         len(self.results_pending),
         [len(p) for _, p in self.task_pools]))
    LOG.info("Outstanding blocks: %s" % ', '.join(self.results_pending))

class _UpdateStateCallable(object):
  """Callable class for updating States in ReceiveEvents. Takes a current State
  object (or None if it doesn't exist yet), and returns a tuple of (updated
  state, projection)

  This is implemented as a callable class instead of a closure to improve
  memory performance and avoid leaks.
  """

  def __init__(self, handlers, taba_map):
    """
    Args:
      handlers - Map of {Taba Name: Handler}
      taba_map - Map of {(Client ID, Taba Name): [Events]}
    """
    self.handlers = handlers
    self.taba_map = taba_map

  def __call__(self, *args, **kwargs):
    return self._UpdateState(*args, **kwargs)

  def _UpdateState(self, client_id, name, state):
    handler = self.handlers[name]

    # Initialize the State if it doesn't exist yet.
    if not state:
      state = StateStruct(
          payload=handler.NewState(client_id, name),
          version=handler.CURRENT_VERSION)

    # Upgrade the version of the State if necessary.
    if state.version != handler.CURRENT_VERSION:
      state.payload = handler.Upgrade(state.payload, state.version)
      state.version = handler.CURRENT_VERSION

    # Fold in the new events, and generate the projection.
    state.payload = handler.FoldEvents(
       state.payload,
       self.taba_map[(client_id, name)])

    return state
