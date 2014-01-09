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

"""Classes for managing processing Queues.
"""

import cPickle as pickle
from collections import defaultdict
import functools
import heapq
import logging
import random
import time
import traceback
import zlib

import gevent

from taba.server.model import model_provider
from taba.server.model.identity_registry import ServerRoles
from taba.server.storage import util
from taba.util import thread_util
from taba.util.ttl_heartbeat_cache import TtlHeartbeatCache

LOG = logging.getLogger(__name__)

# Redis keys for the Shard <--> Server mapping.
KEY_SHARD_MAP = 'ShMapH'
KEY_LAST_COMPUTED = 'ShMapLast'
KEY_LOCK_NAME = 'ShMapLock'

# Redis keys for storing the processing queues.
KEY_PATTERN_SHARD_QUEUE = 'QSh%d'
KEY_PATTERN_SHARD_AGE = 'QShAg%d'

# Redis keys for assignment statistics.
KEY_QUEUE_WEIGHTS = 'QWg'
KEY_SERVER_WEIGHTS = 'SWg'

# The target probability that shard assignment recalculation will happen at each
# round. Lower values (exponentially) allow each server to attempt to
# participate in fewer rounds, lowering DB contention for the lock. (Setting
# this to 1.0 will force all servers to attempt to participate in each round).
RECALC_CLUSTER_PROB = 0.999

# Queue and Server Weight carry-over fractions on (0, 1]. Higher values will
# cause weights to change more slowly.
QUEUE_WEIGHT_DECAY = 0.75
SERVER_WEIGHT_DECAY = 0.75

# Number of Queues to shard updates into.
DEFAULT_NUM_SHARDS = 10000

# Default frequency at which Queue assignments are recalculated. Higher values
# reduce contention, but decrease responsiveness to changes in the cluster.
DEFAULT_SHARD_COMPUTATION_SEC = 120

# Default maximum number of Tasks to allow in all the Queues before rejecting
# all incoming updates from front-ends. Higher values allow for higher total
# throughput, at the expense of maximal latency.
DEFAULT_MAX_QUEUE_CAPACITY = 2000000

# Default number of Tasks in all the Queues at which point soft throttling of
# updates from front-ends begins. Higher values allow lower overall front-end
# latency, at the expense of higher back-end and database load.
DEFAULT_QUEUE_THROTTLE_CAPACITY = 50000

# Frequency at which the cached number of Tasks will be refreshed locally.
# Higher values increase responsiveness to Queue capacity, but increase
# database load.
QUEUE_MONITOR_FREQUENCY_SEC = 30

# Assigned Shards cache constants.
ASSIGNED_SHARDS_CACHE_TTL = 2
ASSIGNED_SHARDS_KEY = 'k'
ASSIGNED_SHARDS_VAL = 'v'

# Duration of the shard recalculation lock.
LOCK_DURATION_SEC = 10.0

# Maximum number of shards of process simultaneously when flushing the queues.
MAX_PARALLEL_FLUSH = 200

# Maximum size of the compressed data to retrieve for a single Task.
MAX_TASK_SIZE = 1024 * 1024 * 2

# Task generator constants.
TASK_QUEUE_LENGTH = 2
NUM_TASK_WORKERS = 2

class Task(object):
  """Contained objects for a single entry in a processing queue.
  """

  def __init__(self, nid, cid, sstate_part, enqueue_time):
    """
    Args:
      nid - NID of the task.
      cid - CID of the task.
      sstate_part - Serialized State binary string representing a partial State.
      enqueue_time - UTC creation timestamp of the Task (used to track queue
          processing latency).
    """
    self.nid = nid
    self.cid = cid
    self.sstate_part = sstate_part
    self.enqueue_time = enqueue_time

class QueueRegistry(object):
  """Class for managing processing Queues.
  """

  def __init__(self,
      engine,
      num_shards=DEFAULT_NUM_SHARDS,
      compute_sec=DEFAULT_SHARD_COMPUTATION_SEC,
      queue_capacity=DEFAULT_MAX_QUEUE_CAPACITY,
      queue_throttle_capacity=DEFAULT_QUEUE_THROTTLE_CAPACITY):
    """
    Args:
      engine - RedisEngine compatible storage manager.
      num_shards - Total number of processing queues to use.
      compute_sec - Frequency in seconds of queue assignment recalculation.
      queue_capacity - Total maximum number of Tasks allowed in all the queues.
      queue_throttle_capacity - Total number of Tasks allowed in all the queues
          before soft throttling of incoming updates is applied.
    """
    self.engine = engine

    self.num_shards = num_shards
    self.compute_freq = compute_sec
    self.queue_capacity = queue_capacity
    self.queue_throttle_capacity = queue_throttle_capacity

    self.assigned_shards_cache = TtlHeartbeatCache(
        ASSIGNED_SHARDS_CACHE_TTL,
        heartbeat_width=1)
    self.assigned_shards = []
    self.assigned_timestamp = None

    # Start the background process to contribute to shard re-mapping.
    self.stop = False
    self.worker = gevent.spawn(self._RecomputeWorker)

    # Start the background greenlet to monitor queue overflow.
    self.total_tasks = 0
    self.queue_monitor = thread_util.ScheduleOperationWithPeriod(
        QUEUE_MONITOR_FREQUENCY_SEC,
        self._QueueMonitor)

    # Task generator member.
    self.shard_queue = gevent.queue.Queue(maxsize=1)
    self.task_queue = gevent.queue.Queue(maxsize=1)

    self.shard_worker = None
    self.task_workers = None

    self.task_generator_started = False

  def ShutDown(self):
    """Shut-down hook. Stops background processes.
    """
    self.stop = True

  #########################################################
  # Front-end Interface
  #########################################################

  def CanAcceptUpdate(self):
    """Determine whether an update from the front-end should be accepted, based
    on the total number of Tasks in all the Queues. The rules are:

    - If tasks are below the 'throttle capacity', then new updated are always
      accepted.
    - If tasks it above the 'capacity', then new updates are always rejected.
    - If tasks is between the two capacities, then tasks are randomly rejected
      with a probability linearly related to the distence the number of tasks
      is between the two capacities (e.g. tasks == 'throttle capacity' -> 0%;
      tasks == 'capacity' -> 100%).

    Returns:
      True if the update is accepted; False if it is rejected.
    """
    if self.total_tasks > self.queue_capacity:
      LOG.error('Task queues are Full!')
      return False

    elif self.total_tasks > self.queue_throttle_capacity:
      pct_over = (
          float(self.total_tasks - self.queue_throttle_capacity) /
          float((self.queue_capacity - self.queue_throttle_capacity)))

      if random.random() < pct_over:
        LOG.warning('Throttling frontend updates')
        return False

      else:
        return True

    else:
      return True

  def EnqueueUpdate(self, nid, cid, sstate):
    """Put a batch of State updates in the queue for reducing into the global
    state.

    Args:
      nid - NID being updated.
      cid - CID of the update.
      sstate - Serialized State binary string.

    Returns:
      Operation object.
    """
    now = time.time()
    shard = self._ShardForNid(nid)

    payload = zlib.compress(pickle.dumps(Task(nid, cid, sstate, now)))

    queue_op = util.StrictOp('enqueuing for %d:%d to %d' % (cid, nid, shard),
        self.engine.ListLeftPush,
        KEY_PATTERN_SHARD_QUEUE % shard, payload)

    # Set the shard age flag. We only set it if it doesn't exist, since if it
    # does, it will have a value older than the timestamp we were attempting to
    # put. The flag therefore represents the oldest Task in the queue.
    age_op = util.StrictOp('setting update time for shard queue %d' % shard,
        self.engine.PutNotExist,
        KEY_PATTERN_SHARD_AGE % shard, now)

    return util.CompoundOperation(queue_op, age_op)

  def _QueueMonitor(self):
    """Cache the total number of Tasks that are in the processing queue.
    """
    try:
      keys = [KEY_PATTERN_SHARD_QUEUE % s for s in xrange(self.num_shards)]
      op = self.engine.ListLenBatch(keys)
      if not op.success:
        return

      self.total_tasks = sum(map(int, op.response_value))

    except Exception:
      LOG.error('Exception in queue monitor')
      LOG.error(traceback.format_exc())

  #########################################################
  # Back-end Interface
  #########################################################

  def GetTask(self):
    """Pull a chunk of work from one of the queues assigned to this server
    process.

    Returns:
      Tuple of (
          {NID, {CID: [SerializedState]}},
          [Enqueue Times])
    """
    if not self.task_generator_started:
      self._InitializeTaskGenerator()

    return self.task_queue.get()

  def _InitializeTaskGenerator(self):
    """Start the task generator components.
    """
    self.shard_worker = gevent.spawn(self._ShardWorker)
    self.task_workers = [
        gevent.spawn(self._TaskWorker) for _ in xrange(NUM_TASK_WORKERS)]

    self.task_generator_started = True

  def _TaskWorker(self):
    """Worker to produce tasks for this server process. Pulls shard numbers
    from the Shards Queue and puts associated Tasks on the Task Queue.
    """
    while True:
      try:
        # Determine the next shard that to process.
        shard = self.shard_queue.get()
        if shard is None:
          continue

        # Retrieve the list of updates to apply for this shard, and remove the
        # queue age marker. Note that we delete the age marker first, so that
        # in the event of a concurrent write, we won't have updates without an
        # age marker.
        util.StrictOp('removing age key for shard %d' % shard,
            self.engine.Delete,
            KEY_PATTERN_SHARD_AGE % shard)

        queue_op = util.StrictOp('pulling updates for shard %d' % shard,
            self.engine.ListGetAndDeleteAtomic,
            KEY_PATTERN_SHARD_QUEUE % shard)

        if not queue_op.response_value:
          continue

        # Format the returned task data.
        task_data = defaultdict(functools.partial(defaultdict, list))
        enqueue_times = []
        task_len = 0

        for task_enc in queue_op.response_value:
          task = pickle.loads(zlib.decompress(task_enc))
          task_data[task.nid][task.cid].append(task.sstate_part)
          enqueue_times.append(task.enqueue_time)
          task_len += len(task_enc)

          if task_len > MAX_TASK_SIZE:
            model_provider.GetLatency().RecordForDequeue(enqueue_times)
            self.task_queue.put((task_data, enqueue_times))
            gevent.sleep(0)

            task_data = defaultdict(functools.partial(defaultdict, list))
            enqueue_times = []
            task_len = 0

        model_provider.GetLatency().RecordForDequeue(enqueue_times)
        self.task_queue.put((task_data, enqueue_times))

      except Exception:
        LOG.error('Error in task generator loop')
        LOG.error(traceback.format_exc())

  def _ShardWorker(self):
    """Worker to produce shard numbers for this server to process, based on the
    current Queue assignments. Puts shard numbers onto the Shards Queue.
    """
    shard_generator = self._NextShardGenerator()

    while True:
      gevent.sleep(0)

      try:
        self.shard_queue.put(shard_generator.next())

      except Exception:
        LOG.error('Error in shard generator loop')
        LOG.error(traceback.format_exc())

  def _NextShardGenerator(self):
    """Generator which yields a stream of the next queue number to process.
    Queue numbers are generated in order of oldest pending Task (although that
    is not strictly guaranteed).
    """
    while True:
      try:
        # Fetch the task age of all the shards assigned to this server.
        shards = self.GetAssignedShardNumbers()
        shard_age_keys = [KEY_PATTERN_SHARD_AGE % s for s in shards]

        op = util.StrictOp('retrieving shard ages',
            self.engine.BatchGet,
            shard_age_keys)

        if not op.response_value:
          yield None
          continue

        shard_ages = filter(
            lambda e: e[1] is not None,
            zip(shards, op.response_value))

        if not shard_ages:
          yield None
          continue

        # Sort the shards from oldest to newest. Yield the shard numbers in that
        # order (we can do this without having to go back to the database since
        # we know that any shard age added since we last fetched must be newer
        # than the newest from the last fetch).
        shard_ages = sorted(shard_ages, key=lambda e: float(e[1]))

        for shard, age in shard_ages:
          # Double-check that the shard is still assigned to this server.
          shards = self.GetAssignedShardNumbers()
          if age is not None and shard in shards:
            yield shard

      except Exception:
        LOG.error('Exception in _NextShardGenerator')
        LOG.error(traceback.format_exc())
        yield None

  def FlushAll(self):
    """Delete all pending Tasks in all queues. Use with caution!
    """
    def _flush_shard(shard):
      util.StrictOp('flushing age key for shard %d' % shard,
          self.engine.Delete,
          KEY_PATTERN_SHARD_AGE % shard)

      util.StrictOp('flushing shard %d' % shard,
          self.engine.Delete,
          KEY_PATTERN_SHARD_QUEUE % shard)

    for i in xrange(0, self.num_shards, MAX_PARALLEL_FLUSH):
      greenlets = [
          gevent.spawn(_flush_shard, shard)
          for shard in xrange(i, i + MAX_PARALLEL_FLUSH)]
      gevent.joinall(greenlets)

  #########################################################
  # Shard Mapping Methods
  #########################################################

  def GetAssignedShardNumbers(self):
    """Retrieve the list of queue numbers assigned to the current server.

    Returns:
      List of queue numbers (integers).
    """
    # Check in the cache to determine whether the TTL has expired.
    sentinel = self.assigned_shards_cache[ASSIGNED_SHARDS_KEY]
    if sentinel is not None:
      return self.assigned_shards

    # The cache has expired -- check the last updated timestamp for the mapping
    # in the DB. If it hasn't changed since we last fetched the mapping, use the
    # current values.
    op = util.CheckedOp('retrieving last shard mapping computation',
        self.engine.Get,
        KEY_LAST_COMPUTED)
    assignment_time = op.response_value

    if assignment_time and assignment_time == self.assigned_timestamp:
      # Mapping hasn't changed -- refresh the TTL and return the last mapping.
      self.assigned_shards_cache[ASSIGNED_SHARDS_KEY] = ASSIGNED_SHARDS_VAL
      return self.assigned_shards

    # The mapping has changed since it was last fetched -- re-fetch and update
    # the local values.
    name = model_provider.GetIdentity().GetName()
    op = util.StrictOp('retrieving shards for server %s' % name,
        self.engine.HashGet,
        KEY_SHARD_MAP, name)

    self.assigned_timestamp = assignment_time
    self.assigned_shards_cache[ASSIGNED_SHARDS_KEY] = ASSIGNED_SHARDS_VAL

    if not op.response_value:
      self.assigned_shards = []
    else:
      self.assigned_shards = map(int, op.response_value.split(','))

    return self.assigned_shards

  def ForceRecomputeShards(self):
    """Recompute the shard mapping, whether or not it's needed based on the last
    computation time. This is useful e.g. when a new server comes up, so that it
    is added to the backend rotation immediately.
    """
    self._TryRecomputeMapping(force=True)

  def _RecomputeWorker(self):
    """Worker loop to monitor the time until the next mapping re-computation
    and execute that process when appropriate.
    """
    while not self.stop:
      try:

        # Randomly determine whether to participate in this round. As the
        # number of registered servers increases, each individual server has to
        # participate less often, even while maintaining a similar probability
        # of the entire operation happening at each round.
        num_servers = float(model_provider.GetIdentity().NumServers())
        threshold = (1.0 - RECALC_CLUSTER_PROB) ** (1.0 / num_servers)
        if random.random() < threshold:
          gevent.sleep(self.compute_freq)
          continue

        # Check the last computation time of the shard mapping.
        next_compute_delay = self._GetSecsUntilNextRecompute()
        if next_compute_delay is None:
          gevent.sleep(1)  # Avoid a fast loop on lookup failures.
          continue

        # If we passed the next computation time, attempt to set the new mapping
        if next_compute_delay <= 0:
          try:
            self._TryRecomputeMapping()
          except Exception:
            LOG.error('Error attempting to recompute mapping')
            LOG.error(traceback.format_exc())

          next_compute_delay = self.compute_freq

        # Sleep the remainder of the gap, with some randomness so that all
        # clients don't wake up at the same time (+- 2%).
        gevent.sleep(next_compute_delay * (1 - 0.04 * (random.random() - 0.5)))

      except Exception:
        LOG.error('Unknown exception in shard recompute worker.')
        LOG.error(traceback.format_exc())

  def _GetSecsUntilNextRecompute(self):
    """Retrieve the last update time for the shard mapping from the DB, and
    compute the number of seconds until the next update.

    Returns:
      Number of seconds until the next re-computation (may be fractional). If
      the DB lookup failed, None is returned.
    """
    op = util.CheckedOp('retrieving last shard mapping computation',
        self.engine.Get,
        KEY_LAST_COMPUTED)

    if not op.success:
      return None

    last_compute = float(op.response_value) if op.response_value else 0
    return last_compute + self.compute_freq - time.time()

  def _TryRecomputeMapping(self, force=False):
    """Compute the current desired shard mapping, and attempt to write it into
    the DB. The operation is done with a global lock, since it's possible (and
    likely) that multiple clients will attempt to recompute the mapping. The
    first server to acquire the lock gets precedence.

    Args:
      force - If True, recompute the mapping, whether or not it needs it.
    """
    # Acquire the global lock on the shard mapping. Also double-check that
    # another server didn't update the mapping between the time we identified
    # it needed updating, and when we acquire the lock.
    with self.engine.Lock(KEY_LOCK_NAME, LOCK_DURATION_SEC):
      if self._GetSecsUntilNextRecompute() > 0 and not force:
        return

      LOG.info('Recalculating Queue Assignments.')

      # Pull the data needed for the computation.
      servers, _ = model_provider.GetIdentity().GetAllServers(
          ServerRoles.BACKEND)

      if len(servers) == 0:
        util.StrictOp('updating shard mapping last updated',
            self.engine.Put,
            KEY_LAST_COMPUTED, '%.3f' % time.time())

        util.StrictOp('setting new shard mapping',
            self.engine.Delete,
            KEY_SHARD_MAP)

        util.StrictOp('updating Queue weights',
            self.engine.Delete,
            KEY_QUEUE_WEIGHTS)

        util.StrictOp('updating Server weights',
            self.engine.Delete,
            KEY_SERVER_WEIGHTS)

        return

      queue_weight_op = util.StrictOp('retrieving Queue weights',
          self.engine.HashGetAll,
          KEY_QUEUE_WEIGHTS)
      queue_weights = dict(
          (int(k), float(v))
          for k, v in queue_weight_op.response_value.iteritems())

      server_weight_op = util.StrictOp('retrieving Queue weights',
          self.engine.HashGetAll,
          KEY_SERVER_WEIGHTS)
      server_weights = dict(
          (k, float(v))
          for k, v in server_weight_op.response_value.iteritems())

      len_op = util.StrictOp('retrieving Queue Lengths',
          self.engine.ListLenBatch,
          [KEY_PATTERN_SHARD_QUEUE % s for s in xrange(self.num_shards)])
      queue_lengths = map(int, len_op.response_value)

      map_op = util.StrictOp('retrieving shard mapping',
          self.engine.HashGetAll,
          KEY_SHARD_MAP)
      current_mapping = dict(
          (server, map(int, shards_str.split(',') if shards_str else []))
          for server, shards_str in map_op.response_value.iteritems())

      # Compute and encode the new desired mapping.
      new_mapping, new_qweights, new_server_weights = self._ComputeMapping(
          servers.keys(),
          current_mapping,
          queue_weights,
          queue_lengths,
          server_weights)

      # Put the updated data in the database.

      #
      # TODO: Compress this somehow (makes a really long string).
      #
      new_mapping_enc = dict(
          (server, ','.join(map(str, shards)))
          for server, shards in new_mapping.iteritems())

      util.StrictOp('setting new shard mapping',
          self.engine.HashReplace,
          KEY_SHARD_MAP, new_mapping_enc)

      util.StrictOp('updating shard mapping last updated',
          self.engine.Put,
          KEY_LAST_COMPUTED, '%.3f' % time.time())

      util.StrictOp('updating Queue weights',
          self.engine.HashReplace,
          KEY_QUEUE_WEIGHTS, new_qweights)

      util.StrictOp('updating Server weights',
          self.engine.HashReplace,
          KEY_SERVER_WEIGHTS, new_server_weights)

  def _ComputeMapping(self,
      server_names,
      current_mapping,
      queue_weights,
      queue_lengths,
      server_weights):
    """Core logic to compute the desired mapping of {Server Name: [Queues]}.

    Args:
      server_names - List of Server name strings to distribute Queues among.
      current_mapping - Current assignment of {Server Name: [Queue Numbers]}
      queue_weights - Current Queue weights: {Queue Number: Weight}
      queue_lengths - List of number of tasks in each Queue.
      server_weights - Current Server weights: {Server Name: Weight}

    Returns:
      Tuple of (
          New Server => Queue mapping: {Server Name: [Queue Numbers]},
          Updated Queue weights: {Queue Number: Weight},
          Updated Server weights: {Server Name: Weight})
    """
    all_tasks = float(sum(queue_lengths))
    num_queues = self.num_shards

    # Calculate the instantaneous load of each Queue.
    if all_tasks:
      queue_loads = [queue_lengths[q] / all_tasks for q in xrange(num_queues)]
    else:
      queue_loads = [0.0] * num_queues

    # Update the decayed Queue weights with the current loads.
    new_qweights = {}
    for q in xrange(num_queues):
      old_weight = queue_weights.get(q, 1)
      load = queue_loads[q]
      new_qweights[q] = load + (old_weight - load) * QUEUE_WEIGHT_DECAY

    # Calculate the instantaneous load of each Server.
    server_loads = {}
    for server in server_names:
      shards = current_mapping.get(server, [])
      if all_tasks:
        server_loads[server] = (
            sum(queue_lengths[q] for q in shards if q < len(queue_lengths))
            / all_tasks)
      else:
        server_loads[server] = 0

    # Update the decayed Server weights with the current loads.
    new_sweights = {}
    for server in server_names:
      load = server_loads.get(server, 1)
      old_weight = server_weights.get(server, 1)
      new_sweights[server] = load + (old_weight - load) * SERVER_WEIGHT_DECAY

    min_weight = min(new_sweights.values())
    max_weight = max(new_sweights.values())

    if min_weight == max_weight:
      new_sweights_norm = new_sweights
    else:
      new_sweights_norm = dict(
          (server, (sweight - min_weight) / (max_weight - min_weight))
          for server, sweight in new_sweights.iteritems())

    # Distribute the Queues among the Servers. Each Server is tracked with it's
    # total assigned weight (initialized to 0) in a heap. The first (and
    # therefore least assigned) Server is repeatedly pop'ed from the heap,
    # assigned the next Queue, and put back in the heap. The weight each Queue
    # contributes to each Server is partially scaled by the Server's weight.
    # This heuristic, while not optimal, will more-or-less evenly distribute
    # Tasks to Servers, based on each Server's exhibited capacity.
    server_assignments = defaultdict(list)

    server_total_weight = [(0, s) for s in server_names]
    heapq.heapify(server_total_weight)

    for q, qweight in thread_util.YieldByCount(new_qweights.iteritems(), 10):
      total_weight, server = heapq.heappop(server_total_weight)

      server_assignments[server].append(q)

      updated_weight = total_weight + qweight * (new_sweights_norm[server] + 1)

      heapq.heappush(server_total_weight, (updated_weight, server))

    if model_provider.debug:
      self._PrintQueueAssignmentDebugInfo(
          queue_lengths, queue_loads, queue_weights, new_qweights,
          current_mapping, server_names, server_loads, server_weights,
          new_sweights, server_assignments)

    return server_assignments, new_qweights, new_sweights

  #########################################################
  # Monitoring
  #########################################################

  def GetQueueStats(self):
    """Retrieve current statistics about Queues and Servers.

    Returns:
      Dict of statistics per server containing:
        queues - Number of Queues assigned to the Server.
        tasks - Number of total Tasks pending for the Server.
        age - Age in seconds of the oldest pending Task for the Server.
        load - Relative load on the Server (in per-mill).
        weight - Current handicap of the Server used to calculate assignments.
    """
    now = time.time()

    # Max Task Age.
    age_op = util.StrictOp('retrieving Queue Ages',
        self.engine.BatchGet,
        [KEY_PATTERN_SHARD_AGE % s for s in xrange(self.num_shards)])
    ages = [
        now - float(a) if a is not None else 0
        for a in age_op.response_value]

    # Number of Tasks.
    len_op = util.StrictOp('retrieving Queue lengths',
        self.engine.ListLenBatch,
        [KEY_PATTERN_SHARD_QUEUE % s for s in xrange(self.num_shards)])
    lengths = len_op.response_value

    # Server => Queues mapping.
    map_op = util.CheckedOp('retrieving Queue map',
        self.engine.HashGetAll,
        KEY_SHARD_MAP)
    shard_map = map_op.response_value if map_op.success else None

    # Server weights.
    server_weight_op = util.StrictOp('retrieving Queue weights',
        self.engine.HashGetAll,
        KEY_SERVER_WEIGHTS)
    weights = server_weight_op.response_value

    # Build the output.
    all_tasks = float(sum(lengths))
    server_stats = {}

    for server, shards_str in shard_map.iteritems():
      shards = map(int, shards_str.split(',')) if shards_str else []

      server_ages = filter(None, [ages[s] for s in shards])
      server_age = max(server_ages) if server_ages else 0

      tasks = sum([lengths[s] for s in shards])
      load = (tasks / all_tasks) - 1.0 / len(shard_map) if all_tasks else 0.0
      weight = float(weights.get(server, 0))

      server_stats[server] = {
          'queues': len(shards),
          'tasks': tasks,
          'age': server_age,
          'load': load * 1000,
          'weight': weight * 1000}

    return {
        'total_tasks': all_tasks,
        'max_age': max(ages) if ages else 0,
        'servers': server_stats}

  #########################################################
  # Misc.
  #########################################################

  def _PrintQueueAssignmentDebugInfo(self,
      qlengths, qloads, qweights, new_qweights, old_mapping, snames, sloads,
      sweights, new_sweights, new_mapping):
    """Print to the logger detailed information about a queue assignment run.
    """
    # Convert float values to per-mill for easy printing/reading.
    pm = lambda v: int(v * 1000)

    for server in sorted(snames):
      queues =  old_mapping.get(server, None)
      LOG.info('  S) %s | T: %4s | L: %3d | W: %3s => %3d | Q: %4d | NT: %d' % (
          server,
          sum(qlengths[q] for q in queues if q < len(qlengths)) \
              if queues is not None else 'xx',
          pm(sloads[server]),
          pm(sweights[server]) if server in sweights else 'xx',
          pm(new_sweights[server]),
          len(new_mapping[server]),
          sum(qlengths[q] for q in new_mapping[server])))

  def _ShardForNid(self, nid):
    """Calculate the Shard Number for an NID.

    Args:
      nid - Name ID.

    Returns:
      Shard number on (0, self.num_shards-1).
    """
    # Mix the bits in the NID so that we have a more uniform distribution
    # (especially for the case where the number of NIDs < self.num_shards)
    h = nid
    h = ((h >> 16) ^ h) * 0x45d9f3b;
    h = ((h >> 16) ^ h) * 0x45d9f3b;
    h = ((h >> 16) ^ h);

    return h % self.num_shards
