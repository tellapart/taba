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

"""Class for tracking the Taba Server cluster event latency.
"""

import logging
import time

from taba import client
from taba.handlers.tab_type import TabType
from taba.server.model import model_provider
from taba.server.storage import util
from taba.util import instrumentation_util
from taba.util import thread_util

LOG = logging.getLogger(__name__)

# Redis key of the per-server latency Hash.
KEY_LATENCY_HASH = 'Latency'

# Period in seconds at which to sync with the database.
CLUSTER_LATENCY_REFRESH_FREQUECY_SEC = 30

# Period in seconds for which a database latency record will be used to compute
# the total cluster latency.
EXPIRY = CLUSTER_LATENCY_REFRESH_FREQUECY_SEC * 10

# Percentile of latencies to pull from local samples.
PERCENTILE = 0.50

# Constants for the events per second logger.
LOGGER_FREQUENCY_SEC = 60
LOGGER_POINTS = 100

# Tab objects.
taba_events = client.Tab('taba_events_recieved')

taba_enqueue = client.Tab(
    'taba_latency_enqueue_sec',
    type=TabType.PERCENTILE_GROUP)

taba_dequeue = client.Tab(
    'taba_latency_task_in_queue_ms',
    type=TabType.PERCENTILE_GROUP)

taba_processed = client.Tab(
    'taba_latency_task_processed_ms',
    TabType.PERCENTILE_GROUP)

class LatencyTracker(object):
  """Class for tracking total event latencies locally, and synchronizing with
  all the servers in the cluster through the database.
  """

  def __init__(self, engine):
    """
    Args:
      engine - RedisEngine compatible storage manager.
    """
    self.engine = engine

    self.enqueue_sampler = instrumentation_util.Sampler(10, 600)
    self.processed_sampler = instrumentation_util.Sampler(10, 600)

    self.latency_cache = {}

    self._RefreshClusterLatency()
    thread_util.ScheduleOperationWithPeriod(
        CLUSTER_LATENCY_REFRESH_FREQUECY_SEC,
        self._RefreshClusterLatency)

    # Initialize the logger greenlet.
    self.event_hist = [0]
    thread_util.ScheduleOperationWithPeriod(LOGGER_FREQUENCY_SEC, self._Logger)

  def _Logger(self):
    """Worker function to log the running events/second estimate to the local
    log file.
    """
    rate = sum(self.event_hist) / (LOGGER_FREQUENCY_SEC * len(self.event_hist))
    if rate == 0:
      return

    LOG.info(">>> Events/s: %d" % rate)

    self.event_hist = [0] + self.event_hist
    if len(self.event_hist) > LOGGER_POINTS:
      self.event_hist = self.event_hist[-LOGGER_POINTS:]

  def RecordForPost(self, event_bundle):
    """Record the latency for events being posted to the front-end. This is the
    'enqueue' latency, i.e. the time between the event being created and when it
    arrives at the Taba Server.

    Args:
      event_bundle - EventBundle object.
    """
    now = time.time()

    sum_latency, count = 0, 0
    for event in event_bundle:
      latency = now - event.timestamp

      self.enqueue_sampler.AddValue(latency, now)
      sum_latency += latency
      count += 1

    self.event_hist[0] += count

    avg_latency = sum_latency / count if count else 0
    taba_enqueue.RecordValue(avg_latency)

    taba_events.RecordValue(count)

  def RecordForDequeue(self, enqueue_times):
    """Record the latency for a Task, from the time it was added to a Queue,
    until it was pulled off by a back-end worker.

    Args:
      enqueue_times - List of UTC timestamps of when Tasks were added to Queues.
    """
    now = time.time()
    for enqueue_time in enqueue_times:
      taba_dequeue.RecordValue((now - enqueue_time) * 1000)

  def RecordForProcessed(self, enqueue_times):
    """Record the latency for a Task, from the time it was added to a Queue,
    until it was fully processed and committed to the State records.

    Args:
      enqueue_times - List of UTC timestamps of when Tasks were added to Queues.
    """
    now = time.time()
    for enqueue_time in enqueue_times:
      taba_processed.RecordValue((now - enqueue_time) * 1000)
      self.processed_sampler.AddValue(now - enqueue_time, now)

  def GetClusterLatency(self):
    """Retrieve the average percentile latency from the cluster.

    Return:
      Cluster latency in seconds.
    """
    total_lats = [en + pr for en, pr in self.latency_cache.values()]
    return sum(total_lats) / len(total_lats) if total_lats else 0

  def GetStatistics(self):
    """Retrieve a dict of cluster latency statistics.

    Returns:
      Dict of {
        'enqueue_avg': Average cluster enqueue latency.
        'processed_avg': Average cluster processed latency.
    """
    enqueue_lats = [e for e, _ in self.latency_cache.values()]
    procesed_lats = [p for _, p in self.latency_cache.values()]

    en_avg = sum(enqueue_lats) / len(enqueue_lats) if enqueue_lats else 0
    pr_avg = sum(procesed_lats) / len(procesed_lats) if procesed_lats else 0

    return {
        'enqueue_avg': en_avg,
        'processed_avg': pr_avg,
        'cluster_aggregate': self.GetClusterLatency(), }

  def _RefreshClusterLatency(self):
    """Put the local latencies into the database, and pull the full cluster
    data-set into the local cache.
    """
    now = time.time()

    local_enqueue_latency = self.enqueue_sampler.GetPercentile(PERCENTILE)
    local_processed_latency = self.processed_sampler.GetPercentile(PERCENTILE)

    latency_str = '%.3f|%.2f|%.2f' % (
        now,
        local_enqueue_latency,
        local_processed_latency)
    name = model_provider.GetIdentity().GetName()

    put_op = util.CheckedOp('putting local latency',
        self.engine.HashPut,
        KEY_LATENCY_HASH, name, latency_str)

    if not put_op or not put_op.success:
      return

    get_op = util.CheckedOp('getting latency map',
        self.engine.HashGetAll,
        KEY_LATENCY_HASH)

    if not get_op or not get_op.success or not get_op.response_value:
      return

    expired = set()
    latencies = {}
    for name, latency_str in get_op.response_value.iteritems():
      put_time, enqueue_lat, processed_lat = map(float, latency_str.split('|'))
      if now - EXPIRY < put_time:
        latencies[name] = (enqueue_lat, processed_lat)
      else:
        expired.add(name)

    self.latency_cache = latencies

    if expired:
      util.CheckedOp('removing expired latencies',
          self.engine.HashBatchDelete,
          KEY_LATENCY_HASH, list(expired))
