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

"""Class implementing the Taba Server backend processing. The backend pulls
Tasks out of Queues assigned to it, and reduces them in to the main State
records. The Garbage Collector also runs as part of the backend.
"""

import logging
import random
import time
import traceback

import gevent

from taba.server.model import model_provider
from taba.server.storage import util
from taba.util.instrumentation_util import Timer

# Default number of parallel background workers.
DEFAULT_WORKERS = 4

# Default frequency at which the automatic database Garbage Collector will run.
DEFAULT_GC_FREQUENCY_SEC = 60 * 60   # 1 hour

# Database key storing the last time the Garbage Collector ran.
KEY_LAST_PASS_TIME = 'GcTm'

# Constants for the Garbage Collector lock.
KEY_LOCK_NAME = 'GcLock'
LOCK_DURATION_SEC = 60 * 5  # 5 minutes

LOG = logging.getLogger(__name__)

class TabaServerBackend(object):
  """Taba Server back-end processing manager.
  """

  def __init__(self,
      engine,
      initialize=True,
      num_workers=DEFAULT_WORKERS,
      gc_frequency=DEFAULT_GC_FREQUENCY_SEC):
    """
    Args:
      engine - RedisEngine compatible storage manager.
      initialize - If True, start the background workers.
      num_workers - The number of parallel background worker greenlets.
      gc_frequency - Frequency (in seconds) at which to run the database
          Garbage Collector.
    """
    self.engine = engine
    self.num_workers = num_workers
    self.gc_frequency = gc_frequency

    self.stop = False

    if initialize:
      self.Initialize()

  def Initialize(self):
    """Start the background workers (Queue processor and Garbage Collector)
    """
    self.queue_workers = [
        gevent.spawn(self._EventQueueWorker) for _ in xrange(self.num_workers)]
    self.gc_worker = gevent.spawn(self._GarbageCollectorWorker)

  def ShutDown(self):
    """Stop all background workers.
    """
    self.stop = True

  def _EventQueueWorker(self):
    """Worker loop for the Queue processor. Continuously pulls Tasks off the
    Queues assigned to this process and reduces them into the main States.
    """
    while not self.stop:
      gevent.sleep(0)

      try:
        with Timer('task_process_time', False) as t:
          # Pull the next task off the queue.
          task, enqueue_times = model_provider.GetQueues().GetTask()

          # Retrieve the Tab Types for the NIDs so we know what handler to use.
          nid_to_type = model_provider.GetNames().GetTypes(task.keys())

          # Add the queued state parts to the main states.
          greenlets = [
              gevent.spawn(
                  model_provider.GetStates().ReduceInto,
                  nid, cid_to_sstate, nid_to_type[nid])
              for nid, cid_to_sstate in task.iteritems()]

          gevent.joinall(greenlets)

        LOG.info('Processed tasks %3dN %5dC %7.3fs' % (
            len(task), len(enqueue_times), t.Elapsed))

        model_provider.GetLatency().RecordForProcessed(enqueue_times)

      except Exception:
        LOG.error('Exception in Queue Worker loop')
        LOG.error(traceback.format_exc())
        gevent.sleep(1)

  #########################################################
  # Garbage Collection Background Worker
  #########################################################

  def _GarbageCollectorWorker(self):
    """Worker loop for the Garbage Collector. Continuously monitors the time
    until the next pass, and when it comes up, attempts to start a collection
    pass.
    """
    while not self.stop:
      try:
        # Check the last computation time of the shard mapping.
        next_pass_delay = self._GetSecsUntilNextPass()
        if next_pass_delay is None:
          gevent.sleep(1)  # Avoid a fast loop on lookup failures.
          continue

        # If we passed the next computation time attempt to run the GC.
        if next_pass_delay <= 0:
          with self.engine.Lock(KEY_LOCK_NAME, LOCK_DURATION_SEC):
            if self._GetSecsUntilNextPass() > 0:
              continue
            self._ResetLastPassTime()

          model_provider.GetStates().RunGarbageCollector()
          next_pass_delay = self._GetSecsUntilNextPass()

        # Sleep the remainder of the gap, with some randomness so that all
        # clients don't wake up at the same time (+- 2%).
        gevent.sleep(next_pass_delay + (1 - 0.04 * (random.random() - 0.5)))

      except Exception:
        LOG.error('Error in Garbage Collector loop.')
        LOG.error(traceback.format_exc())

  def _GetSecsUntilNextPass(self):
    """Get the time until the next pass from the database.

    Returns:
      Seconds until the next Garbage Collection pass.
    """
    op = util.CheckedOp('retrieving last GC pass time',
        self.engine.Get,
        KEY_LAST_PASS_TIME)

    if not op.success:
      return None

    last_compute = float(op.response_value) if op.response_value else 0
    return last_compute + self.gc_frequency - time.time()

  def _ResetLastPassTime(self):
    """Reset the next collection pass entry in the database.
    """
    util.StrictOp('updating last GC pass time',
        self.engine.Put,
        KEY_LAST_PASS_TIME, '%.3f' % time.time())
