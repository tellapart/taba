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

Concurrency utilities for Taba.
"""

import time
import traceback

import gevent
from gevent import Timeout
from gevent import pool
from gevent import queue

from tellapart.taba.util import misc_util

LOG = misc_util.MakeStreamLogger(__name__)

def ScheduleOperationWithPeriod(period_seconds, fn, *args, **kwargs):
  """Calls a function with the given arguments every 'period_seconds' seconds.

  Args:
    period_seconds - How long, in float seconds, to wait between function calls.
    fn - A Python callable to invoke.
    *args, **kwargs - The arguments to pass to 'fn'.

  Returns:
    The greenlet in which the scheduled operation will run.  To cancel the
    periodic calling of the operation, call kill() on the returned greenlet.
  """
  def _WaitAndCall():
    while True:
      gevent.sleep(period_seconds)
      fn(*args, **kwargs)

  return gevent.spawn(_WaitAndCall)

def PerformOperationWithTimeout(timeout_seconds, fn, *args, **kwargs):
  """Calls a function with the given arguments, but times out after
  timeout_seconds have elapsed. The function is called in the same greenlet. On
  a timeout an exception is thrown and caught.

  NOTE: The function must cooperatively yield (via non-blocking I/O or an
  explicit gevent.sleep() call).

  Args:
    timeout_seconds - How long, in float seconds, to give the function before
        timing out and returning
    fn - A Python callable to invoke.
    *args, **kwargs, The arguments to pass to 'fn'.

  Returns:
    A (return_value, timed_out) tuple, where 'return_value' is the value
    returned from the called function ('fn'), and 'timed_out' is a boolean value
    indicating whether 'fn' timed out when called.  If 'timed_out' is True,
    'return_value' is None.
  """
  try:
    returned_value = gevent.with_timeout(timeout_seconds, fn, *args, **kwargs)
  except Timeout:
    return (None, True)

  # Small workaround for a feature of with_timeout which returns the value
  # supplied with the kwarg timeout_value. If this value was returned then the
  # function timedout without throwing Timeout.
  if 'timeout_value' in kwargs and returned_value == kwargs['timeout_value']:
    return (None, True)

  return (returned_value, False)

class GreenletWorkerPool(object):
  """A pool of Greenlets that perform an operation on a large amount of input,
  and optionally passes the results along to one or more consumer WorkerPool
  instances.

  Note that in reasonable usage, one will want the callable to cooperatively
  yield via gevent; either through explicit gevent calls, or through the monkey
  patched calls.

  After a WorkerPool has been instantiated and consumer pool(s) registered with
  AddConsumerPool(), call Start() to begin work and Finish() to end it.
  """
  def __init__(self, num_workers, callable, poll_timeout_secs=10, name=None,
               queue_size=None):
    """
    Args:
      num_workers - The max number of concurrent greenlets in this pool.
      callable - Python callable that will be invoked with each task input. The
          callable should return the tuple that will be passed as input to all
          registered consumer pools, or None if nothing should be passed to
          consumer pools.
      poll_timeout_secs - How often the pool will check to see if it has been
          signaled to finish.
      name - An optional name for the worker pool; used in logging.
      queue_size - The size of the underlying input queue. If this is None, the
          size is unbounded, and adding a task input to this pool will never
          block the calling thread. If queue_size > 0, the queue may get filled
          up, in which case, adding a task input will block the calling thread.
    """
    self.name = name
    self.poll_timeout_secs = poll_timeout_secs
    self.callable = callable

    self.queue = queue.JoinableQueue(maxsize=queue_size)
    self.driver = gevent.Greenlet(self._Driver)
    self.finish_signaled = gevent.event.Event()
    self.pool = pool.Pool(num_workers)

    self.consumer_pools = []
    self.producer_pools = []
    self.producer_pools_finished = 0

    self.counter_time_since_add = None
    self.counter_num_inputs_added = None
    self.counter_queue_size_at_processing_time = None

  def Start(self):
    """Instruct the worker pool to begin processing its input.
    """
    self.driver.start()
    LOG.debug('Started greenlet worker pool %s' % repr(self.name))

  def Finish(self):
    """Instruct the worker pool to finish running.

    This method is used by the caller to signal that no additional input will be
    added to this worker pool.  The pool will not actually finish executing
    until all input has been consumed, including input from producer worker
    pools that feed this worker pool.
    """
    self.finish_signaled.set()
    LOG.debug('Signaled greenlet worker pool %s to finish' % repr(self.name))

  def AddSingleTaskInput(self, input, from_producer=False):
    """Add a single work item to the pool's work queue.

    Args:
      input - A tuple of arguments suitable for passing to the callable object
          registered with this worker pool at instantiation time.
      from_producer - External callers should never set this. Specifies whether
          the given input is from a producer pool.
    """
    if self.finish_signaled.is_set() and not from_producer:
      raise Exception(
        'Cannot add more input to the work queue after Finish() is called')

    # Each task put into the queue is a (input, added_time) tuple.
    self.queue.put((input, time.time()))
    if self.counter_num_inputs_added:
      self.counter_num_inputs_added.RecordValue(1)

  def RegisterConsumerPool(self, consumer_pool):
    """Registers a separate WorkerPool as a consumer of this worker pool's
    output.

    The consumer pool will not finish running until this pool finishes
    running so that all input is processed.

    Args:
      consumer_pool - A WorkerPool to register as a consumer of this  worker
                      pool's output.
    """
    self.consumer_pools.append(consumer_pool)
    consumer_pool.producer_pools.append(self)

  def Wait(self):
    """Blocks the calling thread until this worker pool has fully terminated.
    The worker pool has terminated when its driver thread finishes.
    """
    if self.driver.ready():
      return
    return self.driver.join()

  @property
  def finished(self):
    """Whether this worker pool has fully terminated.
    """
    return self.driver.ready()

  def _ProducerFinished(self):
    """Called by a producer pool when it finishes.
    """
    self.producer_pools_finished += 1

  def _Driver(self):
    """Function called within a separate greenlet that gets requests from the
    queue and puts them into the pool.
    """
    while True:
      if self.counter_queue_size_at_processing_time:
        queue_size = self.queue.qsize()
        self.counter_queue_size_at_processing_time.RecordValue(queue_size)

      try:
        task = self.queue.get(block=True, timeout=self.poll_timeout_secs)
        self.pool.spawn(self._InvokeCallable, task)

      except queue.Empty:
        pass

      if (self.finish_signaled.is_set() and self.queue.empty()
          and self._ProducerPoolsFinished()):
        break

    self.queue.join()
    for consumer_pool in self.consumer_pools:
      consumer_pool._ProducerFinished()

  def _InvokeCallable(self, task):
    """Helper method that calls through to the Python callable given at
    instantiation time.
    """
    try:
      args, added_time = task
      if self.counter_time_since_add:
        # Record how much time elapsed since the input was added.
        delta_seconds = time.time() - added_time
        self.counter_time_since_add.RecordValue(delta_seconds * 1000)

      result = self.callable(*args)
      self._FeedConsumerPools(result)

    except Exception:
      LOG.error(traceback.format_exc())

    finally:
      self.queue.task_done()

  def _FeedConsumerPools(self, result):
    """Callback function used internally to distribute the output of this pool's
    workers to the registered consumer pools.
    """
    if result is None:
      return

    for pool in self.consumer_pools:
      pool.AddSingleTaskInput(result, from_producer=True)

  def _ProducerPoolsFinished(self):
    """Returns whether all producer pools to this pool have finished or not.
    """
    return self.producer_pools_finished == len(self.producer_pools)
