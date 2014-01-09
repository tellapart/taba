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

"""Gevent concurrency utilities.
"""

import functools
import logging
import traceback

import gevent
from gevent import Timeout
from gevent import queue

LOG = logging.getLogger(__name__)

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
  @functools.wraps(fn)
  def _WaitAndCall():
    while True:
      gevent.sleep(period_seconds)
      try:
        fn(*args, **kwargs)
      except Exception:
        LOG.error('Exception in periodic operation %s' % fn.__name__)
        LOG.error(traceback.format_exc())

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
    'return_value' is unspecified.
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

def BatchedAsyncIterator(tasks, get_fn, yield_fn, workers=3, buffer_len=8):
  """Split a list of 'tasks' into a set of background workers, and yield the
  results in an iterator as they complete. Useful for retrieving a large set of
  database records without overloading the database with all the requests at
  once, while still having a local buffer as full as possible.

  Args:
    tasks - Iterable of 'task' objects. Passed to get_fn one at a time.
    get_fn - Callable with one argument, given an element of tasks, and returns
        the result of acting on the task.
    yield_fn - Callable which accepts the original task element and the result,
        and returns a transformation into the value that will be yielded by the
        iterator.
    workers - Number of concurrent greenlets to employ.
    buffer_len - Number of queue slots to hold results.

  Returns:
    Iterator/generator on the task results.
  """
  # Split the lookups into batches, and start a background greenlet to
  # retrieve the batches. Use a queue to retrieve results so that they can be
  # processes as soon as they are available, and limit the size of the queue
  # to control memory usage.
  result_queue = queue.Queue(buffer_len)

  # TODO(kevin): Add instrumentation to this method, especially on the Queue.

  task_iter = iter(tasks)
  def _GetBatchWorker():
    while True:

      try:
        task = task_iter.next()
      except StopIteration:
        result_queue.put(None)
        return

      try:
        result = get_fn(task)
        result_queue.put((task, result))

      except Exception as e:
        LOG.error('Exception in BatchedAsyncIterator')
        LOG.error(traceback.format_exc())
        result_queue.put(e)

  workers = [gevent.spawn(_GetBatchWorker) for _ in xrange(workers)]

  # Extract the results as long as there are unprocessed slices or there are
  # results available.
  while not all([w.ready() for w in workers]) or not result_queue.empty():
    gevent.sleep(0)
    item = result_queue.get()
    if item is None or isinstance(item, BaseException):
      continue

    task, result = item
    yield yield_fn(task, result)

class AsyncProcessor(object):
  """Process a sequence of tasks in parallel. Tasks are pulled from the task
  iterator and placed in a queue for worker greenlets to pull from. The size of
  the task queue is limited, so that only a small number of tasks are pulled
  from the iterator at a time (useful if the task iterator is dynamically
  generating the tasks). Callbacks are provided for processing the tasks, and
  dealing with the returned results.
  """

  def __init__(self, tasks, process_fn, result_fn, workers=3):
    """
    Args:
      tasks - Iterator of tasks.
      process_fn - Callable with the signature fn(task) -> result. Called on
          every task.
      result_fn - Callable with the signature fn(result). Called on every task
          result.
      workers - Number of parallel worker greenlets.
    """
    self.tasks = tasks
    self.process_fn = process_fn
    self.result_fn = result_fn
    self.workers = workers

    self.sentinel = object()

  def Process(self):
    """Begin processing the tasks.

    TODO(kevin): Add instrumentation on this mechanism.
    """
    task_queue = queue.Queue(self.workers * 2)

    # Create the worker objects.
    worker_objs = [
        _AsyncProcessrWorker(
            task_queue,
            self.process_fn,
            self.result_fn,
            self.sentinel)
        for _ in xrange(self.workers)]

    # Spawn a greenlet for each worker.
    worker_greenlets = [gevent.spawn(w.Run) for w in worker_objs]

    # Pull tasks and put them in the queue. This will block once the task queue
    # is full, regulating the number of tasks in-flight at any given time.
    for task in self.tasks:
      task_queue.put(task)

    # All the tasks have been added to the queue. Add the sentinel object for
    # each worker, which tells them that processing it complete.
    for _ in worker_objs:
      task_queue.put(self.sentinel)

    # Wait for the workers to finish.
    while not all([w.ready() for w in worker_greenlets]):
      gevent.sleep(0)

class _AsyncProcessrWorker(object):
  """AsyncProcessor worker object.
  """
  __slots__ = ['task_queue', 'process_fn', 'result_fn', 'sentinel',]

  def __init__(self, task_queue, process_fn, result_fn, sentinel):
    """
    Args:
      task_queue - Queue to pull tasks from.
      process_fn - Callable to pass tasks to.
      result_fn - Callable to pass results to.
      sentinel - Special object which, when pulled from the task queue,
          signals to stop processing.
    """
    self.task_queue = task_queue
    self.process_fn = process_fn
    self.result_fn = result_fn
    self.sentinel = sentinel

  def Run(self):
    while True:

      task = self.task_queue.get()
      if task is self.sentinel:
        return

      try:
        result = self.process_fn(task)
        self.result_fn(result)

      except Exception:
        LOG.error('Exception in AsyncProcessor')
        LOG.error(traceback.format_exc())

def YieldByCount(iter, yield_interval=5000):
  """Generator which wraps an iterable, and yields to other greenlets at a fixed
  number of elements.

  Args:
    iter - An iterable
    yield_interval - The number of elements between yields.
  """
  gevent.sleep(0)

  for i, item in enumerate(iter):
    yield item

    if (i % yield_interval) == 0:
      gevent.sleep(0)
