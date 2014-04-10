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

import logging
import threading
import time
import traceback

from taba.client.async_engine import BufferedAsyncClientEngine

LOG = logging.getLogger(__name__)

class ThreadEngine(BufferedAsyncClientEngine):
  """TabaClientEngine which uses Python threading to schedule a periodic flush
  of buffered events.
  """

  def __init__(self, client_id, event_post_url, flush_period):
    """Override. See super-class definition.
    """
    super(ThreadEngine, self).__init__(client_id, event_post_url, flush_period)
    self.thread = None

  def Initialize(self):
    """Start periodically flushing the Event buffer.
    """
    self.thread = PeriodicThread(self.flush_period, self._Flush)
    self.thread.start()

  def Stop(self):
    """Override. See super-class definition.
    """
    if self.thread is not None:
      self.thread.stop()
    self._Flush()

class PeriodicThread(threading.Thread):
  """Thread class that will execute a callable at a fixed period.

  Note that we aren't using thread_util here so that the client can support
  applications that not use gevent.
  """

  def __init__(self, period, fn, *args, **kwargs):
    """
    Args:
      period - Interval in seconds at which the periodic operation will execute.
      fn - Callable which will be executed.
      args - Positional arguments to pass to fn.
      kwargs - Keyword arguments to pass to fn.
    """
    super(PeriodicThread, self).__init__()
    self.period = period
    self.fn = fn
    self.args = args
    self.kwargs = kwargs

    self.stop_loop = threading.Event()

  def run(self):
    """Override. See super-class definition.
    """
    while not self.stop_loop.isSet():
      time.sleep(self.period)
      try:
        self.fn(*self.args, **self.kwargs)
      except Exception:
        LOG.error('Exception in periodic operation %s' % self.fn.__name__)
        LOG.error(traceback.format_exc())

  def stop(self):
    """Flag the periodic operation to stop.
    """
    self.stop_loop.set()
