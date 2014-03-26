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

from collections import defaultdict
import logging
import time
import traceback

import cjson
import requests

from taba.client.engine import TabaClientEngine
from taba.common import transport

LOG = logging.getLogger(__name__)

class BufferedAsyncClientEngine(TabaClientEngine):
  """Base class of TabaClientEngine that buffers events and periodically flushes
  them to a remote endpoint over HTTP. The actual scheduling of the period
  flush is left to sub-classes.
  """

  def __init__(self, client_id, event_post_url, flush_period):
    """Override. See super-class definition.
    """
    self.flush_period = flush_period
    self.event_post_url = event_post_url

    self.buffer = defaultdict(list)
    self.buffer_size = 0
    self.client_id = client_id
    self.failures = 0

    self.session = requests.session()

  def RecordEvent(self, name, tab_type, value):
    """Override. See super-class definition.
    """
    self.buffer[name].append(transport.MakeEventTuple(tab_type, time.time(), value))
    self.buffer_size += 1

  def Status(self):
    """Override. See super-class definition.
    """
    status = {
        'failures' : self.failures,
        'buffer_size' : len(self.buffer_size)}
    return cjson.encode(status)

  def _Flush(self):
    """Serialize and flush the internal buffer of Events to the Taba Agent.
    """
    try:
      num_events = self.buffer_size
      if num_events == 0:
        return

      flush_buffer = self.buffer
      self.buffer = defaultdict(list)
      self.buffer_size = 0

      body = transport.Encode(self.client_id, flush_buffer)

      response = self.session.post(self.event_post_url, body)

      if response.status_code != 200:
        LOG.error('Failed to flush %d Taba buffer to Agent' % num_events)
        self.failures += 1

    except Exception:
      LOG.error('Exception flushing Taba Client buffer.')
      LOG.error(traceback.format_exc())
