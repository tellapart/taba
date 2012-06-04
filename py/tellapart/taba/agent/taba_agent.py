# Copyright 2012 TellApart, Inc.
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

"""
Taba Agent core functionality. The Taba Agent is a process which runs
along-side Taba Clients. It acts as a smart buffer and proxy to the Taba Server.
"""

from collections import defaultdict
import traceback
import zlib

import gevent

from tellapart.taba.util import misc_util
from tellapart.taba.util import thread_util

LOG = misc_util.MakeStreamLogger(__name__)

class TabaAgent(object):
  """Class encapsulating the functionality of the Taba Agent. Accepts Taba
  Events from multiple Taba Clients, buffers them shortly, and flushes the
  events to the Taba Server. Handles retrying failed flush requests.
  """
  def __init__(self):
    self.max_buffer_size = None
    self.buffer = defaultdict(list)

    self.last_url_index = -1
    self.server_event_urls = []

    self.pending_requests = 0
    self.pending_events = 0

    self.request_workers = thread_util.GreenletWorkerPool(
        num_workers=32,
        callable=self._FlushSingleRequest)

  def _CurrentBufferSize(self):
    """Retrieve the current number of buffered events across all Taba Clients.

    Returns:
      The number of buffered Taba Events.
    """
    buffered_size = sum([len(e) for e in self.buffer.values()])
    return buffered_size + self.pending_events

  def Initialize(self,
      server_endpoints,
      agent_flush_period,
      max_buffer_size=5000000,
      max_events_per_request=80000,
      max_requests_per_url=2,
      dummy_mode=False):
    """Initializes the Taba Agent. Starts the periodic flushing operation.

    Args:
      server_endpoints - List of Taba Server end-point dictionaries, containing
          'host' and 'port' fields for each end-point.
      agent_flush_period - The period, in seconds, at which the Taba Agent
          should flush its buffer.
      max_buffer_size - The total number of Taba Events to hold in the buffer
          before discarding new Events.
      max_events_per_request - The maximum number of Taba Events to place in a
          single request.
      max_requests_per_url - The maximum number of simultaneous requests to
          send to any particular Taba Server end-point.
      dummy_mode - If True, flushing to the Server is disabled.
    """
    self.max_buffer_size = max_buffer_size
    self.max_request_events = max_events_per_request
    self.max_pending_reqs = max_requests_per_url
    self.dummy_mode = dummy_mode

    # Generate the Taba Server end-point URLs
    self.server_event_urls = [
        'http://%s:%d/post_zip' % (e['host'], e['port'])
        for e in server_endpoints]

    # Array of number of pending requests per URL (indices match URLs in
    # self.server_event_urls)
    self.server_requests_pending = [0] * len(self.server_event_urls)

    # Start periodic flushing
    thread_util.ScheduleOperationWithPeriod(agent_flush_period, self.Flush)

  def Flush(self):
    """Flush the buffered Taba Events to the Taba Server.
    """
    # If there are too many pending requests, wait until they flush. (Helps
    # build larger single requests which process more efficiently)
    if self.pending_requests > \
        self.max_pending_reqs * len(self.server_event_urls) * 2:
      return

    # Move the buffer into local variables and reset it.
    to_send_events = sum([len(e) for e in self.buffer.values()])
    to_send = self.buffer
    self.buffer = defaultdict(list)

    if self.dummy_mode:
      return True

    # Generate a request body from the buffered Events, and add it to the queue.
    # If there are more than the maximum number of Events per request, split it
    # into several requests. Each request is handled by it's own greenlet, which
    # is responsible for retrying failed requests.
    #
    # The format of the body is a series of newline separated objects. Each
    # section starts with the Client ID, followed by a list of Events, and
    # ending with a blank line. The entire body is then compressed for transfer.
    if to_send_events > 0:
      for client_id, events in to_send.iteritems():
        for i in xrange(0, len(events), self.max_request_events):
          # Build the body according to the protocol.
          body_lines = []
          body_lines.append(client_id)
          body_lines.extend(events[i:i + self.max_request_events])
          body_lines.append('')

          # Compress and encrypt.
          body = zlib.compress('\n'.join(body_lines))

          # Spawn a greenlet to flush the request to the Taba Server.
          size = min(self.max_request_events, len(events))
          request_bunch = {
              'client_id': client_id,
              'body': body,
              'size': size,
              'tries': 0}

          self.pending_events += size
          self.pending_requests += 1

          gevent.spawn(self._FlushSingleRequest, request_bunch)

  def _FlushSingleRequest(self, request):
    """Flushes a single request to the Taba Server, handling retries of failed
    requests across different server end-points.

    Args:
      request - Dictionary containing the body and metadata of the request to
          flush.
    """
    success = False
    url_index = None

    # Function for generating the next URL index. Pick the initial URL with an
    # affinity for sending requests for the same Client ID to the same place (to
    # help reduce transactional collisions). If the queue for that Server is
    # full (i.e. it has too many pending requests), or the request fails, choose
    # the next server round-robin.
    def _next_url_index(url_index):
      if url_index is None:
        url_index = hash(request['client_id']) % len(self.server_event_urls)

      else:
        url_index += 1
        if url_index >= len(self.server_event_urls):
          url_index = 0

      return url_index

    try:
      while not success:
        # If all the request 'queues' are full, wait until one is free.
        while all([
            bool(reqs >= self.max_pending_reqs)
            for reqs in self.server_requests_pending]):
          gevent.sleep(1)

        # Choose the next URL index.
        url_index = _next_url_index(url_index)
        while self.server_requests_pending[url_index] >= self.max_pending_reqs:
          url_index = _next_url_index(url_index)

        # Perform the actual request.
        self.server_requests_pending[url_index] += 1
        try:
          response = misc_util.GenericFetchFromUrlToString(
              self.server_event_urls[url_index],
              post_data=request['body'])

          if response.status_code == 200:
            success = True

          else:
            LOG.error("Failed to flush buffer to server")
            LOG.error(response)
            gevent.sleep(1)

        except Exception:
          LOG.error("Failed to send data")
          LOG.error(traceback.format_exc())

        finally:
          self.server_requests_pending[url_index] -= 1

        # If successful, update the buffered size. Otherwise put the request
        # back on the pending queue.
        if not success:
          gevent.sleep(0)
          request['tries'] += 1

    finally:
      self.pending_events -= request['size']
      self.pending_requests -= 1

  def Buffer(self, client_id, events):
    """Add a list of Taba Events to the buffer for a Taba Client. If the buffer
    if full, the Events may be discarded.

    Args:
      client_id - The Taba Client ID string to buffer Events for.
      events - A list of encoded Taba Events to buffer.
    """
    cur_size = self._CurrentBufferSize()
    new_size = len(events)

    if new_size == 0:
      return

    if cur_size >= self.max_buffer_size:
      LOG.error("Buffer Full! Discarding %d events for %s" % \
          (new_size, client_id))
      return

    if cur_size + new_size > self.max_buffer_size:
      LOG.error("Buffer Full! Discarding %d events for %s" % \
          (new_size + cur_size - self.max_buffer_size, client_id))
      events = events[:self.max_buffer_size - cur_size]

    self.buffer[client_id].extend(events)

  def Status(self):
    """Retrieve the current status of the Taba Agent.

    Returns:
      A JSON string of the Taba Agent status with fields:
        buffered_events - The number of Taba Events in the buffer.
        buffer_pct - The percent usage of the buffer.
        unprocessed_events - The number of Events not yet processed into a
            request body.
        pending_events - The number of Events processed into a request body and
            awaiting successful posting to a Taba Server.
        pending_requests - The number of outstanding Taba Server requests.
    """
    buffer_size = self._CurrentBufferSize()

    status = {
      'buffered_events': buffer_size,
      'buffer_pct': float(buffer_size) / self.max_buffer_size,
      'unprocessed_events': sum([len(e) for e in self.buffer.values()]),
      'pending_events': self.pending_events,
      'pending_requests': self.pending_requests,
    }

    return status
