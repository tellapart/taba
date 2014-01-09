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

"""Taba Agent core functionality. The Taba Agent is a process which runs
along-side Taba Clients. It acts as a smart buffer and proxy to the Taba
Servers.
"""

from collections import defaultdict
import hashlib
import itertools
import logging
import random
import time
import traceback
import zlib

import gevent
import requests

from taba.common import transport
from taba.util import thread_util

LOG = logging.getLogger(__name__)

class TabaAgent(object):
  """Class encapsulating the functionality of the Taba Agent. Accepts Events
  from multiple Clients, buffers them shortly, and flushes the events to the
  Taba Servers. Handles retrying failed flush requests.
  """
  def __init__(self):
    self.max_buffer_size = None
    self.max_request_events = None

    self.server_event_urls = None
    self.url_shard_mapper = None
    self.buffer_manager = None

    self.stop = False

  def Initialize(self,
      server_endpoints,
      agent_flush_period=1,
      max_buffer_size=5000000,
      max_events_per_request=60000,
      queues_per_url=1,
      num_shards=750,
      load_threshold=0.04,
      unload_cooldown=60):
    """Initializes the Taba Agent.

    Args:
      server_endpoints - List of Taba Server end-point dictionaries, containing
          'host' and 'port' fields for each end-point.
      agent_flush_period - The period, in seconds, at which the Taba Agent
          should flush its buffer. If 0, puts the Agent into 'passive' mode,
          where no background workers will be started (useful for testing).
      max_buffer_size - The total number of Events to hold in the buffer before
          discarding new Events.
      max_events_per_request - The maximum number of Events to place in a
          single request to a Taba Server end-point.
      queues_per_url - The number of parallel queues to use per end-point URL.
      num_shards - Number of virtual shards to use for distributing load.
      load_threshold - Threshold for a given end-point at which load will start
          being unassigned. Valid values are from 0 to 1.
      unload_cooldown - After load is unassigned from an end-point, wait this
          many seconds before removing load from that end-point again.
    """
    self.max_buffer_size = max_buffer_size
    self.max_request_events = max_events_per_request
    self.flush_period = agent_flush_period
    self.num_shards = num_shards

    # Generate the Taba Server end-point URLs
    self.server_event_urls = [
        'http://%s:%d/post_zip' % (e['host'], e['port'])
        for e in server_endpoints]

    # Initialize the URL => Shard mapper.
    self.url_shard_mapper = UrlShardMapper(
        urls=self.server_event_urls,
        num_shards=self.num_shards,
        load_threshold=load_threshold,
        unload_cooldown=unload_cooldown)

    # Initialize the buffer manager.
    self.buffer_manager = TabaAgentBufferManager(
        urls=self.server_event_urls,
        num_shards=self.num_shards,
        url_shard_mapper=self.url_shard_mapper,
        max_events_per_request=max_events_per_request)

    # Initialize the pending request tracker.
    self.pending_events_by_url = dict([
        (url, 0) for url in self.server_event_urls])
    self.pending_requests_by_url = dict([
        (url, 0) for url in self.server_event_urls])

    if agent_flush_period != 0:
      # Start the background worker that pulls from the shard request queues
      # for each URL.
      for url in self.server_event_urls:
        for _ in xrange(queues_per_url):
          gevent.spawn(self._UrlBackgroundWorker, url)

      # Start the periodic re-balancing task.
      thread_util.ScheduleOperationWithPeriod(10, self.Rebalance)

  def Stop(self):
    """Instruct the background workers to stop.
    """
    self.stop = True

  def Buffer(self, client_id, name_events_map):
    """Add a list of Events to the buffer for a Client. If the buffer if full,
    the Events may be discarded.

    Args:
      client_id - The Client ID string to buffer events for.
      name_events_map - A map of {Tab Name: [Encoded Events]} to buffer.
    """
    new_size = sum([len(events) for events in name_events_map.values()])
    if new_size == 0:
      return

    # Check whether the buffer is full.
    buffer_info = self.buffer_manager.GetStats()
    pending_events = sum(self.pending_events_by_url.values())
    if buffer_info.total_events + pending_events >= self.max_buffer_size:
      LOG.error("Buffer Full! Discarding %d events for %s" % \
          (new_size, client_id))
      return

    # Add the new events to the buffer manager.
    for name, events in name_events_map.iteritems():
      self.buffer_manager.AddEvents(client_id, name, events)

  def Status(self):
    """Retrieve the current status of the Taba Agent.

    Returns:
      A JSON string of the Taba Agent status.
    """
    buffer_info = self.buffer_manager.GetStats()

    # Build a status record for each URL.
    url_stats = {}
    for url in self.server_event_urls:
      shards = self.url_shard_mapper.GetShards(url)

      buffered_events = sum([
          buffer_info.buffered_events_by_shard[shard]
          for shard in shards])

      total_events = buffered_events + buffer_info.queued_events_by_url[url] + \
          self.pending_events_by_url[url]

      url_stats[url] = {
          'shards': len(shards),
          'total_events': total_events,
          'buffered_events': buffered_events,
          'queued_events': buffer_info.queued_events_by_url[url],
          'queued_requests': buffer_info.queued_requests_by_url[url],
          'pending_events': self.pending_events_by_url[url],
          'pending_requests': self.pending_requests_by_url[url], }

    all_total = sum([stat['total_events'] for stat in url_stats.values()])
    for stat in url_stats.values():
      stat['load_factor'] = 1000 * (\
          float(stat['total_events']) / all_total - 1.0 / len(url_stats) \
          if all_total else 0)

    pending_events = sum(self.pending_events_by_url.values())
    all_events = buffer_info.total_events + pending_events

    status = {
      'all_events': all_events,
      'buffered_events': buffer_info.buffered_events,
      'buffer_pct': float(all_events) / self.max_buffer_size,
      'queued_events': buffer_info.queued_events,
      'queued_requests': buffer_info.queued_requests,
      'pending_events': sum(self.pending_events_by_url.values()),
      'pending_requests': sum(self.pending_requests_by_url.values()),
      'url_stats': url_stats, }

    return status

  def Rebalance(self):
    """Call the mapper's re-balancing algorithm.
    """
    # Generate a mapping of URL to total buffered Events.
    buffer_info = self.buffer_manager.GetStats()

    url_events = {}
    for url in self.server_event_urls:
      buffered_events = sum([
          buffer_info.buffered_events_by_shard[shard]
          for shard in self.url_shard_mapper.GetShards(url)])

      url_events[url] = \
          buffered_events + \
          buffer_info.queued_events_by_url[url] + \
          self.pending_events_by_url[url]

    # Have the mapper perform re-balancing.
    self.url_shard_mapper.Rebalance(url_events)

  def _UrlBackgroundWorker(self, url):
    """Worker which continuously polls the shard request queues assigned to a
    URL and flushes any requests to the Taba Server URL.

    Args:
      url_index - URL string to flush requests for.
    """
    while not self.stop:
      try:
        start = time.time()

        while not self.stop:
          # Process the requests for this URL.
          self._UrlRequestProcessor(url)

          # If there is a request data object queues for this URL, yield and
          # continue. Otherwise, if the time elapsed in this iteration is less
          # than the flush period, wait the remainder of the period. Otherwise,
          # just yield.
          if self.buffer_manager.IsRequestQueued(url):
            gevent.sleep(0)

          else:
            elapsed = time.time() - start
            gevent.sleep(max(0, self.flush_period - elapsed))
            break

      except Exception:
        LOG.error('Unknown exception in URL background worker.')
        LOG.error(traceback.format_exc())

  def _UrlRequestProcessor(self, url):
    """Perform a single pass through the shard request queues assigned to a URL,
    and flush any requests.

    Args:
      url - URL string to flush requests for.

    Returns:
      True if a request data object was available. False otherwise.
    """
    # Get the next request data object to transmit.
    request_data = self.buffer_manager.GetRequest(url)
    if request_data is None:
      return False

    # Update pending statistics.
    self.pending_events_by_url[url] += request_data['size']
    self.pending_requests_by_url[url] += 1

    # Transmit the request and check the response.
    success = False
    try:
      response = requests.post(url, request_data['body'])

      if response.status_code == 200:
        success = True

      elif response.status_code in (502, 503):
        # Ignore 502 and 503 errors, as they are temporary. Add a slight delay
        # to avoid an unnecessarily tight loop.
        gevent.sleep(1)

      else:
        LOG.error("Failed to flush buffer to server")
        LOG.error(response)

    except Exception:
      LOG.error("Failed to send data")
      LOG.error(traceback.format_exc())

    finally:
      # Make sure to properly update pending statistics.
      self.pending_events_by_url[url] -= request_data['size']
      self.pending_requests_by_url[url] -= 1

      # If the operation was not successful, put the request back onto the
      # queue to be processed later.
      if not success:
        request_data['tries'] += 1
        self.buffer_manager.AddRequest(url, request_data)

    return success

class UrlShardMapper(object):
  """Class for managing and publishing the mapping from shards to URLs.
  """

  def __init__(self,
      urls,
      num_shards,
      load_threshold,
      unload_cooldown):
    """
    Args:
      urls - List of URL string to map to.
      num_shards - Total number of shards to include in the mapping.
      load_threshold - Threshold for a given URL at which shards will start
          being unassigned. Valid values are from 0 to 1.
      unload_cooldown - After a shard is unassigned from a URL, wait this many
          seconds before removing another shard from that URL.
    """
    self.urls = urls
    self.num_shards = num_shards
    self.load_threshold = load_threshold
    self.unload_cooldown = unload_cooldown

    self.last_unload_by_url = dict([(url, 0) for url in urls])

    # Populate the initial mapping evenly using round-robin.
    self.mapping = dict([(url, set()) for url in urls])

    url_loop = itertools.cycle(urls)
    for shard_index in xrange(self.num_shards):
      self.mapping[url_loop.next()].add(shard_index)

  def GetShards(self, url):
    """Get the shards currently mapped to the given URL.

    Args:
      url - URL string.

    Returns:
      A Set of shard that map to the given URL.
    """
    return self.mapping[url]

  def Rebalance(self, url_queue_sizes):
    """Based on the number of Events buffered for each URL, reassign shards from
    overloaded to under-loaded URLs.

    Args:
      url_queue_sizes - Map of {URL: Total number of buffered Events}
    """
    # Calculate the buffer statistics.
    total = sum(url_queue_sizes.values())
    average = float(total) / len(url_queue_sizes)

    url_load = dict([
        (url, (queue - average) / total if total else 0)
        for url, queue in url_queue_sizes.iteritems()])

    # Calculate which URLs are overloaded or under-loaded.
    now = time.time()

    urls_overloaded = []
    urls_underloaded = []

    for url, load in url_load.iteritems():
      if load > self.load_threshold:

        # If the URL was unloaded within the cool-down period, or it's only
        # managing 1 shard, don't unload it any further.
        if now - self.last_unload_by_url[url] > self.unload_cooldown \
            and len(self.mapping[url]) > 1:
          urls_overloaded.append(url)

      elif load < 0:
        urls_underloaded.append(url)

    # Sort the under-loaded URLs, with the least loaded first.
    urls_underloaded = sorted(urls_underloaded, key=lambda u: url_load[u])

    # Transfer a shard from each overloaded URL and distribute them among the
    # under-loaded URLs.
    underloaded_loop = itertools.cycle(urls_underloaded)
    for overloaded_url in urls_overloaded:
      underloaded_url = underloaded_loop.next()

      shard = self.mapping[overloaded_url].pop()
      self.mapping[underloaded_url].add(shard)

      self.last_unload_by_url[overloaded_url] = now
      LOG.info('Transferring shard %s from %s to %s' % \
          (shard, overloaded_url, underloaded_url))

class TabaAgentBufferManager(object):
  """Class that manages the event buffer and request data object queues for the
  Taba Agent. Events are added to the buffer on a (Client ID, Tab Name) basis.
  That key is used as a signature to decide which shard number the Events will
  be added to. URLs are dynamically mapped to groups of shards (through the
  UrlShardMapper object). Events are periodically removed from the buffer,
  combined for a given URL, and encoded into a series of request data objects,
  which are queued.
  """

  def __init__(self,
      urls,
      num_shards,
      url_shard_mapper,
      max_events_per_request=80000):
    """
    Args:
      urls - List of URL strings to prepare request objects for.
      num_shards - Number of shards to split the Event buffer into.
      url_shard_mapper - UrlShardMapper object which maps groups of shard
          indexes to URLs. All shards within the given range must be mapped to
          known URLs. It is understood that the mapping may change occasionally.
      max_events_per_request - Maximum number of Events to put into a single
          request. (It is treated as a soft limit. It may be exceeded
          slightly.)
    """
    self.num_shards = num_shards
    self.urls = urls
    self.mapper = url_shard_mapper
    self.max_events_per_request = max_events_per_request

    # For each shard, we maintain a multi level structure of
    # {Client ID: {Tab Name: [Encoded Events]}}
    self.shard_event_buffers = [
        defaultdict(lambda: defaultdict(list))
        for _ in xrange(self.num_shards)]

    # For each URL, we maintain a list of encoded requests ready to send.
    self.url_queues = dict([(url, []) for url in urls])

  def _CalculateShard(self, client_id, name):
    """Map a (Client ID, Tab Name) signature to a shard index (based on a hash
    of the signature).

    Args:
      client_id - Client ID string.
      name - Tab Name string.

    Returns:
      Shard index.
    """
    signature = '%s%s' % (client_id, name)
    return int(hashlib.md5(signature).hexdigest(), 16) % self.num_shards

  def AddEvents(self, client_id, name, events):
    """Add events to the event buffer for a Client ID and Tab Name.

    Args:
      client_id - Client ID string the Events are associated with.
      name - Tab Name string the Events are associated with.
      events - List of encoded Events to buffer.
    """
    shard = self._CalculateShard(client_id, name)
    self.shard_event_buffers[shard][client_id][name].extend(events)

  def GetEvents(self, shards, limit=0, client_limit=0):
    """Removes and return (up to the given limit) Events from the event buffer
    for the given shards.

    Args:
      shards - Set of shard indexes to pull from.
      limit - Maximum number of Events to pull overall. If 0, no limit
          is placed. (It is treated as a soft limit. It may be exceeded
          slightly.)
      client_limit - Maximum number of Events to pull for any particular Client
          ID. If 0, no limit is placed. (It is treated as a soft limit. It may
          be exceeded slightly.)

    Returns:
      Structure of {Client ID: {Tab Name: [Encoded Events]}} pulled from all
      the shard event buffers given, of to the limit given.
    """
    result = defaultdict(lambda: defaultdict(list))
    result_events = 0
    client_events = defaultdict(lambda: 0)

    # Start pulling event blocks into the result and out of the event buffer.
    # The shards are traversed in random order to prevent consistently biasing
    # toward certain shards.
    shards_ordered = list(shards)
    random.shuffle(list(shards_ordered))
    for shard in shards_ordered:

      # Iterate through the Client ID -> Tab Name -> Events tree.
      for client_id in self.shard_event_buffers[shard].keys():

        # If we've already exceeded the event limit for this Client ID, skip it.
        if client_limit and client_events[client_id] >= client_limit:
          continue

        name_events_map = self.shard_event_buffers[shard][client_id]
        for name in name_events_map.keys():

          # Pull the current events into the result, and remove them from the
          # event buffer.
          events = name_events_map[name]
          limit_cutoff = _GetCutoff(
              limit, client_limit, result_events, client_events[client_id],
              len(events))

          if limit_cutoff:
            events_leftover = events[limit_cutoff:]
            events = events[:limit_cutoff]

            result[client_id][name] = events
            name_events_map[name] = events_leftover

          else:
            if len(events) > 0:
              result[client_id][name] = events
            del name_events_map[name]

          result_events += len(events)
          client_events[client_id] += len(events)

          # Check if we've exceeded the soft limits, and if so, stop.
          if limit and result_events >= limit:
            break

          if client_limit and client_events[client_id] >= client_limit:
            break

        # Remove the entry for this Client ID if we've processed all its
        # outstanding Events for this shard.
        if len(self.shard_event_buffers[shard][client_id].keys()) == 0:
          del self.shard_event_buffers[shard][client_id]

        # Check if we've exceeded the soft limits, and if so, stop.
        if limit and result_events >= limit:
          return result

    return result

  def AddRequest(self, url, request_data):
    """Add a request data object directly to the request queue for a URL. This
    is typically done when performing the request failed and it should be
    retried later.

    Args:
      url - URL string to add the request data object for.
      request_data - Request data object to add.
    """
    self.url_queues[url].append(request_data)

  def IsRequestQueued(self, url):
    """Returns whether there is a request data object queued for a URL.

    Args:
      url - URL string to check if there is a queued request data object.

    Returns:
      True if there is a queued request data object.
    """
    return len(self.url_queues[url]) > 0

  def GetRequest(self, url):
    """Retrieve a request data object for the given URL. If one isn't available,
    it may be built from the event buffer.

    Args:
      url - URL string to retrieve a request data object for.

    Returns:
      Request data object with meta-data, or None if there are no Events pending
      for the given URL.
    """
    if len(self.url_queues[url]) == 0:
      # There are no request data objects ready for this URL. Try to make some
      # from Events in the event buffer.
      self.PrepareRequestData(url)

    if len(self.url_queues[url]) == 0:
      # There are still no request data objects ready for this URL, which means
      # there are no Events in the buffer waiting to flush.
      request_data = None

    else:
      # There's a request data object on the queue for this URL, so return it
      # directly.
      request_data = self.url_queues[url][0]
      self.url_queues[url] = self.url_queues[url][1:]

    return request_data

  def PrepareRequestData(self, url):
    """Pull Events from the event buffer and prepare encoded request data
    objects for the URL. Uses the current Shard -> URL mapping provided by the
    mapper. The resulting request data objects are placed on the request queue
    for this URL. Note: this method yields.

    Args:
      url - URL string to pull Events from the buffer for.
    """
    # Pull some event data from the event buffer for the set of shards currently
    # mapped to the given URL.
    event_data = self.GetEvents(
        shards=self.mapper.GetShards(url),
        client_limit=self.max_events_per_request)

    for client_id, name_events_map in event_data.iteritems():

      # Calculate the number of Events in this block.
      size = sum(len(events) for events in name_events_map.values())
      if size == 0:
        continue

      # Encode the Event data into a transmitable string.
      body = transport.Encode(
        client_id,
        name_events_map,
        encode_events=False)

      body = zlib.compress(body)

      # Combine the encoded data with some meta-data, and place it on the queue.
      request_data = {
          'body': body,
          'size': size,
          'tries': 0}

      self.url_queues[url].append(request_data)

      # This can be a lengthy operation, so be nice and yield between blocks.
      gevent.sleep(0)

  def GetStats(self):
    """Get the current key buffer metrics.

    Returns:
      BufferStats object.
    """
    buffered_events_by_shard = [
        sum([
            len(events)
            for name_events_map in self.shard_event_buffers[shard].values()
            for events in name_events_map.values()])
        for shard in xrange(self.num_shards)]

    queued_events_by_url = dict([
        (url, sum([request_data['size'] for request_data in request_datas]))
        for url, request_datas in self.url_queues.iteritems()])

    queued_requests_by_url = dict([
        (url, len(request_datas))
        for url, request_datas in self.url_queues.iteritems()])

    return TabaAgentBufferManager.BufferStats(
        buffered_events_by_shard,
        queued_events_by_url,
        queued_requests_by_url)

  class BufferStats(object):
    def __init__(self,
        buffered_events_by_shard,
        queued_events_by_url,
        queued_requests_by_url):
      self.buffered_events_by_shard = buffered_events_by_shard
      self.queued_events_by_url = queued_events_by_url
      self.queued_requests_by_url = queued_requests_by_url

      self.buffered_events = sum(buffered_events_by_shard)
      self.queued_events = sum(queued_events_by_url.values())
      self.queued_requests = sum(queued_requests_by_url.values())

      self.total_events = self.buffered_events + self.queued_events

def _GetCutoff(total_limit, client_limit, total_cur, client_cur, num_events):
  """Calculate the cut-off point for a number of events given a possible total
  and per-client limit.

  Args:
    total_limit - Total request event limit, or 0 if unlimited.
    client_limit - Per-Client event limit, or 0 if unlimited.
    total_cur - Current total number of events in buffer.
    client_cur - Current per-Client number of events in buffer.
    num_events - Number of events being considered to add to buffer.

  Returns:
    Maximum number of new events that can be added to buffer without exceeding
    either limit. None if the number of events to add will remain under the
    limits.
  """
  if not total_limit and not client_limit:
    return None

  total_cutoff = None
  if total_limit and num_events + total_cur > total_limit:
    total_cutoff = total_limit - total_cur

  client_cutoff = None
  if client_limit and num_events + client_cur > client_limit:
    client_cutoff = client_limit - client_cur

  if total_cutoff or client_cutoff:
    return min(filter(None, [total_cutoff, client_cutoff]))
  else:
    return None
