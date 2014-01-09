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

"""Dict-like class that applies a Time-to-Live to all keys, acting as a local
cache. Timeouts are done on a 'heart-beat' basis, which fires at a fixed
interval. This is done to avoid calling time.time() on every access, which is
a potentially expensive system call. This allows the cache to be used for high
volume access without adversely affecting performance, at the expense of
timeout resolution.
"""

import heapq
import logging
import time

from taba.util import thread_util

LOG = logging.getLogger(__name__)

# Default number of seconds between heart-beats.
DEFAULT_HEARTBEAT_WIDTH = 10

class TtlHeartbeatCache(object):
  """Dict-like class that applies a Time-to-Live to all keys, acting as a local
  cache. Timeouts are done on a 'heart-beat' basis, which fires at a fixed
  interval. This is done to avoid calling time.time() on every access, which is
  a potentially expensive system call. This allows the cache to be used for high
  volume access without adversely affecting performance.

  NOTE: Items are only expired on retrieval. Do not rely on expiration to remove
  stale items (e.g. for memory management).
  """

  def __init__(self, ttl, heartbeat_width=DEFAULT_HEARTBEAT_WIDTH):
    """
    Args:
      ttl - Number of seconds for which values will be retained. If None, values
          will never be expired.
      heartbeat_width - Number of seconds between 'heart-beats'. This is
          effectively the resolution of the cache.
    """
    self.ttl = ttl

    self.data = {}
    self.expiry = []

    if self.ttl is not None:
      self.heartbeat_time = time.time()
      thread_util.ScheduleOperationWithPeriod(
          heartbeat_width,
          self._DoHeartbeat)

  def _DoHeartbeat(self):
    """Update the heart-beat time."""
    self.heartbeat_time = time.time()
    self._FlushExpired()

  def _FlushExpired(self):
    """Remove any expired keys from the dict."""
    if len(self.expiry) == 0:
      return

    created, key = self.expiry[0]
    while created + self.ttl <= self.heartbeat_time:
      if key in self.data:
        del self.data[key]

      heapq.heappop(self.expiry)
      if len(self.expiry) == 0:
        break

      created, key = self.expiry[0]

  def __setitem__(self, key, value):
    """Add an item to the dict."""
    self.data[key] = value
    if self.ttl is not None:
      heapq.heappush(self.expiry, (self.heartbeat_time, key))

  def __getitem__(self, key):
    """Retrieve an item from the dict. Checks whether the item has expired."""
    return self.data.get(key, None)

  def __delitem__(self, key):
    """Delete an item from the dict."""
    if key in self.data:
      self.data.__delitem__(key)

  def __contains__(self, key):
    """Test if an item is in the dict. Forces expiration is applicable."""
    return self.data.__contains__(key)

  def __len__(self):
    """Get the size of the dict."""
    return self.data.__len__()
