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
Taba Client and Server, embedded within the same process. Exposes a
TabaClient interface which posts Events directly to an embedded, in-memory
TabaServer.
"""

import time

import cjson

from tellapart.storage.engine.memory_redis_engine import MemoryRedisEngine
from tellapart.taba import taba_client
from tellapart.taba.server import taba_server

class EmbeddedTabaClientServer(object):
  """TabaClient-like object that wraps an in-memory Taba Server, and posts
  Events directly to it.
  """

  def __init__(self):
    self.client_id = 'embedded'

  def _Flush(self):
    pass

  def Initialize(self):
    """Initialize this Taba Client by configuring the embedded Server.
    """
    self.server = taba_server.TabaServer(
        taba_server_storage_manager=MemoryRedisEngine(None, None),
        endpoints=None)

  def RecordEvent(self, name, type, value):
    """Create and post a Taba Event.

    Args:
      name - Taba Name to post the Event to.
      type - Taba Type of the Taba Name.
      value - The value being recorded.
    """
    now = time.time()
    self.server.ReceiveEvents(
        client_id=self.client_id,
        events=[(name, type, cjson.encode(value), now)])

  def State(self):
    """Return a JSON encoded dictionary of the Taba Client state. The returned
    value has the fields:
        failures - The number of flush events that failed.
        buffer_size - The number of Taba Events currently in the buffer.
    """
    state = {
        'failures' : 0,
        'buffer_size' : 0}
    return cjson.encode(state)

def InitializeEmbeddedServer():
  """Initializes the local Taba Client to post events directly to a Taba Server
  object running in the same process.

  NOTE: These Events will not get persisted anywhere! This is intended for
  scripts and utilities that only want to use Tabs to track and retrieve state
  only over the life of the process.
  """
  taba_client.client = EmbeddedTabaClientServer()
  taba_client.client.Initialize()

def GetTabaServer():
  """Retrieve the TabaServer object from the embedded Client/Server.
  """
  client = taba_client.client
  return client.server
