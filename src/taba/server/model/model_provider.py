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

"""Model factory for the Taba Server.
"""

import gevent

_engine = None
_clients = None
_names = None
_states = None
_identity = None
_queues = None
_latency = None
_frontend = None
_backend = None

def Initialize(
    server_name,
    db_endpoints,
    db_vbuckets,
    roles='fe',
    use_memory_engine=False,
    use_debug=False,
    name_blacklist=None,
    client_blacklist=None,
    additional_handlers=None):
  """Initialize the Taba Server storage and controller stack.

  Args:
    server_name - Unique name for this server process.
    db_endpoints - List of database end-point information dictionaries. Each
        entry should contain 'host', 'port', 'vbuckets' (where 'vbuckets' is
        a 2 element list specifying the start and end vbucket for that
        end-point).
    db_vbuckets - Total number of vbuckets in the database.
    roles - Role specification string (i.e. comma separated ServerRoles values).
    use_memory_engine - If True, use an in-process memory storage engine. This
        does _NOT_ support persistence!
    use_debug - Whether to run the server in debug mode.
    name_blacklist - List of regular expressions which will be used to ignore
        any matching Tab Names. If None, no Name blacklisting will be
        performed.
    client_blacklist - List of regular expressions which will be used to ignore
        any matching Client Names. If None, no Client blacklisting will be
        performed.
    additional_handlers - Map from TabType string to fully qualified class
        names, used to augment or override the default Handler mapping.
  """
  from taba.server.storage import memory_engine
  from taba.server.storage import redis_engine
  from taba.server import backend
  from taba.server import frontend
  from taba.server.model import client_storage
  from taba.server.model import identity_registry
  from taba.server.model import latency_tracker
  from taba.server.model import name_storage
  from taba.server.model import queue_registry
  from taba.server.model import state_manager
  from taba.server.model import tab_registry

  global _engine
  global _clients
  global _names
  global _states
  global _identity
  global _queues
  global _latency
  global _frontend
  global _backend

  global debug
  debug = use_debug

  # Initialize the Handler registry.
  tab_registry.InitializeRegistry(additional_handlers)

  # Initialize the bottom-level storage manager.
  if use_memory_engine:
    _engine = memory_engine.MemoryRedisEngine()

  else:
    redis_endpoints = []
    for endpoint in db_endpoints:
      redis_endpoints.append(
          redis_engine.RedisServerEndpoint(
              host=endpoint['host'],
              port=endpoint['port'],
              vbucket_start=endpoint['vbuckets'][0],
              vbucket_end=endpoint['vbuckets'][1]))

    _engine = redis_engine.RedisEngine(
        redis_endpoints, db_vbuckets, timeout=60.0,
        pool_tab_prefix='taba_server')

  # Parse some additional settings.
  roles = identity_registry.ServerRoles.ParseRoles(roles)
  init_backend = bool(identity_registry.ServerRoles.BACKEND in roles)

  # Initialize the identity, shard queue, and latency managers.
  _identity = identity_registry.IdentityRegistry(server_name, _engine, roles)
  _queues = queue_registry.QueueRegistry(_engine)
  _latency = latency_tracker.LatencyTracker(_engine)

  # Initialize the rest of the storage/model stack.
  _clients = client_storage.ClientStorageManager(_engine)
  _names = name_storage.NameStorageManager(_engine)
  _states = state_manager.StateManager(_engine)

  # Initialize the Front-end and Back-end controllers.
  _frontend = frontend.TabaServerFrontend(name_blacklist, client_blacklist)
  _backend = backend.TabaServerBackend(_engine, initialize=init_backend)

  # Force shard assignments to put this process in the pool.
  gevent.spawn(_queues.ForceRecomputeShards)

def ShutDown():
  """Cleanly shut down the Taba Server stack.
  """
  _backend.ShutDown()
  _queues.ShutDown()
  _identity.ShutDown()
  _engine.ShutDown()

def GetEngine():
  return _engine

def GetClients():
  return _clients

def GetNames():
  return _names

def GetStates():
  return _states

def GetIdentity():
  return _identity

def GetQueues():
  return _queues

def GetLatency():
  return _latency

def GetFrontend():
  return _frontend

def GetBackend():
  return _backend

debug = True
