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
Module to launch the Taba Server and register its HTTP handlers.
"""

def StartTabaServer(
    primary_port,
    secondary_port,
    db_endpoints,
    db_vbuckets,
    server_endpoints,
    pruning_enabled=False,
    use_memory_engine=False):
  """Initialize the Taba Server, and setup Juno request handlers for the Taba
  Client and monitoring interface.

  Args:
    primary_port - Port to listen on for primary requests.
    secondary_port - Port to listen on for secondary requests.
    db_endpoints - List of database end-point information dictionaries. Each
        entry should contain 'host', 'port', 'vbuckets' (where 'vbuckets' is
        a 2 element list specifying the start and end vbucket for that
        end-point).
    db_vbuckets - Total number of vbuckets in the database.
    server_endpoints - List of Taba Server end-points information dictionaries.
        Each entry should contain 'host' and 'port' fields.
    pruning_enabled - If True, enable the periodic pruning service for this
        process.
    use_memory_engine - If True, use an in-memory database instead of the usual
        Redis one. Useful for testing.
  """
  # Monkey patch gevent before doing anything.
  from gevent import monkey
  monkey.patch_all()

  # Attempt to turn heapy remote monitoring on.
  try:
    import guppy.heapy.RM   #@UnusedImport
  except ImportError:
    pass

  # Attempt to attach a stack-trace dump to the SIGQUIT (kill -3) signal.
  try:
    import signal
    signal.signal(signal.SIGQUIT, _PrintTrace)
  except ImportError:
    pass

  # Compile the Cython components.
  from tellapart.taba.util.misc_util import BootstrapCython
  BootstrapCython(primary_port)

  from tellapart.frontend import juno_patches
  from tellapart.third_party import juno

  from tellapart.storage.engine import redis_engine
  from tellapart.taba import taba_client
  from tellapart.taba.server import taba_server
  from tellapart.taba.server import taba_server_storage_manager
  from tellapart.taba.server import taba_server_handlers
  from tellapart.taba.util.misc_util import HandlerSpec

  # Initialize the Taba Server, storage manager, and Redis engine.
  if use_memory_engine:
    from tellapart.storage.engine import memory_redis_engine
    engine = memory_redis_engine.MemoryRedisEngine(None, None)
    server_endpoints = None

    taba_url = 'http://localhost:%d/post' % secondary_port

  else:
    redis_endpoints = []
    for endpoint in db_endpoints:
      redis_endpoints.append(
          redis_engine.RedisServerEndpoint(
              host=endpoint['host'],
              port=endpoint['port'],
              vbucket_start=endpoint['vbuckets'][0],
              vbucket_end=endpoint['vbuckets'][1]))

    engine = redis_engine.RedisEngine(redis_endpoints, db_vbuckets)

    taba_url = 'http://localhost:%d/post' % server_endpoints[0]['port']

  dao = taba_server_storage_manager.TabaServerStorageManager(engine)
  taba_server_handlers.global_taba_server = \
      taba_server.TabaServer(dao, server_endpoints, prune=pruning_enabled)

  # Setup the local Taba Client.
  taba_client.Initialize('taba_server', taba_url, 60)

  # Setup all necessary handlers.
  juno_patches.InitializeJuno('wsgi')

  _handlers = [
      # Event posting handlers.
      HandlerSpec(
          '/post_zip',
          taba_server_handlers.HandlePostCompressed,
          methods=('POST',)),
      HandlerSpec(
          '/post',
          taba_server_handlers.HandlePostDirect,
          methods=('POST',)),

      # Single Get handlers.
      HandlerSpec('/raw', taba_server_handlers.HandleGetRaw),
      HandlerSpec('/projection', taba_server_handlers.HandleGetProjection),
      HandlerSpec('/aggregate', taba_server_handlers.HandleGetAggregate),
      HandlerSpec('/taba', taba_server_handlers.HandleGetTaba),

      # Batch Get handlers.
      HandlerSpec(
          '/raw_batch',
          taba_server_handlers.HandleGetRawBatch,
          methods=('POST',)),
      HandlerSpec(
          '/projection_batch',
          taba_server_handlers.HandleGetProjectionBatch,
          methods=('POST',)),
      HandlerSpec(
          '/aggregate_batch',
          taba_server_handlers.HandleGetAggretateBatch,
          methods=('POST',)),

      # Meta-data handlers.
      HandlerSpec('/clients', taba_server_handlers.HandleGetClients),
      HandlerSpec('/names', taba_server_handlers.HandleGetTabaNames),
      HandlerSpec('/type', taba_server_handlers.HandleGetType),

      # Administrative handlers.
      HandlerSpec('/delete', taba_server_handlers.HandleDeleteName),
      HandlerSpec('/prune', taba_server_handlers.HandlePrune),
      HandlerSpec('/upgrade', taba_server_handlers.HandleUpgrade),
      HandlerSpec('/status', taba_server_handlers.HandleStatus),
  ]

  for h in _handlers:
    for method in h.methods:
      juno.route(h.route_url, method)(h.handler_func)

  # Move 'wsgi.input' to 'request_body_bytes' so that we can read it directly
  # instead of having Juno parse it into a dictionary.
  juno.config(
      'middleware',
      [('tellapart.taba.util.middleware.CircumventJunoInputParsing', {}),])

  # Create and start the Primary and Secondary Server objects.
  from tellapart.taba.util import wsgi_server
  application = juno.run()

  primary_server = wsgi_server.launch_gevent_wsgi_server(
      application,
      port=primary_port,
      max_concurrent_requests=8,
      server_name='Taba Server - Primary',
      should_run_forever=False,
      use_clean_shutdown=True)

  secondary_server = wsgi_server.launch_gevent_wsgi_server(
      application,
      port=secondary_port,
      max_concurrent_requests=8,
      server_name='Taba Server - Secondary',
      should_run_forever=False,
      use_clean_shutdown=True)

  primary_server.start()
  secondary_server.start()

  primary_server._stopped_event.wait()
  secondary_server._stopped_event.wait()

def _PrintTrace(sig, frame):
  """Called from a signal handler, prints to the stack trace of the calling
  frame to stderr

  Arguments:
    sig - The signal number being handled.
    frame - The current execution Frame object.
  """
  import sys
  import traceback

  stack = ''.join(traceback.format_stack(frame))
  print sys.stderr, "Signal received. Traceback:\n" + stack
