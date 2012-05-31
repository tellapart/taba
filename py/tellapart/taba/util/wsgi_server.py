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

Utilities for launching gevent WSGI servers.
"""

def launch_gevent_wsgi_server(
    application,
    port,
    max_concurrent_requests,
    server_name='server',
    shutdown_cleanup_fn=None,
    should_run_forever=True,
    use_clean_shutdown=False,
    **kwargs):
  """Set up and launch a gevent WSGI server in the local process.

  NOTE: gevent monkey patching should occur prior to calling this method.

  Args:
    application - A callable that accepts two arguments, per PEP-333 WSGI spec.
    port - Port that the server should run on (integer).
    max_concurrent_requests - The maximum number of concurrent requests.
    server_name - Optional server name to print to logs.
    shutdown_cleanup_fn - If specified, a function to call after the wsgi_server
        is stopped to cleanup resources (e.g. connections.)
    should_run_forever - If True, server is kicked off to run forever, if False,
        the server object is returned, not running.
    use_clean_shutdown - If True, and not using pywgsi, use a subclass of
        gevent.WSGIServer which waits for requests to finish before terminating
        the listening socket.
    **kwargs - Additional keyword args are passed to the WSGIServer constructor.
  """
  import signal
  import gevent
  from gevent import pool

  server_class = _GetServerClass(use_clean_shutdown)

  wsgi_server = None
  def _shut_down_wsgi_server():
    """Gracefully terminate the WSGI server when receiving a SIGTERM.
    """
    # Ignore any future SIGINTs. This will make our exponential backoff killer
    # not kill the process on a second loop. Only a SIGKILL or SIGTERM will
    # kill this process now.
    # NOTE: This does not call gevent.signal as there is no support built in for
    # SIG_IGN. Call the signal module directly for this functionality!
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    print 'Stopping %s %s' % (server_class.__module__, server_name)

    if shutdown_cleanup_fn:
      shutdown_cleanup_fn()

    if wsgi_server:
      wsgi_server.stop()

  gevent.signal(signal.SIGTERM, _shut_down_wsgi_server)
  gevent.signal(signal.SIGINT, _shut_down_wsgi_server)

  print 'Starting %s %s' % (server_class.__module__, server_name)

  try:
    greenlet_pool = pool.Pool(max_concurrent_requests)
    wsgi_server = server_class(
        ('', port),
        application,
        spawn=greenlet_pool,
        log=None,
        **kwargs)

    if should_run_forever:
      wsgi_server.serve_forever()
    else:
      return wsgi_server

  except KeyboardInterrupt:
    _shut_down_wsgi_server()

def _GetServerClass(use_clean_shutdown):
  """Determine the server class to use.

  Args:
    use_clean_shutdown - If True, and not using pywgsi, use a subclass of
        gevent.WSGIServer which waits for requests to finish before terminating
        the listening socket.

  Returns:
    A server class.
  """
  from gevent import wsgi

  if use_clean_shutdown:
    class CleanShutdownWsgiServer(wsgi.WSGIServer):
      stop_timeout = 30

      def stop(self, timeout=None):
        """Stop accepting the connections and close the listening socket.

        Overrides the default behavior by first closing the greenlet pool, then
        waiting for the running greenlets to complete _before_ closing the
        socket. That way, the running requests can send responses. Also
        increases the default join timeout to 30 seconds (from 1).
        """
        if timeout is None:
          timeout = self.stop_timeout

        if self.pool:
          self.pool.size = 0
          self.pool.join(timeout=timeout)
          self.pool.kill(block=True, timeout=1)

        self.kill()
        self.post_stop()

    return CleanShutdownWsgiServer

  else:
    return wsgi.WSGIServer
