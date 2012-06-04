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
Module for launching the Taba Agent service.
"""

import os
import sys

from gevent import monkey

from tellapart.frontend import juno_patches
from tellapart.taba.agent.taba_agent_handlers import global_taba_agent
from tellapart.taba.agent.taba_agent_handlers import HandlePost
from tellapart.taba.agent.taba_agent_handlers import HandleStatus
from tellapart.taba.util import misc_util
from tellapart.third_party import juno

LOG = misc_util.MakeStreamLogger(__name__)

def main():
  """Launch the Taba Agent from a settings file.
  """
  # Get the path to the settings file.
  if len(sys.argv) == 1:
    settings_path = '/etc/tellapart/taba/taba_agent_settings.json'
    print 'Using default settings path %s' % settings_path

  elif len(sys.argv) == 2:
    settings_path = sys.argv[1]

  else:
    _Usage('Invalid arguments')

  if not os.path.exists(settings_path):
    _Usage('Settings file does not exist')

  # Decode the settings.
  settings = misc_util.DecodeSettingsFile(settings_path)

  # Launch the Taba Agent.
  StartTabaAgentServer(
      settings['TABA_SERVER_ENDPOINTS'],
      settings['TABA_AGENT_FLUSH_SECONDS'],
      settings['TABA_AGENT_PORT'])

def _Usage(message=None):
  """Print a message, the general usage, and exit.
  """
  if message:
    print message

  print 'Usage: taba_agent_main.py <settings file path>'
  sys.exit(1)

def StartTabaAgentServer(server_endpoints, flush_period_seconds, port):
  """Initialize the Taba Agent, and setup Juno request handlers for the Taba
  Client and monitoring interface.

  Args:
    server_endpoints - List of Taba Server end-point dictionaries, containing
        'host' and 'port' fields for each end-point.
    flush_period_seconds - The period, in seconds, at which the Taba Agent
        should flush its buffer to the Server.
    port - Port the Taba Agent to listen to for receiving Events from Clients.
  """
  # Monkey patch gevent before doing anything.
  monkey.patch_all()

  # Initialize the Taba Agent
  global_taba_agent.Initialize(server_endpoints, flush_period_seconds)

  # Setup all necessary handlers.
  juno_patches.InitializeJuno('wsgi')

  _handlers = [
      misc_util.HandlerSpec('/post', HandlePost, methods=('post',)),
      misc_util.HandlerSpec('/status', HandleStatus),
  ]

  for h in _handlers:
    for method in h.methods:
      juno.route(h.route_url, method)(h.handler_func)

  # Move 'wsgi.input' to 'request_body_bytes' so that we can read it directly
  # instead of having Juno parse it into a dictionary.
  juno.config(
      'middleware',
      [('tellapart.taba.util.middleware.CircumventJunoInputParsing', {}),])

  application = juno.run()
  server_name = 'Taba Agent'

  from tellapart.taba.util import wsgi_server
  wsgi_server.launch_gevent_wsgi_server(application, port, 16, server_name)

if __name__ == '__main__':
  main()
