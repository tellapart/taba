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

"""Module for launching the Taba Agent service.
"""

import logging
import os
import sys

from gevent import monkey
monkey.patch_all()

from taba.agent.handlers import global_taba_agent
from taba.third_party import bottle
from taba.util import misc_util

LOG = logging.getLogger(__name__)

def main():
  """Launch the Taba Agent from a settings file.
  """
  # Get the path to the settings file.
  if len(sys.argv) == 1:
    settings_path = '/etc/taba/agent_settings.json'
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
      settings['SERVER_ENDPOINTS'],
      settings['AGENT_FLUSH_SECONDS'],
      settings['AGENT_PORT'],
      settings.get('AGENT_QUEUES_PER_SERVER', 1),
      settings.get('DOUBLE_AGENT', None))

def _Usage(message=None):
  """Print a message, the general usage, and exit.
  """
  if message:
    print message

  print 'Usage: main.py <settings file path>'
  sys.exit(1)

def StartTabaAgentServer(
    server_endpoints,
    flush_period_seconds,
    port,
    queues_per_url,
    double_agent_conf=None):
  """Initialize the Taba Agent, and setup the HTTP request handlers for the
  Client and monitoring interface.

  Args:
    server_endpoints - List of Taba Server end-point dictionaries, containing
        'host' and 'port' fields for each end-point.
    flush_period_seconds - The period, in seconds, at which the Taba Agent
        should flush its buffer.
    port - Port the Taba Agent to listen to for receiving Events.
    queues_per_url - Number of parallel queues to use per URL end-point.
    double_agent_conf - If not None, represents an alternative Taba Agent
      configuration which will be spawned and forwarded a copy of all incoming
      requests.
  """
  # Compile the Cython components.
  from taba.util.misc_util import BootstrapCython
  BootstrapCython(port)

  # Grab the bottle Application object for possibly wrapping in middleware.
  app = bottle.app()

  # If running in 'Double Agent' mode, inject the forwarding middleware and
  # spin up the sub-process.
  subproc = None
  if double_agent_conf:
    from taba.util.middleware import DoubleAgent
    app = DoubleAgent(app, double_agent_conf['AGENT_PORT'])

    from multiprocessing import Process
    subproc = Process(
        target=StartTabaAgentServer,
        args=(
            double_agent_conf['SERVER_ENDPOINTS'],
            double_agent_conf['AGENT_FLUSH_SECONDS'],
            double_agent_conf['AGENT_PORT'],
            double_agent_conf.get('AGENT_QUEUES_PER_SERVER', 1),
            None))
    subproc.start()

  logging.basicConfig(level=logging.WARNING)

  # Initialize the Taba Agent
  global_taba_agent.Initialize(
      server_endpoints,
      flush_period_seconds,
      queues_per_url=queues_per_url)

  try:
    bottle.run(app=app, host='0.0.0.0', port=port, server='gevent')

  finally:
    if subproc:
      subproc.terminate()
      subproc.join()

if __name__ == '__main__':
  main()
