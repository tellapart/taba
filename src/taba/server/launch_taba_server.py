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

"""Taba Server launcher.
"""

import multiprocessing
import os
import signal
import socket
import subprocess
import sys
import time
import traceback

###########################################################
# CLI / System
###########################################################

def main():
  """Launch the Taba Server worker processes from a settings file.
  """
  # Get the path to the settings file.
  if len(sys.argv) == 1:
    settings_path = '/etc/taba/server_settings.json'
    print 'Using default settings path %s' % settings_path

  elif len(sys.argv) == 2:
    settings_path = sys.argv[1]

  else:
    _Usage('Invalid arguments')

  if not os.path.exists(settings_path):
    _Usage('Settings file does not exist')

  # Decode the settings.
  from taba.util import misc_util
  settings = misc_util.DecodeSettingsFile(settings_path)

  # Launch the Taba Server processes.
  _LaunchProcesses(settings)

def _Usage(message=None):
  """Print a message, the general usage, and exit.
  """
  if message:
    print message

  print 'Usage: launch_taba_server.py <settings file path>'
  sys.exit(1)

###########################################################
# Launcher
###########################################################

class ProcessInfo(object):
  """Container class for information about a worker process.
  """

  def __init__(self, process, arguments, port_offset):
    """
    Args:
      process - Process object. May be None.
      arguments - Tuple of arguments passed to the launch function.
      port_offset - Offset from the base port for the process.
    """
    self.process = process
    self.arguments = arguments
    self.port_offset = port_offset

def _LaunchProcesses(settings):
  """Launch the Taba Server worker processes.

  Args:
    settings - Taba Server settings dictionary.
  """
  process_infos = []

  try:
    # Spawn NUM_SERVER_PROCESSES child processes to handle requests.
    for port_offset in xrange(settings['SERVER_WORKERS']):
      port = settings['BASE_PORT'] + port_offset

      args = (
          port,
          settings['DB_SETTINGS'],
          settings['SERVER_ROLES'],
          'http://localhost:%d/post' % settings['AGENT_PORT'],
          True,
          settings.get('DEBUG', False),
          settings.get('NAME_BLACKLIST', None),
          settings.get('CLIENT_BLACKLIST', None),
          settings.get('ADDITIONAL_HANDLERS', None))

      process_info = ProcessInfo(None, args, port_offset)
      process_infos.append(process_info)

      _LaunchSingleProcess(process_info)

    # Monitor the worker processes in a loop, until a KeyboardInterrupt is
    # received. If one of the worker processes falls over, start a new one to
    # replace it.
    while True:

      for process_info in process_infos:
        if not process_info.process.is_alive():
          _LaunchSingleProcess(process_info)

      # Don't wait in a tight loop.
      time.sleep(5)

  except KeyboardInterrupt:
    pass

  print 'Terminating %d server processes' % len(process_infos)
  for info in process_infos:
    info.process.terminate()

  print 'Waiting for %d server processes to die' % len(process_infos)
  for info in process_infos:
    info.process.join()

def _LaunchSingleProcess(process_info):
  """Launch a single worker process. The newly created Process object is added
  to the given ProcessInfo object in-place.

  Args:
    process_info - ProcessInfo object used to launch the worker process. The
        newly launched Process object is added to the ProcesSInfo in-place.
  """
  process = multiprocessing.Process(
      target=_StartTabaServer,
      args=process_info.arguments)
  process.start()

  _SetProcessorAffinity(process, process_info.port_offset)

  process_info.process = process

def _StartTabaServer(
    port,
    db_settings,
    roles,
    agent_url=None,
    priority=False,
    debug=False,
    name_blacklist=None,
    client_blacklist=None,
    additional_handlers=None):
  """Initialize the Taba Server worker process.

  Args:
    port - Port to listen on for requests.
    db_settings -
    roles - Comma delimited list of ServerRoles.
    agent_url - Agent URL to use for posting internally generated Events.
    priority - If True, bump up this process' priority.
    debug - Whether to launch in debug mode.
    name_blacklist - List of regular expressions that will be used to ignore
        any matching Names. If None, no filtering will be applied.
    client_blacklist - List of regular expressions that will be used to ignore
        any matching Client Names. If None, no filtering will be applied.
  """
  # Monkey patch gevent before doing anything.
  from gevent import monkey
  monkey.patch_all()

  # If this process is flagged as priority, adjust the nice value.
  if priority:
    os.nice(-2)

  # Attempt to turn heapy remote monitoring on.
  try:
    # noinspection PyUnresolvedReferences
    import guppy.heapy.RM
  except ImportError:
    pass

  # Attach a stack-trace dump to the SIGQUIT (kill -3) signal.
  signal.signal(signal.SIGQUIT, _PrintTrace)

  # Compile the Cython components.
  from taba.util.misc_util import BootstrapCython
  BootstrapCython(port)

  import logging
  logging.basicConfig(level=logging.WARNING)

  server_name = 'taba_server.%s.%d' % (socket.gethostname(), port)

  # Initialize the server.
  from taba.server.model import model_provider
  model_provider.Initialize(
      server_name,
      db_settings,
      roles,
      debug,
      name_blacklist,
      client_blacklist,
      additional_handlers)

  # Setup the local Taba Client.
  if agent_url:
    from taba import client
    client_name = 'taba_server.%s' % socket.gethostname()
    client.Initialize(client_name, agent_url, flush_period=10)

  # Start the server.
  from taba.third_party import bottle

  app = bottle.app()
  bottle.run(app=app, host='0.0.0.0', port=port, server='gevent')

###########################################################
# Misc.
###########################################################

def _SetProcessorAffinity(process, process_index):
  """Sets the processor affinity for a process.

  Args:
    process - A multiprocessing.Process object.
    process_index - The number of the order of the process.
  """
  num_cpus = multiprocessing.cpu_count()

  try:
    # Pick a CPU affinity based on the process_index, using round-robin. Each
    # process is assigned to a primary and secondary CPU, sequential in number
    # (e.g. '2,3'). However, the process whose primary is the highest CPU number
    # does not have a secondary, in order to reduce the load on CPU 0 (since the
    # kernel runs primarily on CPU 0).
    affinity = num_cpus - (process_index % num_cpus) - 1
    if affinity == 0:
      affinity_str = str(affinity)
    else:
      affinity_str = '%s,%s' % (affinity, affinity - 1)

    # Execute the command to set the CPU affinity.
    process = subprocess.Popen(
        ['taskset', '-cp', affinity_str, str(process.pid)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    _, err = process.communicate()

    if process.returncode != 0:
      err = '%s => %s = %s' % (process.pid, affinity_str, err)

  except Exception, e:
    err = str(e)

  if err:
    print "Failed to set processor affinity: ", err.strip()

def _PrintTrace(sig, frame):
  """Called from a signal handler, prints to the stack trace of the calling
  frame to stderr

  Arguments:
    sig - The signal number being handled.
    frame - The current execution Frame object.
  """
  stack = ''.join(traceback.format_stack(frame))
  print sys.stderr, "Signal received. Traceback:\n" + stack

if __name__ == '__main__':
  main()
