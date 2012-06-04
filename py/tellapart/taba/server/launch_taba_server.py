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
Taba Server launcher.
"""

from multiprocessing import Process
import os
import sys

from tellapart.taba.server import taba_server_main
from tellapart.taba.util import misc_util

def main():
  """Launch the Taba Server worker processes from a settings file.
  """
  # Get the path to the settings file.
  if len(sys.argv) == 1:
    settings_path = '/etc/tellapart/taba/taba_server_settings.json'
    print 'Using default settings path %s' % settings_path

  elif len(sys.argv) == 2:
    settings_path = sys.argv[1]

  else:
    _Usage('Invalid arguments')

  if not os.path.exists(settings_path):
    _Usage('Settings file does not exist')

  # Decode the settings.
  settings = misc_util.DecodeSettingsFile(settings_path)

  # Launch the Taba Server processes.
  _LaunchProcesses(settings)

def _Usage(message=None):
  """Print a message, the general usage, and exit.
  """
  if message:
    print message

  print 'Usage: taba_agent_main.py <settings file path>'
  sys.exit(1)

def _LaunchProcesses(settings):
  """Launch the Taba Server worker processes.

  Args:
    settings - Taba Server settings dictionary.
  """
  procs = []

  try:
    # Spawn TABA_SERVER_WORKERS child processes to handle requests.
    for port_offset in xrange(settings['TABA_SERVER_WORKERS']):
      primary_port = settings['PRIMARY_BASE_PORT'] + port_offset
      secondary_port = settings['SECONDARY_BASE_PORT'] + port_offset

      p = Process(
          target=taba_server_main.StartTabaServer,
          args=(
              primary_port,
              secondary_port,
              settings['TABA_DB_ENDPOINTS'],
              settings['TABA_DB_VBUCKETS'],
              settings['TABA_SERVER_ENDPOINTS'],
              (port_offset == 0),
              settings['TABA_USE_MEMORY_DB']))

      p.start()
      procs.append(p)

    # Wait for the child Server processes to finish.  While the child processes
    # are serving requests, the main process will block here.
    for p in procs:
      p.join()

  except KeyboardInterrupt:
    pass

  print 'Terminating %d server processes' % len(procs)
  for p in procs:
    p.terminate()

  print 'Waiting for %d server processes to die' % len(procs)
  for p in procs:
    p.join()

if __name__ == '__main__':
  main()
