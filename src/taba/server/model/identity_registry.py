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

"""Class for managing the registration of each server process with the database.
Each process in the Taba cluster registers itself, along with a list of Roles.
"""

import logging
import time

from taba.util import thread_util
from taba.server.storage import util

# Number of seconds between registration refreshes.
DEFAULT_PING_FREQUENCY_SEC = 15

# Number of seconds between passes to unregister expired servers.
DEFAULT_FLUSH_FREQUENCY_SEC = 60

# Timeout in seconds after which a server is considered dead.
DEFAULT_TIMEOUT_SEC = DEFAULT_PING_FREQUENCY_SEC * 3.1

# Database Hash key for the mapping.
KEY_IDENTITY_MAP = 'AllServers'

LOG = logging.getLogger(__name__)

class ServerRoles(object):
  """Enumeration of the available server roles.
  """
  FRONTEND = 'fe'
  BACKEND = 'be'
  CONTROLLER = 'ctrl'

  @staticmethod
  def ParseRoles(roles_str):
    """Parse a role specification string into a list of ServerRoles values.

    Args:
      roles_str - Role specification string (comma separated).

    Returns:
      List of ServerRoles values.
    """
    roles = set()
    for role_str in roles_str.split(','):
      for v in ServerRoles.__dict__.itervalues():
        if isinstance(v, basestring) and v == role_str:
          roles.add(role_str)
          break
      else:
        raise ValueError('Unknown role "%s"' % role_str)

      return roles

class IdentityRegistry(object):
  """Class for managing the registration of each server process with the
  database. Each process in the cluster registers itself, along with a list of
  Roles.
  """

  def __init__(self,
      server_name,
      engine,
      roles,
      ping_frequency=DEFAULT_PING_FREQUENCY_SEC,
      flush_frequency=DEFAULT_FLUSH_FREQUENCY_SEC,
      timeout=DEFAULT_TIMEOUT_SEC):
    """
    Args:
      server_name - Name for this server process. Must be unique across the
          cluster.
      engine - RedisEngine compatible storage engine.
      roles - List of ServerRoles for this process.
      ping_frequency - Frequency in seconds at which the registration will be
          refreshed.
      roles - List of ServerRoles values.
      flush_frequency - Frequency in seconds at which expired servers will be
          unregistered.
      timeout - Timeout in seconds after which a server is considered dead,
          and will be unregistered on the next flush pass.
    """
    self.name = server_name
    self.engine = engine
    self.roles = roles
    self.ping_frequency = ping_frequency
    self.flush_frequency = flush_frequency
    self.timeout = timeout

    self._Ping()
    thread_util.ScheduleOperationWithPeriod(self.ping_frequency, self._Ping)

    thread_util.ScheduleOperationWithPeriod(
        self.flush_frequency,
        self.FlushExpiredServers)

  def GetName(self):
    """
    Returns:
      The name of the current server process.
    """
    return self.name

  def _Ping(self):
    """Refresh the current server's registration in the database.
    """
    # Refresh the registration to the current millisecond.
    id_str = _MakeIdString(self.roles)

    util.CheckedOp('putting refreshed identity %s: %s' % (self.name, id_str),
        self.engine.HashPut,
        KEY_IDENTITY_MAP, self.name, id_str)

  def ShutDown(self):
    """ShutDown hook. Removes the current server from the registry.
    """
    LOG.info('Unregistering identity...')
    util.CheckedOp('removing identity',
        self.engine.HashDelete,
        KEY_IDENTITY_MAP, self.name)

  def NumServers(self):
    """Retrieve the current number of registered servers.

    Returns:
      Number of currently registered servers.
    """
    op = util.StrictOp('retrieving number of registered servers',
        self.engine.HashSize,
        KEY_IDENTITY_MAP)

    return op.response_value

  def GetAllServers(self, role=None):
    """Retrieve the set of all registered servers (alive and expired).

    Args:
      role - ServerRole value, if specified, restricts results to servers
          matching that role.

    Returns:
      ({Alive Server Name: [Roles]}, {Expired Server Name: [Roles]}).
    """
    op = util.StrictOp('retrieving all registered servers',
        self.engine.HashGetAll,
        KEY_IDENTITY_MAP)

    if not op.response_value:
      return set(), set()

    refresh_cutoff = time.time() - self.timeout

    active_servers, expired_servers = {}, {}
    for name, id_str in op.response_value.iteritems():
      refreshed, roles = _ParseIdString(id_str)

      if role and role not in roles:
        continue

      if refreshed >= refresh_cutoff:
        active_servers[name] = roles
      else:
        expired_servers[name] = roles

    return active_servers, expired_servers

  def FlushExpiredServers(self):
    """Remove all the expired servers from the registry.

    Returns:
      Operation object.
    """
    _, expired_servers = self.GetAllServers()

    if not expired_servers:
      return

    LOG.warning('Expiring Servers: %s' % ', '.join(expired_servers))

    all_ops = []
    for name in expired_servers:
      all_ops.append(util.CheckedOp('deleting expired server %s % name',
          self.engine.HashDelete,
          KEY_IDENTITY_MAP, name))

    return util.CompoundOperation(*all_ops)

def _MakeIdString(roles):
  """Generate the string to store for this server in the database Hash.

  Args:
    roles - List of ServerRoles for this server.

  Returns:
    String to store in the database.
  """
  now_str = '%.3f' % time.time()
  roles_str = ','.join(roles)
  return '|'.join([now_str, roles_str])

def _ParseIdString(id_str):
  """Parse an database string generated by _MakeIdString.

  Returns:
    (Creation UTC timestamp, [Roles])
  """
  time_str, roles_str = id_str.split('|')
  return float(time_str), roles_str.split(',')
