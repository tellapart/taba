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

"""Blocking, Sentinel enabled Redis connection pool.
"""

import logging
import weakref
import time

from taba import client

from gevent.queue import LifoQueue, Empty
from redis.sentinel import SentinelManagedConnection
from taba.util import thread_util

LOG = logging.getLogger(__name__)

class BlockingSentinelMasterGeventConnectionPool(object):
  """Blocking, Sentinel enabled Redis connection pool.
  """

  # Timeout, in seconds, when trying to retrieve a connection from the
  # redis connection pool. This is set to infinite (i.e. a worker will wait
  # indefinitely for a connection to become available). Any actual remote
  # failure should be caught and surfaced by the socket timeout.
  GET_CONNECTION_TIMEOUT = None

  def __init__(self,
      service_name,
      sentinel_manager,
      pool_size=8,
      tab_prefix='redis_bsmg_pool',
      connection_class=SentinelManagedConnection,
      connection_kwargs={},
      sentinel_check_connections=False,
      is_master=True):
    """
    Args:
      service_name - Name of the Sentinel service name to connect to.
      sentinel_manager - Sentinel manager object.
      pool_size - Number of connections to maintain in the pool.
      tab_prefix - Tab name prefix for Tabs recorded by this class.
      connection_class - Class to use for creating connections. Must be a
          sub-class of (or be API compatible with) SentinelManagedConnection.
      connection_kwargs - Keyword arguments to pass through to connection
          constructor.
      sentinel_check_connections - Whether to enable Sentinel connection
          checking on establishing each connection.
      is_master - Always True. Included to match SentinelConnectionPool API.
    """
    self.service_name = service_name
    self.sentinel_manager = sentinel_manager
    self.pool_size = pool_size
    self.tab_prefix = tab_prefix
    self.conn_class = connection_class
    self.conn_kwargs = connection_kwargs

    # Sentinel connection pool API member variables.
    self.is_master = is_master
    self.check_connection = sentinel_check_connections
    self.master_address = None

    # Actual pool containers.
    self.closed = False
    self.all = set()
    self.pool = LifoQueue(maxsize=self.pool_size)

    # Initialize the pool.
    for _ in xrange(self.pool_size):
      conn = self.conn_class(
          connection_pool=weakref.proxy(self),
          **connection_kwargs)
      self.all.add(conn)
      self.pool.put(conn)

  def __repr__(self):
    return "%s<%s|%s>" % (
         type(self).__name__,
         self.connection_class.__name__,
         self.connection_kwargs)

  #########################################################
  # Connection Pool API Methods
  #########################################################

  def get_connection(self, command_name, *keys, **options):
    """Get a connection from the pool.

    Args: Ignored. Included to match ConnectionPool API.
    """
    if self.closed:
      raise Empty()

    try:
      return self.pool.get(timeout=self.GET_CONNECTION_TIMEOUT)
    except Empty as e:
      client.Counter(self.tab_prefix + '_redis_conn_pool_get_conn_timeout')
      LOG.error('Cannot get connection for %s:%d' % (self.host, self.port))
      raise e

  def release(self, connection):
    """Releases the connection back to the pool

    Args:
      connection - Connection to put back in the pool. Must have been initially
          taken from this pool.
    """
    if connection not in self.all:
      raise ValueError()
    self.pool.put(connection)

  def disconnect(self):
    """Disconnects all connections in the pool."""
    for conn in self.all:
      conn.disconnect()

  def shutdown(self):
    """Close the pool and disconnect all connections.
    """
    self.closed = True
    try:
      # Wait for all the connections to finish and get returned to the pool.
      def _wait_ready():
        while not self.pool.full():
          time.sleep(0.5)
      thread_util.PerformOperationWithTimeout(30, _wait_ready)

    except Exception as e:
      LOG.error(e)
    finally:
      # Disconnect anyway.
      self.disconnect()

  #########################################################
  # Sentinel Pool API Methods
  #########################################################

  def get_master_address(self):
    """SentinelConnectionPool API compatibility. Get the connection information
    to the service master.

    Returns:
      Tuple of (Master Hostname, Master Port)
    """
    master_address = self.sentinel_manager.discover_master(self.service_name)

    if self.master_address is None:
      self.master_address = master_address
    elif master_address != self.master_address:
      # Master address changed. Reset all connections.
      self.disconnect()

    return master_address

  def rotate_slaves(self):
    """SentinelConnectionPool API compatibility. Not implemented.
    """
    pass
