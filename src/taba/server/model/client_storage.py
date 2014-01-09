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

"""Class for managing the mapping of Client Names to auto-generated numeric
Client IDs (CIDs). Values are also cached locally based on a TTL.
"""

import logging

from taba.server.storage import util
from taba.server.storage.double_index_storage import DoubleIndexStorageManager

# Key prefix used with the DoubleIndexStorageManager.
KEY_PREFIX = 'C'

# Local caching period.
CACHE_TTL_SEC = None  # Never expire.

LOG = logging.getLogger(__name__)

class ClientStorageManager(object):
  """Class for managing the mapping of Client Names to auto-generated numeric
  Client IDs (CIDs).
  """

  def __init__(self, engine):
    """
    Args:
      engine - Storage engine compatible with RedisEngine.
    """
    self.index_manager = DoubleIndexStorageManager(
        engine=engine,
        key_prefix=KEY_PREFIX,
        cache_ttl=CACHE_TTL_SEC)

  def GetCids(self, clients, create_if_new=False):
    """Retrieve the CIDs for a list of Client Names.

    Args:
      clients - Iterable of Client Names to retrieve CIDs for.
      create_if_new - If True, Client Names that do not have a CID associated
          with them will be added to the mapping, generating new CIDs.

    Returns:
      Dict of {Client Name: CID}.
    """
    op = util.StrictOp('retrieving CIDs for Client Names',
        self.index_manager.GetIdsForValues,
        clients, create_if_new)
    return op.response_value

  def GetClients(self, cids):
    """Retrieve the Client Names for a list of CIDs.

    Args:
      cids - Iterable of CIDs to retrieve Client Names for.

    Returns:
      Dict of {CID: Client Name}
    """
    op = util.StrictOp('retrieving Client Names for CIDs',
        self.index_manager.GetValuesForIds,
        cids)
    return op.response_value

  def GetAllClients(self):
    """Retrieve all known Client Names.

    Returns:
      List of Client Names.
    """
    op = util.StrictOp('retrieving all Client Names',
        self.index_manager.GetAllValues)
    return op.response_value

  def GetAllCids(self):
    """Retrieve all known CIDs.

    Returns:
      List of CIDs.
    """
    op = util.StrictOp('retrieving all CIDs',
        self.index_manager.GetAllIds)
    return op.response_value

  def GetClientCidMap(self):
    """Retrieve the full known mapping of Client Name to CID.

    Returns:
      Dict of {Client Name: CID}
    """
    op = util.StrictOp('retrieving total Client => CID map',
        self.index_manager.GetAllValueIdMap)
    return op.response_value

  def AddClient(self, client):
    """Add a new Client Name to the mapping (generating a new CID if the Client
    Name is new).

    Args:
      client - Client Name to add.
    """
    self.index_manager.AddValue(client)

  def RemoveClient(self, client):
    """Delete a Client Name from the mapping. (Note: CIDs will never be
    recycled, even after they are deleted).

    Args:
      client - Client Name to remove from the mapping.
    """
    self.index_manager.RemoveValue(client)

  def SelfCheck(self, dry=True):
    """Verify the consistency of the Client<->CID mapping.

    Args:
      dry - Whether this is a dry run. If False, inconsistencies will be
          deleted.
    """
    LOG.info('Self-check: Checking Client<->CID index.')
    self.index_manager.SelfCheck(dry)
