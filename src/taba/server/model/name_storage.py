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

"""Class for managing the mapping of Tab Names to Name IDs (NIDs), and the
mapping from NIDs to TabTypes. Values in both mappings are cached locally
with a TTL.
"""

import copy
import logging

from taba.server.storage import util
from taba.server.storage.double_index_storage import DoubleIndexStorageManager
from taba.server.storage.mapping_storage import MappingStorageManager

# Redis key prefixes.
KEY_PREFIX_NID = 'N'
KEY_PREFIX_TYPES = 'T'

# Local cache Time-to-Live in seconds.
CACHE_TTL_SEC = 3600

LOG = logging.getLogger(__name__)

class NameStorageManager(object):
  """Class for managing the mapping of Tab Names to Name IDs (NIDs), and the
  mapping from NIDs to TabTypes.
  """

  def __init__(self, engine):
    """
    Args:
      engine - Storage engine compatible with RedisEngine.
    """
    self.engine = engine

    self.nid_index_manager = DoubleIndexStorageManager(
        engine=self.engine,
        key_prefix=KEY_PREFIX_NID,
        cache_ttl=CACHE_TTL_SEC)

    self.type_map_manager = MappingStorageManager(
        engine=self.engine,
        key_prefix=KEY_PREFIX_TYPES,
        cache_ttl=CACHE_TTL_SEC)

  def GetNids(self, names, create_if_new=False):
    """Retrieve NIDs for a list of Names. If create_if_new is True, any Names
    not currently in the mapping will be added with newly generated NID.

    Args:
      names - List of Names to retrieve NIDs.
      create_if_new - If True, Names that aren't in the mapping will be added.

    Returns:
      Dict of {Name: NID}.
    """
    op = util.StrictOp('retrieving NIDs for Names',
        self.nid_index_manager.GetIdsForValues,
        names, create_if_new)
    return op.response_value

  def GetAllNids(self):
    """Retrieve the list of all known NIDs. This does _not_ pull from the local
    cache (but will add results to it).

    Returns:
      List of NIDs.
    """
    op = util.StrictOp('retrieving all NIDs',
        self.nid_index_manager.GetAllIds)
    return op.response_value

  def GetNames(self, nids):
    """Retrieve Names for a list of NIDs.

    Args:
      nids - List of NIDs to retrieve Names for.

    Returns:
      Dict of {NID: Name}
    """
    op = util.StrictOp('retrieving Names for NIDs',
        self.nid_index_manager.GetValuesForIds,
        nids)
    return op.response_value

  def GetAllNames(self):
    """Retrieve the list of all known Names. This does _not_ pull from the local
    cache (but will add results to it).

    Returns:
      List of Names.
    """
    op = util.StrictOp('retrieving all Names',
        self.nid_index_manager.GetAllValues)
    return op.response_value

  def GetNameToNidMap(self):
    """Retrieve the full {Name: NID} mapping. This does _not_ pull from the
    local cache (but will add results to it).

    Returns:
      Dict of {Name: NID}
    """
    op = util.StrictOp('retrieving total Name => NId mapping',
        self.nid_index_manager.GetAllValueIdMap)
    return op.response_value

  def RemoveName(self, name):
    """Remove a Name from the NID and Type mappings.

    Note: Make sure the Name and NID aren't referenced anywhere else!

    Args:
      name - Name to remove.
    """
    # Retrieve the NID for the Name so that we can also update the Type map.
    nid = self.GetNids([name], create_if_new=False).get(name, None)
    if nid == None:
      # Name doesn't exist.
      return util.Operation(success=True)

    # Delete the Name <==> NID entries, and Type mapping.
    rem_nid_op = self.nid_index_manager.RemoveValue(name)
    rem_typ_op = self.type_map_manager.Remove(nid)

    return util.CompoundOperation(rem_nid_op, rem_typ_op)

  def GetTypes(self, nids, from_cache=True):
    """Retrieve the TabTypes for a list of NIDs.

    Args:
      nids - List of NIDs to lookup TabTypes for.
      from_cache - If True, look in the local cache before checking the DB.

    Returns:
      Dict of {NID: Tab Type}.
    """
    op = util.StrictOp('retrieving Types for NIDs',
        self.type_map_manager.GetBatch,
        nids, from_cache)
    return op.response_value

  def GetAllTypes(self):
    """Retrieve the full mapping of {NID: TabType}. This does _not_ pull from
    the local cache.

    Returns:
      Dict of {NID: TabType}
    """
    op = util.StrictOp('retrieving total NID: Type map',
        self.type_map_manager.GetAll)

    mapping = dict(
        (int(k), v)
        for k, v in op.response_value.iteritems()
        if k.isdigit())

    return mapping

  def GetOrUpdateTypes(self, nid_to_type_guess):
    """Given a mapping of NIDs to supposed TabTypes, update the Type mapping in
    the database, and check for inconsistencies with the already known Types.

    Args:
      nid_to_type_guesses - Dict of supposed {NID: TabType}.

    Returns:
      Actual dict of {NID: TabType}.
    """
    # Retrieve the known mapping from NID to TabType (from either the cache or
    # the DB if necessary).
    op = util.CheckedOp('retrieving NID -> Type map',
        self.type_map_manager.GetBatch,
        nid_to_type_guess.keys())
    if not op or not op.success or op.response_value is None:
      return None

    # Check if there are any NIDs present that do not have a 'known' TabType
    # in the DB. If there are any, add an entry from the original guesses.
    final_nid_to_type = copy.copy(nid_to_type_guess)
    new_nid_to_type = {}

    for nid, guess_type in nid_to_type_guess.iteritems():
      db_type = op.response_value.get(nid, None)
      if db_type is None:
        new_nid_to_type[nid] = guess_type
      else:
        final_nid_to_type[nid] = db_type

    if new_nid_to_type:
      self.type_map_manager.SetBatch(new_nid_to_type)

    # Cross-check the final Type map with the initial guesses to flag any
    # inconsistencies.
    for nid, tab_type in final_nid_to_type.iteritems():
      if tab_type != nid_to_type_guess[nid]:
        LOG.error('Type inconsistency for %s (In %s, Known %s)' % (
            nid, nid_to_type_guess[nid], tab_type))

    return final_nid_to_type

  def SelfCheck(self, dry=True):
    """Verify the consistency of the NID<->Name index and the NID->Type map.

    Args:
      dry - Whether this is a dry run. If False, inconsistencies will be
          deleted.
    """
    # Run self-check on the Name<->NID double index.
    LOG.info('Self-check: Checking Name<->NID mapping.')
    self.nid_index_manager.SelfCheck(dry)

    # Check that the NID->Type map doesn't contain any orphaned NID entries.
    LOG.info('Self-check: Checking NID->Type mapping.')
    nids = self.GetAllNids()
    type_map = self.GetAllTypes()

    orphan_types = set()

    for nid, tab_type in type_map.iteritems():
      if nid not in nids:
        LOG.error('Orphaned type mapping T(%s > %s)' % (nid, tab_type))
        orphan_types.add(nid)

    LOG.info('Bad type mappings: %s' % ', '.join(map(str, orphan_types)))

    if not dry:
      for nid in orphan_types:
        self.type_map_manager.Remove(nid)
