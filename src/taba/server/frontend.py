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

"""Taba Server front-end implementation. Serves requests to external requesters
(after the specific interface parsing has been done).
"""

import logging
import re

import gevent

from taba import client
from taba.common import transport
from taba.server.model import model_provider
from taba.util import misc_util
from taba.util import thread_util

_CAPACITY_REJECTIONS = client.Tab('taba_updates_rejected')

LOG = logging.getLogger(__name__)

class CapacityException(Exception):
  """Exception class used to indicate a service is over capacity."""
  pass

class TabaServerFrontend(object):
  """Taba Server front-end implementation.
  """

  def __init__(self, name_blacklist, client_blacklist):
    """
    Args:
      name_blacklist - List of regular expressions which will be used to ignore
          any matching Tab Names. If None, no Name blacklisting will be
          performed.
      client_blacklist - List of regular expressions which will be used to
          ignore any matching Client Names. If None, no Client blacklisting
          will be performed.
    """
    self.name_blacklist_patterns = _ConvertReListToPatterns(name_blacklist)
    self.client_blacklist_patterns = _ConvertReListToPatterns(client_blacklist)

  def ReceiveEventsStreaming(self, events_buffer):
    """Receive a batch of Events. Folds them into Tasks and places those on
    the processing Queues.

    Args:
      events_buffer - File-like object that will emit the posted request body,
          line-by-line.

    Returns:
      Number of events processed (this excludes any blacklisted events).
    """
    if not model_provider.GetQueues().CanAcceptUpdate():
      _CAPACITY_REJECTIONS.Record()
      raise CapacityException()

    # Callback which processes a single EventBundle object.
    def _ReceiveBatch(event_bundle):
      client = event_bundle.client
      name = event_bundle.name

      # Check if either the Name or the Client are blacklisted.
      if (_ItemMatchesAnyPattern(self.name_blacklist_patterns, name) or
          _ItemMatchesAnyPattern(self.client_blacklist_patterns, client)):
        LOG.info(
            'Filtering %d events for C:%s N:%s' %
            (event_bundle.num_events, client, name))
        return 0

      # Get or create the CID, NID, and Tab Type for the Client, Name, and
      # incoming Tab Type.
      cid = model_provider.GetClients().GetCids([client], True)[client]
      nid = model_provider.GetNames().GetNids([name], True)[name]
      tab_type = model_provider.GetNames().GetOrUpdateTypes(
          {nid: event_bundle.tab_type})[nid]

      # Convert the EventBundle into a partial Serialized State, and add it to
      # a backend processing Queue.
      try:
        sstate = model_provider.GetStates().ConvertNewEvents(
              client, cid, name, nid, tab_type, event_bundle)

      except Exception:
        LOG.error('Error folding events for %s (%s)' % (name, client))
        raise

      model_provider.GetQueues().EnqueueUpdate(nid, cid, sstate)

      # Add the events to the latency statistics.
      model_provider.GetLatency().RecordForPost(event_bundle)
      return event_bundle.num_events

    # Process the event buffer using an AsyncProcessor (a light-weight worker
    # pool). Use a streaming decoder to minimize in-flight memory usage.
    events_gen = transport.DecodeStreaming(events_buffer)
    result_aggregator = misc_util.Aggregator()

    processor = thread_util.AsyncProcessor(
        events_gen,
        _ReceiveBatch,
        result_aggregator,
        workers=4)
    processor.Process()

    return result_aggregator.value

  #########################################################
  # State Accessors
  #########################################################

  def GetStates(self,
      names=None,
      clients=None,
      use_re=False,
      raw=False,
      accept=None):
    """Retrieve all _non-aggregated_ States matching the query parameters.

    If use_re is True, then the names and clients arguments should each be a
    single string regular expression which will be matched against all known
    Tab Names and Clients, respectively. If use_re is False, then names and
    clients should be lists of explicit strings to filter results to.

    Args:
      names - Tab Names filter to apply.
      clients - Client ID filter to apply.
      use_re - Whether the names and clients arguments represent single regular
          expressions, or lists of explicit values to filter to.
      raw - If True, State objects will not be rendered (i.e. the raw binary
          form will be emitted).
      accept - MIME type for rendering States (only used if raw is False).

    Returns:
      List of: [((Tab Name, Client ID), State String)]
    """
    # Revolve the NIDs to lookup.
    if names is not None and use_re:
      all_names = model_provider.GetNames().GetAllNames()
      filter_names = _FilterListByRegularExpression(all_names, names)
      name_to_nid = model_provider.GetNames().GetNids(filter_names)
      nid_to_name = dict((v, k) for k, v in name_to_nid.iteritems())
      nids = filter(None, name_to_nid.values())

    elif names is not None:
      name_to_nid = model_provider.GetNames().GetNids(names)
      nid_to_name = dict((v, k) for k, v in name_to_nid.iteritems())
      nids = filter(None, name_to_nid.values())

    else:
      name_to_nid = model_provider.GetNames().GetNameToNidMap()
      nid_to_name = dict((v, k) for k, v in name_to_nid.iteritems())
      nids = None

    # Revolve the CIDs to lookup.
    if clients is not None and use_re:
      all_clients = model_provider.GetClients().GetAllClients()
      filter_clients = _FilterListByRegularExpression(all_clients, clients)
      client_to_cid = model_provider.GetClients().GetCids(filter_clients)
      cid_to_client = dict((v, k) for k, v in client_to_cid.iteritems())
      cids = filter(None, client_to_cid.values())

    elif clients is not None:
      client_to_cid = model_provider.GetClients().GetCids(clients)
      cid_to_client = dict((v, k) for k, v in client_to_cid.iteritems())
      cids = filter(None, client_to_cid.values())

    else:
      client_to_cid = model_provider.GetClients().GetClientCidMap()
      cid_to_client = dict((v, k) for k, v in client_to_cid.iteritems())
      cids = None

    # Get the State and return.
    it = model_provider.GetStates().StatesIterator(nids, cids, (not raw), accept)
    return [
        ((nid_to_name.get(nid, None), cid_to_client.get(cid, None)), state)
        for ((nid, cid), state) in it]

  def GetAggregates(self,
      names=None,
      clients=None,
      use_re=False,
      raw=False,
      accept=None):
    """Retrieve all _aggregated_ States matching the query parameters.

    If use_re is True, then the names and clients arguments should each be a
    single string regular expression which will be matched against all known
    Tab Names and Clients, respectively. If use_re is False, then names and
    clients should be lists of explicit strings to filter results to.

    Args:
      names - Tab Names filter to apply.
      clients - Client ID filter to apply.
      use_re - Whether the names and clients arguments represent single regular
          expressions, or lists of explicit values to filter to.
      raw - If True, State objects will not be rendered (i.e. the raw binary
          form will be emitted).
      accept - MIME type for rendering States (only used if raw is False).

    Returns:
      List of: [(Tab Name, State String)]
    """
    # Revolve the NIDs to lookup.
    if names is not None and use_re:
      all_names = model_provider.GetNames().GetAllNames()
      filter_names = _FilterListByRegularExpression(all_names, names)
      name_to_nid = model_provider.GetNames().GetNids(filter_names)
      nid_to_name = dict((v, k) for k, v in name_to_nid.iteritems())
      nids = filter(None, name_to_nid.values())

    elif names is not None:
      name_to_nid = model_provider.GetNames().GetNids(names)
      nid_to_name = dict((v, k) for k, v in name_to_nid.iteritems())
      nids = filter(None, name_to_nid.values())

    else:
      name_to_nid = model_provider.GetNames().GetNameToNidMap()
      nid_to_name = dict((v, k) for k, v in name_to_nid.iteritems())
      nids = None

    # Revolve the CIDs to lookup.
    if clients is not None and use_re:
      all_clients = model_provider.GetClients().GetAllClients()
      filter_clients = _FilterListByRegularExpression(all_clients, clients)
      client_to_cid = model_provider.GetClients().GetCids(filter_clients)
      cids = filter(None, client_to_cid.values())
    elif clients is not None:
      client_to_cid = model_provider.GetClients().GetCids(clients)
      cids = filter(None, client_to_cid.values())
    else:
      cids = None

    # Get the Aggregates and return.
    it = model_provider.GetStates().AggregatesIterator(
        nids, cids, (not raw), accept)
    return [(nid_to_name[nid], state) for (nid, state) in it]

  def GetSuperAggregate(self,
      names=None,
      clients=None,
      use_re=False,
      raw=False,
      accept=None):
    """Retrieve a "super aggregated" State from all the individual States
    matching the query parameters.

    If use_re is True, then the names and clients arguments should each be a
    single string regular expression which will be matched against all known
    Tab Names and Clients, respectively. If use_re is False, then names and
    clients should be lists of explicit strings to filter results to.

    Args:
      names - Tab Names filter to apply.
      clients - Client ID filter to apply.
      use_re - Whether the names and clients arguments represent single regular
          expressions, or lists of explicit values to filter to.
      raw - If True, State objects will not be rendered (i.e. the raw binary
          form will be emitted).
      accept - MIME type for rendering States (only used if raw is False).

    Returns:
      A single State String
    """
    # Resolve the NIDs to lookup.
    if names is not None and use_re:
      all_names = model_provider.GetNames().GetAllNames()
      filter_names = _FilterListByRegularExpression(all_names, names)
      name_to_nid = model_provider.GetNames().GetNids(filter_names)
      nids = filter(None, name_to_nid.values())

    elif names is not None:
      name_to_nid = model_provider.GetNames().GetNids(names)
      nids = filter(None, name_to_nid.values())

    else:
      nids = None

    # Resolve the CIDs to lookup.
    if clients is not None and use_re:
      all_clients = model_provider.GetClients().GetAllClients()
      filter_clients = _FilterListByRegularExpression(all_clients, clients)
      client_to_cid = model_provider.GetClients().GetCids(filter_clients)
      cids = filter(None, client_to_cid.values())

    elif clients is not None:
      client_to_cid = model_provider.GetClients().GetCids(clients)
      cids = filter(None, client_to_cid.values())

    else:
      cids = None

    return model_provider.GetStates().SuperAggregate(
        nids, cids, (not raw), accept)

  #########################################################
  # Misc. Accessors
  #########################################################

  def GetAllClients(self, filter_val=None, use_re=False):
    """Retrieve the list of Client IDs matching the query parameters.

    Args:
      filter_val - Filter to apply to Client IDs.
      use_re - If True, filter_val should be a regular expression to match
          Client IDs to. Otherwise, filter_val should be a list of explicit
          strings to filter to.

    Returns:
      List of Client ID strings.
    """
    clients = model_provider.GetClients().GetAllClients()

    if filter_val and use_re:
      return _FilterListByRegularExpression(clients, filter_val)
    elif filter_val:
      return list(set(filter_val).intersection(set(clients)))
    else:
      return clients

  def GetNames(self, clients=None, filter_val=None, use_re=False):
    """Retrieve a list of Tab Names matching the query parameters.

    Args:
      clients - Filter to apply to Client IDs.
      filter_val - Filter to apply to Tab Names.
      use_re - If True, them the clients and filter_val arguments should be
          single string regular expressions. Otherwise, they should each be
          a list of explicit strings to filter to.

    Returns:
      List of Tab Names.
    """
    if clients:
      # Client filter specified. Find the appropriate set of CIDs to filter to
      # (depending on whether Regular Expressions are enabled).
      if use_re:
        all_clients = model_provider.GetClients().GetAllClients()
        filter_clients = _FilterListByRegularExpression(all_clients, clients)
      else:
        filter_clients = clients

      cids = model_provider.GetClients().GetCids(filter_clients)
      nids = set()
      for cid in filter(None, cids.values()):
        nids.update(model_provider.GetStates().GetNidsForCid(cid))

      names = model_provider.GetNames().GetNames(nids).values()

    else:
      # No Client filter specified; just get all Names.
      names = model_provider.GetNames().GetAllNames()

    # Return the list of Names, possibly filtered.
    if filter_val is not None and use_re:
      return _FilterListByRegularExpression(names, filter_val)
    elif filter_val is not None:
      return list(set(filter_val).intersection(set(names)))
    else:
      return names

  def GetTypes(self, names=None, use_re=False):
    """Retrieve the Tab Types for a set of Tab Names.

    Args:
      names - Filter to apply to Tab Names to lookup.
      use_re - If True, then names should be a single string regular expression.
          Otherwise, names should be a list of explicit strings to filter to.

    Returns:
      Dict of {Tab Name: TabType String}.
    """
    if names:

      if use_re:
        all_names = model_provider.GetNames().GetAllNames()
        filter_names = _FilterListByRegularExpression(all_names, names)
      else:
        filter_names = names

      name_to_nid = model_provider.GetNames().GetNids(filter_names)
      nid_to_name = dict([(nid, name) for name, nid in name_to_nid.iteritems()])

      nid_to_type = model_provider.GetNames().GetTypes(name_to_nid.values())
      types = dict([
          (nid_to_name[nid], type)
          for nid, type in nid_to_type.iteritems()])

    else:
      nid_to_type = model_provider.GetNames().GetAllTypes()
      nids = nid_to_type.keys()
      nid_to_name = model_provider.GetNames().GetNames(nids)

      types = dict([
          (nid_to_name[nid], tp)
          for nid, tp in nid_to_type.iteritems()
          if nid_to_name[nid]])

    return types

  def GetIdMaps(self):
    """Retrieve the Tab Name <-> NID and Client <-> CID mappings.

    Returns:
      ({Name: NID}, {Client: CID})
    """
    names_map = model_provider.GetNames().GetNameToNidMap()
    client_map = model_provider.GetClients().GetClientCidMap()
    return names_map, client_map

  def GetStatus(self):
    """Retrieve statistics about the cluster.

    Returns:
      Dict containing high level statistics.
    """
    queue_stats = model_provider.GetQueues().GetQueueStats()
    latency_stats = model_provider.GetLatency().GetStatistics()

    queue_stats.update(latency_stats)
    return queue_stats

  def GetLatencies(self):
    """Retrieve statistics on the current applied latencies.

    Returns:
      Dict containing statistics about applied latencies.
    """
    return model_provider.GetLatency().GetStatistics()

  def GetServers(self):
    """Retrieve the list of all alive servers in the cluster.

    Returns:
      Dict of {Server Name: Roles} for all alive servers in the cluster.
    """
    servers, _ = model_provider.GetIdentity().GetAllServers()
    return servers

  def StartRecomputeQueues(self):
    """Begin a background Queue assignment recomputation.
    """
    gevent.spawn(model_provider.GetQueues().ForceRecomputeShards)

  def StartUpgradeAll(self):
    """Begin a background Upgrade process.
    """
    gevent.spawn(model_provider.GetStates().UpgradeAll)

  def StartGarbageCollector(self):
    """Begin a background Garbage Collection process.
    """
    gevent.spawn(model_provider.GetStates().RunGarbageCollector)

  def Delete(self, names):
    """Delete a set of Tabs by name.

    Args:
      names - List of Tab Names to delete.
    """
    name_to_nids = model_provider.GetNames().GetNids(names)
    for name, nid in name_to_nids.iteritems():
      model_provider.GetNames().RemoveName(name)
      if nid is not None:
        model_provider.GetStates().Delete(nid)

  def SelfCheck(self, dry=True):
    """Perform a set of internal consistency checks, and optionally delete any
    inconsistencies found.

    Args:
      dry - Whether this is a dry run. If False, and inconsistencies found will
          be deleted.
    """
    def _do_check():
      model_provider.GetNames().SelfCheck(dry)
      model_provider.GetClients().SelfCheck(dry)
      model_provider.GetStates().SelfCheck(dry)
      model_provider.GetStates().DeepClean(dry)

      LOG.info('Self-check Complete.')

    gevent.spawn(_do_check)

  def FlushAllQueues(self):
    """Delete all pending tasks. USE WITH CAUTION!
    """
    model_provider.GetQueues().FlushAll()

def _FilterListByRegularExpression(items, exp):
  """Given a list of strings and a regular expression, return the subset of
  strings matching the expression.

  Args:
    items - List of strings.
    exp - Regular expression string.

  Returns:
    List of strings matching the regular expression.
  """
  pattern = re.compile(exp)
  return filter(pattern.match, items)

def _ConvertReListToPatterns(re_list):
  """Convert a list of regular expression strings into compiled Pattern objects.

  Args:
    re_list - List of regular expression strings (may be None).

  Returns:
    List of Pattern objects, or None if re_list is None.
  """
  if not re_list:
    return re_list

  return map(re.compile, re_list)

def _ItemMatchesAnyPattern(pattern_list, item):
  """Determine whether a string matches any Pattern in a list.

  Args:
    pattern_list - List of Pattern objects.
    item - String to check.

  Returns:
    True if item matches any Pattern in the list. False otherwise.
  """
  if not pattern_list:
    return False

  for pattern in pattern_list:
    if pattern.match(item):
      return True

  return False
