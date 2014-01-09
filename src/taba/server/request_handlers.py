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
from taba.handlers.tab_type import RenderMode

"""Request handler functions for the Taba Server. These methods translate
between the HTTP message formats and the Front-end.
"""

from collections import defaultdict
import functools
import logging

import cjson

from taba.third_party import bottle
from taba.server.frontend import CapacityException
from taba.server.model import model_provider
from taba.util import instrumentation_util
from taba.util import misc_util

LOG = logging.getLogger(__name__)

def route(*args, **kwargs):
  return bottle.route(*args, method=['GET', 'POST'], **kwargs)

###########################################################
# Request parameter parsers
###########################################################

def valid_parameters(*params):
  """Decorator for request handlers which checks that the given query parameter
  names are within a list of valid parameters.

  Args:
    params - List of parameter names
  """

  def decorator(fn):

    @functools.wraps(fn)
    def wrapper(request, *args, **kwargs):
      invalid_params = []

      for key in request.input():
        if key not in params:
          invalid_params.append(key)

      if len(invalid_params) > 0:
        bottle.abort(400, 'Invalid parameters: %s' % ', '.join(invalid_params))

      return fn(request, *args, **kwargs)

    return wrapper
  return decorator

def _GetAccept(default='text/plain'):
  """Get a normalized MIME type from the HTTP ACCEPT header.

  Args:
    default - MIME type to use is one isn't specified.

  Returns:
    MIME type string, normalized to lower-case.
  """
  accept = bottle.request.headers.get('HTTP_ACCEPT', None)
  if accept in (None, '', '*/*'):
    accept = default

  return accept.lower()

def _GetDry():
  """Extract the parameter from the request indicating whether to write changes.

  Returns:
    True if the DB writes should be skipped, False otherwise.
  """
  dry_str = _GetParam('dry')
  return (dry_str.lower() != 'false' if dry_str else True)

def _GetUseRegularExpresions():
  """Extract the parameter from the request indicating whether the 'name' and
  'client' parameters represent lists or regular expressions.

  Returns:
    True if the filters are regular expressions, False otherwise.
  """
  re_str = _GetParam('re')
  return (re_str is not None)

def _GetIsRaw():
  """Extract the parameter indicating whether to render fetched states.

  Returns:
    True if States should be returned raw (e.g. not rendered), False otherwise.
  """
  raw_str = _GetParam('raw')
  return (raw_str is not None)

def _GetNames():
  """Extract the Tab Names filter parameter.

  Returns:
    Tab Names filter. If using regular expressions, a single string is returned,
    otherwise a list of strings is returned.
  """
  use_re = _GetUseRegularExpresions()

  names_str = _GetParam('name')

  if not names_str:
    return None
  elif use_re:
    return names_str
  else:
    return names_str.split(',')

def _GetClients():
  """Extract the Client ID filter parameter.

  Returns:
    Client ID filter. If using regular expressions, a single string is returned,
    otherwise a list of strings is returned.
  """
  use_re = _GetUseRegularExpresions()

  clients_str = _GetParam('client')

  if not clients_str:
    return None
  elif use_re:
    return clients_str
  else:
    return clients_str.split(',')

def _GetParam(param_name):
  """Extract a parameter from a request object. Looks in both the GET parameters
  and POST body. If the POST body it checked, the content will be deserialized
  and cached.

  Args:
    param_name - Name of the parameter to extract.

  Returns:
    Value of the parameter, or None if it isn't present.
  """
  param_val = bottle.request.query.get(param_name)
  if param_val is not None:
    return param_val

  if not hasattr(bottle.request, 'body_decoded'):
    if bottle.request.body:
      bottle.request.body_decoded = cjson.decode(bottle.request.body)
    else:
      bottle.request.body_decoded = {}

  return bottle.request.body_decoded.get(param_name, None)

###########################################################
# Event Receivers
###########################################################

#
# TODO: Use 'Accept-Encoding' HTTP header to differentiate between gzip or not.
#
@bottle.post('/post_zip')
def HandlePostCompressed():
  """Request handler for posting events to the Taba Server.
  """
  with instrumentation_util.Timer('taba_post_zip') as tm:
    body = misc_util.StreamingDecompressor(bottle.request.body)

    try:
      events = model_provider.GetFrontend().ReceiveEventsStreaming(body)
    except CapacityException:
      bottle.abort(503, "Service is temporarily at capacity. Retry later.")

  LOG.info("Finished request %f, (%f) (%d) (%f)" % \
      (tm.Start, tm.ElapsedSec, events, tm.ElapsedMs / events if events else 0))

@bottle.post('/post')
def HandlePostDirect():
  """Request handler for posting events directly form the Taba Client (instead
  of the Taba Agent) to the Taba Server.
  """
  with instrumentation_util.Timer('taba_post_direct') as tm:
    body_buffer = bottle.request.body

    try:
      events = model_provider.GetFrontend().ReceiveEventsStreaming(body_buffer)
    except CapacityException:
      bottle.abort(503, "Service is temporarily at capacity. Retry later.")

  LOG.info("Finished direct request %f, (%f) (%d)" \
      % (tm.Start, tm.ElapsedSec, events))

###########################################################
# State Retrieval
###########################################################

def _MakeSpecLogString(clients, names):
  """Generate a string representation of the Clients and Names arguments,
  truncating long lists.

  Args:
    clients - String or list of strings representing the Client query.
    names - String or list of strings representing the Name query.

  Returns:
    String representation suitable for logging.
  """
  def _to_str(arg):
    if not arg:
      arg_str = str(arg)
    elif type(arg) is list:
      arg_str = ', '.join(arg)
    else:
      arg_str = arg

    if len(arg_str) > 500:
      arg_str = '%s <...%d...>' % (arg_str[:500], len(arg_str))

    return arg_str

  return '(%s, %s)' % (_to_str(clients), _to_str(names))

@route('/projection')
@valid_parameters('name', 'client', 're', 'raw')
def HandleGetProjection():
  """Request handler to get non-aggregated Tabs.
  """
  # Parse and validate query parameters.
  use_re = _GetUseRegularExpresions()
  use_raw = _GetIsRaw()
  names = _GetNames()
  clients = _GetClients()

  spec_str = _MakeSpecLogString(clients, names)
  LOG.info("Starting Get Projection %s" % spec_str)

  with instrumentation_util.Timer('get_projection') as timer:
    accept = _GetAccept()

    renders = model_provider.GetFrontend().GetStates(
        names, clients, use_re, use_raw, accept)

    # Render the Projection objects according to the requested format.
    if accept in (RenderMode.JSON, RenderMode.DETAIL):
      data = defaultdict(dict)
      for (client, name), state in renders:
        if name and client:
          data[name][client] = state

      yield '{'
      for ni, (name, client_to_state) in enumerate(data.iteritems()):
        prefix = '' if ni == 0 else ','
        yield '%s%s: {' % (prefix, cjson.encode(name))

        for ci, (client, state) in enumerate(client_to_state.iteritems()):
          prefix = '' if ci == 0 else ','
          yield '%s%s: %s' % (prefix, cjson.encode(client), state)

        yield '}'

      yield '}'

    else:
      for (client, name), state in sorted(renders):
        yield '(%s, %s): %s\n' % (client, name, state)

  LOG.info("Finished Get Projection %s (%.2f)" % (spec_str, timer.ElapsedSec))

@route('/tabs')
@valid_parameters('name', 'client', 're', 'raw')
def HandleGetTabs():
  """Request handler to retrieve aggregated Tabs.
  """
  # Parse and validate query parameters.
  use_re = _GetUseRegularExpresions()
  use_raw = _GetIsRaw()
  names = _GetNames()
  clients = _GetClients()

  spec_str = _MakeSpecLogString(clients, names)
  LOG.info("Starting Get Tabs %s" % spec_str)

  with instrumentation_util.Timer('get_tabs') as timer:
    accept = _GetAccept()

    renders = model_provider.GetFrontend().GetAggregates(
        names, clients, use_re, use_raw, accept)

    # Render the Projection objects according to the requested format.
    if accept in (RenderMode.JSON, RenderMode.DETAIL):
      yield '{'
      for i, (name, state) in enumerate(renders):
        prefix = '' if i == 0 else ','
        yield '%s%s: %s' % (prefix, cjson.encode(name), state)
      yield '}'

    else:
      for name, state in sorted(renders):
        yield '%s: %s\n' % (name, state)

  LOG.info("Finished Get Tabs %s (%.2f)" % (spec_str, timer.ElapsedSec))

@route('/supertabs')
@valid_parameters('name', 'client', 're', 'raw')
def HandleGetSuperTabs():
  """Request handler to retrieve "super aggregated" Tabs.
  """
  # Parse and validate query parameters.
  use_re = _GetUseRegularExpresions()
  use_raw = _GetIsRaw()
  names = _GetNames()
  clients = _GetClients()

  spec_str = _MakeSpecLogString(clients, names)
  LOG.info("Starting Get Super Tabs %s" % spec_str)

  with instrumentation_util.Timer('get_super_tabs') as timer:
    accept = _GetAccept()

    render = model_provider.GetFrontend().GetSuperAggregate(
        names, clients, use_re, use_raw, accept) or ''

    # Render the Projection objects according to the requested format.
    if accept in (RenderMode.JSON, RenderMode.DETAIL):
      yield render

    else:
        yield render
        yield'\n'

  LOG.info("Finished Get Super Tabs %s (%.2f)" % (spec_str, timer.ElapsedSec))

###########################################################
# Name, Client, Type Accessors
###########################################################

@route('/clients')
@valid_parameters('client', 're')
def HandleGetClients():
  """Request handler to retrieve all the Client IDs.
  """
  req_clients = _GetClients()
  use_re = _GetUseRegularExpresions()

  clients = model_provider.GetFrontend().GetAllClients(req_clients, use_re)

  # Render the Client IDs according to the requested format.
  accept = _GetAccept()

  if accept == RenderMode.JSON:
    yield cjson.encode(clients)

  else:
    yield '\n'.join(sorted(clients))
    yield '\n'

@route('/names')
@valid_parameters('name', 'client', 're')
def HandleGetNames():
  """Request handler to retrieve Tab Names.
  """
  clients = _GetClients()
  names = _GetNames()
  use_re = _GetUseRegularExpresions()

  names = model_provider.GetFrontend().GetNames(clients, names, use_re)

  # Render the Tab Names according to the requested format.
  accept = _GetAccept()

  if accept == RenderMode.JSON:
    yield cjson.encode(names)

  else:
    yield '\n'.join(sorted(names))
    yield '\n'

@route('/types')
@valid_parameters('name', 're')
def HandleGetTypes():
  """Request handler to retrieve the Tab Type for a Tab Name.

  Parameters:
    name - (Optional) The Tab Name to retrieve the Tab Type for.

  Accept Encodings:
    text/plain (default)
    application/json

  Response:
    Tab Type string.
  """
  names = _GetNames()
  use_re = _GetUseRegularExpresions()

  types = model_provider.GetFrontend().GetTypes(names, use_re)

  # Render the types depending on the Accept header.
  accept = _GetAccept()

  if accept == RenderMode.JSON:
    yield cjson.encode(types)

  else:
    for name, type in sorted(types.iteritems()):
      yield '%s: %s\n' % (name, type)

@route('/ids')
@valid_parameters()
def HandleGetIdMaps():
  """Request handler to retrieve the internal Name<->NID and Client<->CID
  mappings.

  Accept Encodings:
    text/plain (default)
    application/json
  """
  names, clients = model_provider.GetFrontend().GetIdMaps()

  accept = _GetAccept()
  if accept == RenderMode.JSON:
    yield cjson.encode({'names': names, 'clients': clients})

  else:
    yield 'Names:\n'
    for name, nid in sorted(names.iteritems()):
      yield '  %s: %s\n' % (name, nid)

    yield 'Clients:\n'
    for client, cid in sorted(clients.iteritems()):
      yield '  %s: %s\n' % (client, cid)

@route('/recompute')
@valid_parameters()
def HandleRecomputeQueues():
  """Request handler to start Queue assignment recomputation.
  """
  model_provider.GetFrontend().StartRecomputeQueues()
  yield "Started\n"

@route('/gc')
@valid_parameters()
def HandleCollectGarbage():
  """Request handler to perform a pruning of the Taba Database. Spawns a
  background greenlet to perform the operation and returns immediately.
  """
  model_provider.GetFrontend().StartGarbageCollector()
  yield "Started\n"

@route('/upgrade')
@valid_parameters()
def HandleUpgrade(request):
  """Request handler to start a State Upgrade process. Spawns a background
  greenlet to perform the operation and returns immediately.
  """
  model_provider.GetFrontend().StartUpgradeAll()
  yield "Started\n"

@route('/flush_all')
@valid_parameters()
def HandleFlushAll():
  """Request handler to start a Flush process. Spawns a background greenlet to
  perform the operation and returns immediately.
  """
  model_provider.GetFrontend().FlushAllQueues()

@route('/delete')
@valid_parameters('name')
def HandleDelete():
  """Request handler to delete a Tab by Name.
  """
  names = _GetNames()
  model_provider.GetFrontend().Delete(names)

@route('/check')
@valid_parameters('dry')
def HandleSelfCheck():
  """Request handler to start a full system self-check. Spawns a background
  greenlet to perform the operation and returns immediately.

  Parameters:
    dry - If 'false', actually delete inconsistencies. Otherwise just check.
  """
  dry = _GetDry()
  model_provider.GetFrontend().SelfCheck(dry)
  yield 'Started\n'

@route('/status')
@valid_parameters()
def HandleStatus():
  """Request handler to retrieve the Taba Server status.

  Accept Encodings:
    application/json (default)
    text/plain

  Returns:
    Statistics about backends, tasks in queue, and task latencies.
  """
  status = model_provider.GetFrontend().GetStatus()

  accept = _GetAccept()

  if accept == RenderMode.TEXT:
    servers = status['servers']
    yield 'Servers: %d\n' % len(servers)
    yield 'Max Age: %.2f s\n' % status['max_age']
    yield 'Tasks:   %d\n\n' % status['total_tasks']

    name_width = max(map(len, servers.keys())) if servers else 0
    yield ' ' * name_width
    yield ' | R. Ld |  Wgt |  Qs | Tsks |  Age\n'

    for server, stat in sorted(servers.iteritems()):
      server_str = server + ' ' * (name_width - len(server))
      yield ('%s | %+5d | %4d | %3d | %4d | %5.2f\n' % (
          server_str, stat['load'], stat['weight'], stat['queues'],
          stat['tasks'], stat['age']))

  else:
    yield cjson.encode(status)

@route('/servers')
@valid_parameters()
def HandleServers():
  """Request handler to retrieve the list of registered active Taba Server
  processes.

  Accept Encodings:
    application/json (defaut)
    text/plain

  Returns:
    Dict of server names to a list of roles they registered for.
  """
  servers = model_provider.GetFrontend().GetServers()

  accept = _GetAccept()
  if accept == RenderMode.TEXT:
    for server, roles in servers.iteritems():
      yield '%s: %s\n' % (server, roles)

  else:
    yield cjson.encode(servers)

@route('/latency')
@valid_parameters()
def HandleLatency():
  """Request handler to retrieve server cluster latency statistics.
  """
  latencies = model_provider.GetFrontend().GetLatencies()

  accept = _GetAccept()
  if accept == 'text/plain':
    for server, roles in sorted(latencies.iteritems()):
      yield '%s: %s\n' % (server, roles)

  else:
    yield cjson.encode(latencies)
