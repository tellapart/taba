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
Juno Request handler functions for the Taba Server. These methods translate
between the HTTP message formats and the TabaServer class.
"""

from cStringIO import StringIO
import time
import zlib

import cjson
import gevent

from tellapart.taba.util import misc_util
from tellapart.taba.taba_event import TABA_EVENT_IDX_VALUE
from tellapart.third_party import juno

LOG = misc_util.MakeStreamLogger(__name__)

global_taba_server = None

def TimedLoggedRequest(request_name):
  """Decorator for Juno request handlers that logs their parameters and
  execution time.

  Args:
    request_name - Human readable name of the request.
  """
  def _Decorator(fn):
    """Since the outer decorator is parameterized, we have to return this inner
    decorator when it is called.
    """

    def _Wrapper(request):
      """Wrap the request handler with logging messages and execution timer.
      """
      args_str = ', '.join([
          '%s: %s' % (str(k), str(v))
          for k, v in sorted(request.input().iteritems(), key=lambda t: t[0])])

      LOG.info("Starting %s (%s)" % (request_name, args_str))
      start = time.time()

      result = fn(request)

      LOG.info("Finished %s (%s) (%.2f)" % \
      (request_name, args_str, time.time() - start))

      return result

    return _Wrapper
  return _Decorator

def _GetAccept(request, default='text/plain'):
  """Get a normalized MIME type from the HTTP ACCEPT header.

  Args:
    request - JunoRequest object.
    default - MIME type to use is one is not specified.

  Returns:
    MIME type string, normalized to lower-case.
  """
  accept = request.raw.get('HTTP_ACCEPT') or default
  return accept.lower()

def _ParseBodyAndReceive(body):
  """Parse a received POST body into Client ID and Taba Event blocks, and send
  the blocks to the core Taba Server.

  Args:
    body - Clear-text (decompressed) File-like buffer containing the POST body
        of Events to parse.

  Returns:
    The total number of Events processed.
  """
  # Parse the document body according to the protocol.
  STATE_CLIENT = 0
  STATE_EVENTS = 1

  state = STATE_CLIENT
  all_events = 0
  for line in body:
    if line[-1] == '\n':
      line = line[:-1]

    if state == STATE_CLIENT:
      client_id = line
      events = []
      state = STATE_EVENTS

    elif state == STATE_EVENTS:
      if line == '':
        global_taba_server.ReceiveEvents(client_id, events)
        events = []
        state = STATE_CLIENT

      else:
        event = cjson.decode(line)
        event[TABA_EVENT_IDX_VALUE] = cjson.decode(event[TABA_EVENT_IDX_VALUE])

        all_events += 1
        events.append(event)

    if all_events % 5000 == 0:
      gevent.sleep(0)

  global_taba_server.ReceiveEvents(client_id, events)

  return all_events

def HandlePostCompressed(request):
  """Juno request handler for posting Events to the Taba Server, where the body
  is compressed.
  """
  start = time.time()

  # Decompress the POST document body.
  body_zip = request.raw['request_body_bytes'].read()
  if not body_zip:
    return

  body_buffer = StringIO(zlib.decompress(body_zip))

  all_events = _ParseBodyAndReceive(body_buffer)

  end = time.time()
  LOG.info("Finished request %f, (%f) (%d) (%f)" % \
      (start, end - start, all_events, (end - start) / all_events * 1000))

def HandlePostDirect(request):
  """Juno request handler for posting Events directly form the Taba Client
  (instead of the Taba Agent) to the Taba Server.
  """
  start = time.time()

  # Taba Clients don't compress or encrypt the body.
  body = request.raw['request_body_bytes'].read()
  if not body:
    return

  all_events = _ParseBodyAndReceive(StringIO(body))

  end = time.time()
  LOG.info("Finished direct request %f, (%f) (%d)" \
      % (start, end - start, all_events))

@TimedLoggedRequest("Get Raw")
def HandleGetRaw(request):
  """Juno request handler to retrieve raw State(s) from the DB.

  Get Parameters:
    client - Client ID to get State objects for, or blank for all Clients IDs.
    taba - Taba Name to get State objects for, or blank for all Taba Names.
    block - Taba Name Block ID to get State objects for, or blank for all.

  Response:
    State objects, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized.
  """
  # Parse and validate query parameters.
  client_id = request.input('client')
  name = request.input('taba')
  block = request.input('block')

  if name and block:
    juno.status(400)
    juno.append('Cannot specify both "taba" and "block"')
    return

  blocks = block.split(',') if block else None
  names = [name] if name else None

  return _RawResponse(client_id, names, blocks, _GetAccept(request))

@TimedLoggedRequest("Get Raw Batch")
def HandleGetRawBatch(request):
  """Juno request handler to retrieve a batch of raw States from the DB by Name.

  Post Body - a JSON dictionary with the following fields:
    client - Client ID to get State objects or, or blank for all.
    taba - List of Taba Names to get State objects for, or blank for all.
    block - List of Taba Name Block IDs to get State objects, or blank for all.

  Response:
    State objects, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized.
  """
  # Parse and validate query parameters.
  body_enc = request.raw['request_body_bytes'].read()
  params = cjson.decode(body_enc)

  client_id = params['client'] if 'client' in params else None
  names = params['taba'] if 'taba' in params else None
  blocks = params['block'] if 'block' in params else None

  if names and blocks:
    juno.status(400)
    juno.append('Cannot specify both "names" and "block"')
    return

  return _RawResponse(client_id, names, blocks, _GetAccept(request))

def _RawResponse(client_id, names, blocks, accept):
  """Retrieve raw State results, and add them to the response.

  Args:
    client_id - Client ID string to retrieve State objects for.
    names - List of Taba Names to retrieve State objects for.
    blocks - List of Taba Name Block IDs to retrieve State objects for.
    accept - MIME type in which to format the output.
  """
  # Get the requested State objects.
  states = global_taba_server.GetStates(client_id, names, blocks)

  # Format the response in accordance with the Accept header.
  if accept == 'text/json':
    juno.append(cjson.encode(dict([s for s in states])))

  else:
    for (state_client_id, state_name), state in states:
      juno.append("(").append(state_client_id).append(", ")
      juno.append(state_name).append("): ")
      juno.append(cjson.encode(state)).append("\n")

@TimedLoggedRequest("Get Projection")
def HandleGetProjection(request):
  """Juno request handler to retrieve raw Projection(s) from the DB.

  Get Parameters:
    client - Client ID to get Projection(s) for, or blank for all Clients IDs.
    taba - Taba Name to get Projection(s) for, or blank for all Taba Names.
    block - Taba Name Block to get Projections for, or blank for all.

  Response:
    Projection objects, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized.
  """
  # Parse and validate query parameters.
  client_id = request.input('client')
  name = request.input('taba')
  block = request.input('block')

  if name and block:
    juno.status(400)
    juno.append('Cannot specify both "taba" and "block"')
    return

  blocks = block.split(',') if block else None
  names = [name] if name else None

  return _ProjectionResponse(client_id, names, blocks, _GetAccept(request))

@TimedLoggedRequest("Get Projection Batch")
def HandleGetProjectionBatch(request):
  """Juno request handler to retrieve a batch of Projections from the DB.

  Post Body - a JSON dictionary with the following fields:
    client - Client ID to get Projection(s) for, or blank for all .
    taba - List of Taba Name to get Projection(s) for, or blank for all.
    block - Liat of Taba Name Block to get Projections for, or blank for all.

  Response:
    Projection objects, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized.
  """
  # Parse and validate query parameters.
  body_enc = request.raw['request_body_bytes'].read()
  params = cjson.decode(body_enc)

  client_id = params['client'] if 'client' in params else None
  names = params['taba'] if 'taba' in params else None
  blocks = params['block'] if 'block' in params else None

  if names and blocks:
    juno.status(400)
    juno.append('Cannot specify both "taba" and "block"')
    return

  return _ProjectionResponse(client_id, names, blocks, _GetAccept(request))

def _ProjectionResponse(client_id, names, blocks, accept):
  """Retrieve Projection results, and add them to the response.

  Args:
    client_id - Client ID string to retrieve Projections for.
    names - List of Taba Names to retrieve Projections for.
    blocks - List of Taba Name Block IDs to retrieve Projections for.
    accept - MIME type in which to format the output.
  """
  # Retrieve the requested Projections.
  projections = global_taba_server.GetProjections(client_id, names, blocks)

  # Render the Projection objects according to the requested format.
  if accept == 'text/json':
    juno.append(cjson.encode(dict([p for p in projections])))

  else:
    for (proj_client_id, proj_name), projection in projections:
      juno.append("(").append(proj_client_id).append(", ")
      juno.append(proj_name).append("): ")
      juno.append(cjson.encode(projection)).append("\n")

@TimedLoggedRequest("Get Aggregate")
def HandleGetAggregate(request):
  """Juno request handler to retrieve Aggregate Projections.

  Get Parameters:
    taba - Taba Name to retrieve Aggregates for, or blank for all Taba Names.
    block - Taba Name Block ID to retrieve Aggregates for, or blank for all.

  Response:
    Aggregate Projections, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized.
  """
  # Parse and validate query parameters.
  name = request.input('taba')
  block = request.input('block')

  if name and block:
    juno.status(400)
    juno.append('Cannot specify both "taba" and "block"')
    return

  names = [name] if name else None
  blocks = block.split(',') if block else None

  return _AggregateResponse(names, blocks, _GetAccept(request))

@TimedLoggedRequest("Get Aggregate Batch")
def HandleGetAggretateBatch(request):
  """Juno request handler to retrieve a batch of Aggregates from the DB.

  Post Body - a JSON dictionary with the following fields:
    taba - List of Taba Name to retrieve Aggregates for, or blank for all.
    block - List of Taba Name Block IDs to retrieve, or blank for all.

  Response:
    Aggregate Projections, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized.
  """
  # Parse and validate query parameters.
  body_enc = request.raw['request_body_bytes'].read()
  params = cjson.decode(body_enc)

  names = params['taba'] if 'taba' in params else None
  blocks = params['block'] if 'block' in params else None

  if names and blocks:
    juno.status(400)
    juno.append('Cannot specify both "taba" and "block"')
    return

  return _AggregateResponse(names, blocks, _GetAccept(request))

def _AggregateResponse(names, blocks, accept):
  """Retrieve Aggregate results, and add them to the response.

  Args:
    names - List of Taba Names to retrieve Aggregates for.
    blocks - List of Taba Name Block IDs to retrieve Aggregates for.
    accept - MIME type in which to format the output.

  """
  # Retrieve the requested Aggregates.
  aggregates = global_taba_server.GetAggregates(names, blocks)

  # Format the response in accordance to the Accept header.
  if accept == 'text/json':
    juno.append(cjson.encode(dict([a for a in aggregates])))

  else:
    for agg_name, aggregate in aggregates:
      juno.append(agg_name).append(": ").append(cjson.encode(aggregate))
      juno.append("\n")

def HandleGetTaba(request):
  """Juno Request handler to retrieve Rendered Tabs.

  Get Parameter:
    client - Client ID to retrieve Rendered Tabs for. If a Client ID is
        specified, this handler retrieves Rendered Projections. Otherwise it
        retrieved Rendered Aggregates.
    taba - Taba Name to retrieve Rendered Tabs for, or blank to retrieve all.
    block - Taba Name Block to retrieve Rendered Tabs for, or blank for all.

  Response:
    Rendered Tabs, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized.
  """
  # Parse and validate query parameters.
  client_id = request.input('client')
  name = request.input('taba')
  block = request.input('block')

  if name and block:
    juno.status(400)
    juno.append('Cannot specify both "taba" and "block"')
    return

  LOG.info("Starting Get Taba (%s, %s, %s)" % (client_id, name, block))
  start = time.time()

  blocks = block.split(',') if block else None
  names = [name] if name else None

  renders = global_taba_server.GetRendered(client_id, names, blocks)

  # Render the Projection objects according to the requested format.
  accept = request.raw.get('HTTP_ACCEPT') or 'text/plain'
  accept = accept.lower()

  if accept == 'text/json':
    juno.append(cjson.encode([r for r in renders]))

  else:
    for render in renders:
      juno.append(render).append('\n')

  LOG.info("Finished Get Taba (%s, %s, %s) (%.2f)" % \
      (client_id, name, block, time.time() - start))

def HandleGetClients(request):
  """Juno request handler to retrieve all the Client IDs.

  Response:
    List of Client IDs, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized list.
  """
  clients = global_taba_server.GetClients()

  # Render the Client IDs according to the requested format.
  accept = request.raw.get('HTTP_ACCEPT') or 'text/plain'
  accept = accept.lower()

  if accept == 'text/json':
    juno.append(cjson.encode(clients))

  else:
    juno.append('\n'.join(clients))

def HandleGetTabaNames(request):
  """Juno request handler to retrieve Taba Names.

  Get Parameters:
    client - Client ID to retrieve registered Taba Names for. If blank, then
        the Taba Names entry for all Clients is retrieved.

  Response:
    List of Taba Names, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized list.
  """
  req_client_id = request.input('client')

  if not req_client_id:
    names = global_taba_server.GetNames()

  else:
    names = global_taba_server.GetNamesForClient(req_client_id)

  # Render the Taba Names according to the requested format.
  accept = request.raw.get('HTTP_ACCEPT') or 'text/plain'
  accept = accept.lower()

  if accept == 'text/json':
    juno.append(cjson.encode(names))

  else:
    juno.append('\n'.join(names))

def HandleGetType(request):
  """Juno request handler to retrieve the Taba Type for a Taba Name.

  Get Parameters:
    taba - (Required) The Taba Name to retrieve the Taba Type for.

  Response:
    Taba Type, serialized depending on the Accept MIME type.

  Accept Types:
    text/plain (default) - Human readable.
    text/json - JSON serialized.
  """
  name = request.input('taba')
  if name:
    names = [name]
  else:
    names = None

  types = global_taba_server.GetTabaTypes(names)

  # Render the types depending on the Accept header.
  accept = request.raw.get('HTTP_ACCEPT') or 'text/plain'
  accept = accept.lower()

  if accept == 'text/json':
    juno.append(cjson.encode(dict(types)))

  else:
    for type in types:
      juno.append('%s: %s\n' % type)

def HandleDeleteName(request):
  """Juno request handler to delete all States for a Taba Name.

  Get Parameters:
    taba - (Required) The Taba Name to delete States for.
  """
  name = request.input('taba')
  if not name:
    juno.status(400)
    juno.append('Must specify "taba" parameter')
    return

  client_id = request.input('client')

  LOG.info("Starting Delete Taba (%s, %s)" % (client_id, name))
  global_taba_server.DeleteTaba(name, client_id)

def HandlePrune(request):
  """Juno request handler to perform a pruning of the Taba Database. Spawns a
  background greenlet to perform the operation and returns immediately.
  """
  gevent.spawn(global_taba_server.PruneAll)
  juno.append("Started\n")

def HandleUpgrade(request):
  """Juno request handler to start a State Upgrade process. Spawns a background
  greenlet to perform the operation and returns immediately.
  """
  force = request.input('force')
  if not force:
    force = False

  gevent.spawn(global_taba_server.Upgrade, force)
  juno.append("Started\n")

def HandleStatus(request):
  """Juno request handler to retrieve the taba Server status.

  Accept Types:
    text/plain (default) - Human readable new-line separated.
    text/json - JSON serialized.
  """
  accept = request.raw.get('HTTP_ACCEPT') or 'text/json'
  accept = accept.lower()

  agent_status = global_taba_server.Status()

  if accept == 'text/plain':
    for key, val in agent_status.iteritems():
      juno.append('%s: %s\n' % (key, val))

  else:
    juno.append(cjson.encode(agent_status))
