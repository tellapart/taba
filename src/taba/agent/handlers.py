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

"""Module containing request handlers for the Taba Agent.
"""

import logging
import traceback

import cjson

from taba.common import transport
from taba.agent.agent import TabaAgent
from taba.third_party import bottle

LOG = logging.getLogger(__name__)

# The TabaAgent singleton.
_GLOBAL_TABA_AGENT = TabaAgent()

@bottle.post('/post')
def HandlePost():
  """Request handler for receiving posted Events from a Client. The request is
  expected to be a POST where the body follows the protocol:
    client_id\n[serialized_event\n]*\n
  The request is expected to be in plain-text. Only one client should be present
  in the request. The client_id and serialized events cannot contain '\n'
  characters.
  """
  # Extract the Client ID and encoded Events.
  body = bottle.request.body
  try:
    client_to_names_to_events = transport.Decode(body, decode_events=False)

  except Exception:
    LOG.error("Error decoding posted data")
    LOG.error(traceback.format_exc())
    bottle.abort(400, "Could not parse document body")

  if not client_to_names_to_events:
    bottle.abort(400, "No events in POST document")

  # Add the events to the Taba Agent buffer.
  try:
    for client_id, name_events_map in client_to_names_to_events.iteritems():
      _GLOBAL_TABA_AGENT.Buffer(client_id, name_events_map)

  except Exception:
    LOG.error("Error buffering events")
    LOG.error(traceback.format_exc())
    bottle.abort(500, "Error buffering events")

@bottle.get('/status')
def HandleStatus():
  """Request handler for retrieving the status of the Taba Agent. The status is
  returned in the body of the response as a JSON string.
  """
  accept = bottle.request.headers.get('HTTP_ACCEPT') or 'text/plain'
  accept = accept.lower()
  if accept in ('', '*/*'):
    accept = 'text/plain'

  agent_status = _GLOBAL_TABA_AGENT.Status()

  if accept == 'text/plain':
    for key, val in agent_status.iteritems():
      if key != 'url_stats':
        yield '%s: %s\n' % (key, val)

    yield 'url_stats:\n'

    width = max([len(url) for url in agent_status['url_stats'].keys()])
    urls = sorted(agent_status['url_stats'].keys())
    for url in urls:
      stats = agent_status['url_stats'][url]
      stats_str = (
          'S: %(shards)3d | %(load_factor)+4d | '
          'B(%(total_events)6d, %(buffered_events)6d) | '
          'Q(%(queued_requests)3d, %(queued_events)6d) | '
          'P(%(pending_requests)3d, %(pending_events)6d)') % stats
      yield '  %s:%s%s\n' % (url, ' ' * (width - len(url) + 1), stats_str)

  else:
    bottle.response.content_type = 'application/json'
    yield cjson.encode(agent_status)
