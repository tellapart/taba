"""
Copyright 2012 TellApart, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

------------------------------------------------------------

Module containing Juno request handlers for the Taba Agent.
"""

import traceback

import cjson

from tellapart.taba.agent.taba_agent import TabaAgent
from tellapart.taba.util import misc_util
from tellapart.third_party import juno

LOG = misc_util.MakeStreamLogger(__name__)

# The TabaAgent singleton.
global_taba_agent = TabaAgent()

def HandlePost(request):
  """Juno request handler for receiving posted Taba Events from a Taba Client.
  The request is expected to be a POST where the body follows the protocol:
    client_id\n[serialized_event\n]*\n
  The request is expected to be in plain-text. Only one client should be present
  in the request. The client_id and serialized events cannot contain '\n'
  characters.
  """
  # Extract the Client ID and encoded Taba Events.
  body = request.raw['request_body_bytes']
  try:
    client_id = body.readline().strip()
    events = [l.strip() for l in body.readlines() if l.strip()]

  except Exception:
    juno.status(400)
    juno.append("Could not parse document body")
    return

  # Check that the parameters are at least present.
  if not client_id:
    juno.status(400)
    juno.append("Client ID must be the first line of the POST document")
    return

  if not events:
    juno.status(400)
    juno.append("No events in POST document")
    return

  # Add the events to the Taba Agent buffer.
  try:
    global_taba_agent.Buffer(client_id, events)

  except Exception:
    LOG.error("Error buffering events")
    LOG.error(traceback.format_exc())
    juno.status(500)
    juno.append("Error buffering events")
    return

def HandleStatus(request):
  """Juno request handler for retrieving the status of the Taba Agent. The
  status is returned in the body of the response as a JSON string.
  """
  accept = request.raw.get('HTTP_ACCEPT') or 'text/json'
  accept = accept.lower()

  agent_status = global_taba_agent.Status()

  if accept == 'text/plain':
    for key, val in agent_status.iteritems():
      juno.append('%s: %s\n' % (key, val))

  else:
    juno.append(cjson.encode(agent_status))
