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

"""Methods for dealing with encoding/decoding Events for transport.
"""

from collections import defaultdict

import cjson
import gevent

# Delimiter between consecutive entries in a block.
RECORD_DELIMITER = '\n'

# Protocol version constants.
PROTOCOL_VERSION_CURRENT = '1'
PROTOCOL_VERSIONS = ['1']

# Maximum number of Events to include in a single EventBundle when using
# streaming decode. To decrease maximum memory usage, decrease this number, and
# to make processing more efficient, increase it.
MAX_EVENTS_PER_BUNDLE = 50000

class Event(object):
  """Wrapper object for a Event."""
  __slots__ = ['_timestamp', '_payload']

  def __init__(self, timestamp, payload):
    self._timestamp = timestamp
    self._payload = payload

  @property
  def timestamp(self):
    return self._timestamp

  @property
  def payload(self):
    return self._payload

  def __repr__(self):
    return 'Event(%s, %s)' % (self.timestamp, self.payload)

###########################################################
# Encoders
###########################################################

def MakeEventTuple(tab_type, timestamp, payload):
  """Return a canonical Event Tuple.

  Args:
    tab_type - The Tab Type identifier.
    timestamp - Timestamp for the Event, in UTC seconds.
    payload - The value being recorded for the Event.

  Returns:
    Event Tuple in the canonical form.
  """
  return (tab_type, timestamp, payload)

def EncodeEventTuple(event):
  """Convert an Event Tuple to the encoded string representation.

  Args:
    event - Canonical Event Tuple.

  Returns:
    String representation of the Event Tuple.
  """
  gevent.sleep(0)

  encoded_event = list(event)
  encoded_event[2] = cjson.encode(encoded_event[2])
  return cjson.encode(encoded_event)

def Encode(client_id, name_events_map, encode_events=True):
  """Generate an encoded string for transmission.

  Args:
    client_id - Client ID string that the Events belong to.
    name_events_map - A mapping of {Tab Name: [Event Tuples]}
    encode_events - If True, encode individual events. Otherwise, leave them as
       given (they must already be encoded).

  Returns:
    An encoded string ready for transmission.
  """
  return EncodeMultiClient({client_id: name_events_map}, encode_events)

def EncodeMultiClient(client_to_name_to_events, encode_events=True):
  """Generate an encoded string for transmission, for multiple Client IDs.

  Args:
    client_to_name_to_events - Dict of dicts of lists the form:
        {Client ID: {Tab Name: [Event Tuples]}}
    encode_events - If True, encode individual events. Otherwise, leave them as
       given (they must already be encoded).

  Returns:
    An encoded string ready for transmission.
  """
  parts = [PROTOCOL_VERSION_CURRENT, '']

  for client, name_to_events in client_to_name_to_events.iteritems():
    parts.append(client)
    parts.append('')

    for name, events in name_to_events.iteritems():
      parts.append(name)

      if encode_events:
        events = map(EncodeEventTuple, events)

      parts.extend(events)
      parts.append('')

      gevent.sleep(0)

    parts.append('')

  parts.append('')
  return RECORD_DELIMITER.join(parts)

###########################################################
# Decoders
###########################################################

STATE_VERSION_START = 0
STATE_VERSION_END = 1
STATE_CLIENT_START = 2
STATE_CLIENT_END = 3
STATE_NAME = 4
STATE_EVENTS = 5

def Decode(body, decode_events=True):
  """Decode a body that was encoded for transmission.

  Args:
    body - StringIO containing the body to decode.
    decode_events - If True, decode the JSON Events into Python objects. If
        False, leave Events in their serialized string representation.

  Returns:
    An object of the form: {Client ID: {Tab Name: [Events, ...]}}.
  """
  events_map = defaultdict(lambda: defaultdict(list))

  for bundle in DecodeStreaming(body, decode_events):

    # Un-stream and possibly decode the Events in the bundle.
    events = [ev for ev in bundle]
    if decode_events:
      events = [
          MakeEventTuple(bundle.tab_type, ev.timestamp, ev.payload)
          for ev in events]

    # Add the current events to the total map.
    events_map[bundle.client][bundle.name].extend(events)

  return events_map

def DecodeStreaming(body, decode_events=True, do_yield=True):
  """Decode a body that was encoded for transmission, generating a stream of
  EventBundle objects.

  Args:
    body - StringIO containing the body to decode.
    decode_events - If True, decode the JSON Events into Python objects. If
        False, leave Events in serialized string representation.
    do_yield - If True, periodically yield to other greenlets.

  Returns:
    Generator emitting EventBundle objects.
  """
  from taba.common import transport_native   #@UnresolvedImport

  protocol_version = None

  names = set()

  state = STATE_VERSION_START
  total_events = 0
  client_id = None

  bundle = transport_native.EventBundle(decode_events)

  for line in body:
    line = line.rstrip('\n')

    if state == STATE_VERSION_START:
      protocol_version = line[:]
      if protocol_version not in PROTOCOL_VERSIONS:
        raise Exception('Invalid version %s' % protocol_version)
      state = STATE_VERSION_END

    elif state == STATE_VERSION_END:
      if line != '':
        raise Exception('Expected newline after version')
      state = STATE_CLIENT_START

    elif state == STATE_CLIENT_START:
      client_id = line
      bundle.client = client_id
      state = STATE_CLIENT_END

    elif state == STATE_CLIENT_END:
      if line != '':
        raise Exception('Expected newline after Client ID')
      state = STATE_NAME

    elif state == STATE_NAME:
      if line == '':
        state = STATE_CLIENT_START
      else:
        bundle.name = line
        names.add(line)

        state = STATE_EVENTS

    elif state == STATE_EVENTS:
      if line == '':
        # Blank line -> separation between blocks. Yield the current bundle and
        # start a new one.
        yield bundle

        bundle = transport_native.EventBundle(decode_events)
        bundle.client = client_id
        state = STATE_NAME

      else:
        bundle.AddEventLine(line)
        total_events += 1

        if bundle.num_events >= MAX_EVENTS_PER_BUNDLE:
          # We reached the maximum number of Events per EventBundle. Yield the
          # current one, and start a new one (bootstrapping it with the current
          # state.
          yield bundle

          name = bundle.name
          bundle = transport_native.EventBundle(decode_events)
          bundle.client = client_id
          bundle.name = name

    else:
      raise Exception('Invalid state %s' % state)

    if total_events % 5000 == 0 and do_yield:
      gevent.sleep(0)

  # Process the last block, in case the body is missing the final blank line.
  if bundle.num_events > 0:
    yield bundle
