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
Taba Handler that groups MovingIntervalCounterGroups together, with a
different suffix for each.
"""

import base64
from collections import defaultdict

import cjson

from tellapart.taba.handlers.moving_interval_counter_group import \
    MovingIntervalCounterGroup
from tellapart.taba.handlers.taba_handler import TabaHandler
from tellapart.taba.taba_event import TABA_EVENT_IDX_VALUE

MOVING_INTERVAL_COUNTER_GROUP = MovingIntervalCounterGroup()

class CommonPrefixCounterGroupState(object):
  """Wrapper class for CommonPrefixCounterGroup State objects.
  """
  def __init__(self, client_id, name, suffixes):
    self.client_id = client_id
    self.name = name
    self.suffixes = suffixes

class CommonPrefixCounterGroup(TabaHandler):
  """Taba Handler that groups MovingIntervalCounterGroups together, with a
  different suffix for each.
  """

  # NOTE: This version number is kept in-sync with the MovingIntervalCounter
  # class, since all contained State objects are upgraded at the same time.
  CURRENT_VERSION = MovingIntervalCounterGroup.CURRENT_VERSION

  def __init__(self):
    self._server = None

  @property
  def server(self):
    return self._server

  @server.setter
  def server(self, value):
    self._server = value
    MOVING_INTERVAL_COUNTER_GROUP.server = value

  def NewState(self, client_id, name):
    """See base class definition.
    """
    return CommonPrefixCounterGroupState(
        client_id=client_id,
        name=name,
        suffixes={})

  def FoldEvents(self, group_state, events):
    """See base class definition.
    """
    # Regroup the events by suffix. Remove the suffix field from the events
    # passed to the sub-handlers, and create new sub-handler States for new
    # suffixes.
    suffix_events_map = defaultdict(list)
    for event in events:
      suffix = event[TABA_EVENT_IDX_VALUE][0]
      value = event[TABA_EVENT_IDX_VALUE][1]

      temp_event = list(event)
      temp_event[TABA_EVENT_IDX_VALUE] = [value]
      suffix_events_map[suffix].append(temp_event)

      if not suffix in group_state.suffixes:
        client_id = group_state.client_id
        name = group_state.name
        group_state.suffixes[suffix] = \
            MOVING_INTERVAL_COUNTER_GROUP.NewState(
                client_id, '%s_%s' % (name, suffix))

    # Fold the events for each suffix into the States.
    for suffix, suffix_events in suffix_events_map.iteritems():
      state = group_state.suffixes[suffix]
      new_state = MOVING_INTERVAL_COUNTER_GROUP.FoldEvents(state, suffix_events)
      group_state.suffixes[suffix] = new_state

    return group_state

  def ProjectState(self, group_state):
    """See base class definition.
    """
    projections = {}
    for suffix, state in group_state.suffixes.iteritems():
      projections[suffix] = MOVING_INTERVAL_COUNTER_GROUP.ProjectState(state)

    return projections

  def Aggregate(self, projections):
    """See base class definition.
    """
    suffix_projection_map = defaultdict(list)
    for projection in projections:
      for suffix, micg_projection in projection.iteritems():
        suffix_projection_map[suffix].append(micg_projection)

    aggregates = {}
    for suffix, projection in suffix_projection_map.iteritems():
      aggregates[suffix] = MOVING_INTERVAL_COUNTER_GROUP.Aggregate(projection)

    return aggregates

  def Render(self, name, projections):
    """See base class definition.
    """
    renders = []
    for projection in projections:
      for suffix, sub_projection in projection.iteritems():
        renders.extend(
            MOVING_INTERVAL_COUNTER_GROUP.Render(
                '%s_%s' % (name, suffix),
                [sub_projection]))

    return renders

  def Upgrade(self, group_state, version):
    """See base class definition.
    """
    if version != MOVING_INTERVAL_COUNTER_GROUP.CURRENT_VERSION:
      group_state.suffixes = dict([
          (suffix, MOVING_INTERVAL_COUNTER_GROUP.Upgrade(state, version))
          for suffix, state in group_state.suffixes.iteritems()])

    return group_state

  def ShouldPrune(self, group_state):
    """See base class definition.
    """
    return all([
        MOVING_INTERVAL_COUNTER_GROUP.ShouldPrune(state)
        for state in group_state.suffixes.values()])
