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
'Abstract' Taba Handler class that groups several Handlers together.
"""

from itertools import izip
import struct

from tellapart.taba.handlers.taba_handler import TabaHandler
from tellapart.taba.server.taba_state import StateStruct

class CounterGroup(TabaHandler):
  """'Abstract' Taba Handler class that groups several Handlers together.
  """
  CURRENT_VERSION = 1

  COUNTERS = None
  LABELS = None

  def NewState(self, client_id, name, states):
    """See base class definition.
    """
    return [
        StateStruct(state, cls.CURRENT_VERSION)
        for cls, state in izip(self.COUNTERS, states)]

  def FoldEvents(self, group_states, events):
    """See base class definition.
    """
    for cls, state in izip(self.COUNTERS, group_states):
      # Check if the member State object requires upgrading.
      if state.version != cls.CURRENT_VERSION:
        state.payload = cls.Upgrade(state.payload, state.version)
        state.version = cls.CURRENT_VERSION

      # Fold the Events into the State object.
      state.payload = cls.FoldEvents(state.payload, events)

    return group_states

  def ProjectState(self, group_states):
    """See base class definition.
    """
    # Check if any of the member State object require upgrading.
    for cls, state in izip(self.COUNTERS, group_states):
      if state.version != cls.CURRENT_VERSION:
        state.payload = cls.Upgrade(state.payload, state.version)
        state.version = cls.CURRENT_VERSION

    # Return the list of Projected States.
    return [
        cls.ProjectState(state.payload)
        for cls, state in izip(self.COUNTERS, group_states)]

  def Aggregate(self, group_projections):
    """See base class definition.
    """
    projections_by_label = zip(*group_projections)

    aggregates = [
        cls.Aggregate(projection_list)
        for cls, projection_list in izip(self.COUNTERS, projections_by_label)]

    return aggregates

  def Render(self, name, group_projections):
    """See base class definition.
    """
    renders = []
    for group_proj in group_projections:
      for cls, label, proj in izip(self.COUNTERS, self.LABELS, group_proj):
        renders.extend(cls.Render('%s_%s' % (name, label), [proj]))

    return renders

  def Upgrade(self, group_states, version):
    """See base class definition.
    """
    if version == 1:
      return group_states

    else:
      raise ValueError('Unsupported state version %d' % version)

  def ShouldPrune(self, group_states):
    """See base class definition.
    """
    # Check if any of the member State object require upgrading.
    for cls, state in izip(self.COUNTERS, group_states):
      if state.version != cls.CURRENT_VERSION:
        state.payload = cls.Upgrade(state.payload, state.version)
        state.version = cls.CURRENT_VERSION

    # Return the AND of all the member State ShouldPrune().
    return all(
        cls.ShouldPrune(state.payload)
        for cls, state in izip(self.COUNTERS, group_states))
