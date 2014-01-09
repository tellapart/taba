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

"""'Abstract' Tab Handler class that groups several Handlers together.
"""

from itertools import izip

from taba.handlers.tab_handler import TabHandler

class CounterGroup(TabHandler):
  """'Abstract' Tab Handler class that groups several Handlers together.

  Sub-classes should at least implement wrap NewState(), and add class member
  variables COUNTERS and LABELS, each of which should be an array of
  instantiated TabHandlers and string labels, respectively.
  """
  CURRENT_VERSION = 0

  def NewState(self, client_id, name, group_state):
    """See base class definition."""
    from taba.server.model.state_manager import StateStruct
    return [
        StateStruct(state, handler.CURRENT_VERSION, None)
        for handler, state in izip(self.COUNTERS, group_state)]

  def FoldEvents(self, group_state, events):
    """See base class definition."""
    group_state = self._Unpack(group_state)

    for handler, state in izip(self.COUNTERS, group_state):
      state.payload = handler.FoldEvents(state.payload, events)

    return group_state

  def Reduce(self, group_states):
    """See base class definition."""
    from taba.server.model.state_manager import StateStruct

    group_states = map(self._Unpack, group_states)

    reduced_group = [None] * len(self.COUNTERS)
    for i, handler in enumerate(self.COUNTERS):
      payload = handler.Reduce([gs[i].payload for gs in group_states])
      reduced_group[i] = StateStruct(payload, handler.CURRENT_VERSION, None)

    return reduced_group

  def Render(self, group_state, accept):
    """See base class definition."""
    group_state = self._Unpack(group_state)
    datas = [
        (lbl, hdlr.Render(state.payload, accept))
        for hdlr, lbl, state in izip(self.COUNTERS, self.LABELS, group_state)]

    return '{%s}' % ', '.join([
        '"%s": %s' % (label, data) for label, data in datas])

  def Upgrade(self, group_state, version):
    """See base class definition."""
    if version == 0:
      return group_state
    else:
      raise ValueError('Unknown version %s' % version)

  def ShouldPrune(self, group_state):
    """See base class definition."""
    group_state = self._Unpack(group_state)
    return all(
        handler.ShouldPrune(state.payload)
        for handler, state in izip(self.COUNTERS, group_state))

  def _Unpack(self, group_state):
    """For an incoming Group State, upgrade it is necessary.

    Args:
      group_state - Group State object.

    Returns:
      Group State, possible upgraded to the current version.
    """
    for handler, state in izip(self.COUNTERS, group_state):
      if state.version != handler.CURRENT_VERSION:
        state.payload = handler.Upgrade(state.payload, state.version)
        state.version = handler.CURRENT_VERSION

    return group_state
