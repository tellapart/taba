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
Simple Taba Handler that accepts single numeric inputs and tracks the count
and total values.
"""

from tellapart.taba.handlers.taba_handler import TabaHandler
from tellapart.taba.taba_event import TABA_EVENT_IDX_VALUE

class TotalsCounterState(object):
  """Wrapper class for TotalCounter States.
  """
  def __init__(self, count, total):
    self.count = count
    self.total = total

class TotalsCounter(TabaHandler):
  """Simple Taba Handler that accepts single numeric inputs and tracks the count
  and total values.
  """
  CURRENT_VERSION = 1

  def NewState(self, client_id, name):
    """See base class definition.
    """
    return TotalsCounterState(0, 0.0)

  def FoldEvents(self, state, events):
    """See base class definition.
    """
    state.count += len(events)
    state.total += sum([e[TABA_EVENT_IDX_VALUE][0] for e in events])

    return state

  def ProjectState(self, state):
    """See base class definition.
    """
    return {'count': state.count, 'total': state.total}

  def Aggregate(self, projections):
    """See base class definition.
    """
    aggregate = {
        'count': sum([p['count'] for p in projections]),
        'total': sum([p['total'] for p in projections])}
    return aggregate

  def Render(self, name, projections):
    """See base class definition.
    """
    renders = [
        '%s\t%.2f\t%d' % (name, p['total'], p['count'])
        for p in projections]

    return renders

  def Upgrade(self, state, version):
    """See base class definition.
    """
    if version == 1:
      return state

    else:
      raise ValueError('Unsupported state version %d' % version)

  def ShouldPrune(self, state):
    """See base class definition.
    """
    projection = self.ProjectState(state)
    if projection['count'] == 0 and projection['total'] == 0:
      return True

    return False
