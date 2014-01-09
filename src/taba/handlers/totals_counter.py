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

"""Simple Tab Handler that accepts single numeric inputs and tracks the count
and total values.
"""

from taba.handlers.tab_handler import TabHandler

class TotalsCounterState(object):
  """Wrapper class for TotalCounter States.
  """
  def __init__(self, count, total):
    self.count = count
    self.total = total

class TotalsCounter(TabHandler):
  """Simple Tab Handler that accepts single numeric inputs and tracks the count
  and total values.
  """
  CURRENT_VERSION = 0

  def NewState(self, client_id, name):
    """See base class definition."""
    return TotalsCounterState(0, 0.0)

  def FoldEvents(self, state, events):
    """See base class definition."""
    count = 0
    total = 0.0
    for event in events:
      count += 1
      total += int(event.payload[0])

    state.count += count
    state.total += total
    return state

  def Reduce(self, states):
    """See base class definition."""
    if len(states) == 0:
      return None
    elif len(states) == 1:
      return states[0]

    base = states[0]
    for state in states[1:]:
      base.count += state.count
      base.total += state.total

    return base

  def Render(self, state, accept):
    """See base class definition."""
    avg = state.total / state.count if state.count != 0 else 0
    return '{"count": %d, "total": %.2f, "average": %.2f}' % (
        state.count, state.total, avg)

  def Upgrade(self, state, version):
    """See base class definition."""
    return state

  def ShouldPrune(self, state):
    """See base class definition."""
    return (state.count == 0)
