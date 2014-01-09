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

"""Tab Handler that does nothing. Useful for ignoring a particular Tab Type.
"""

class Noop(object):
  """Basic TabHandler that does nothing. Useful for ignoring a Tab Type.
  """
  CURRENT_VERSION = 0

  def NewState(self, client_id, name):
    """See base class definition."""
    return 'noop'

  def FoldEvents(self, state, events):
    """See base class definition."""
    return state

  def Reduce(self, states):
    """See base class definition."""
    return states[0]

  def Render(self, state, accept):
    """See base class definition."""
    return 'noop'

  def Upgrade(self, state, version):
    """See base class definition."""
    return state

  def ShouldPrune(self, state):
    """See base class definition."""
    return True
