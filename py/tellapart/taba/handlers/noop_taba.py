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

Taba Handler that does nothing. Useful for ignoring a particular Taba Type.
"""

from tellapart.taba.handlers.taba_handler import TabaHandler

class Noop(TabaHandler):
  """Taba Handler that does nothing. Useful for ignoring a particular Taba Type.
  """

  def NewState(self, client_id, name):
    """See base class definition.
    """
    return ''

  def FoldEvents(self, state, events):
    """See base class definition.
    """
    return state

  def ProjectState(self, state):
    """See base class definition.
    """
    return {}

  def Aggregate(self, projections):
    """See base class definition.
    """
    return {}

  def Render(self, name, projections):
    """See base class definition.
    """
    return ["%s\tnoop" % name]

  def Upgrade(self, state, version):
    """See base class definition.
    """
    return state

  def ShouldPrune(self, state):
    """See base class definition.
    """
    return True
