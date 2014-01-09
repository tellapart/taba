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

"""Base Tab Handler. Specifies the interface all Handlers must implement.
"""

class TabHandler(object):
  """'Abstract' base class for all Tab Handlers. Specifies the interface that
  must be implemented.

  IMPORTANT: TabHandler sub-classes must be state-less.
  """

  # Indicator of the current State object schema version. Sub-classes should
  # overwrite this value, and increment it each time the State schema changes.
  # The Upgrade method should also be capable of converting a State object from
  # each legacy version to the current one.
  CURRENT_VERSION = 0

  def NewState(self, client_id, name):
    """For the given client_id and name, generate a new State object. The object
    must be pickle-able.

    Args:
      client_id - The Client ID this state is representing.
      name - The Tab Name this state is representing.

    Returns:
      A newly initialized State object.
    """
    raise NotImplementedError()

  def FoldEvents(self, state, events):
    """Given a State object and a list of events for a single Client ID and Tab
    Name, fold the events into the State, and return the updated State.

    Args:
      state - A State object of the type returned by NewState().
      events - A list of TabEvent tuples for a single Client and Tab Name.

    Returns:
      A State object with the events folded in.
    """
    raise NotImplementedError()

  def Reduce(self, states):
    """Take a list of State objects, and generate a single aggregate State.
    e.g.: [{'cnt':10, 'tot':'15'}, {'cnt':20, 'tot':22}] => {'cnt':30, 'tot':37}

    Args:
      states - A list of State objects of the type returned by NewState().

    Returns:
      A single State object.
    """
    raise NotImplementedError()

  def Render(self, state, accept):
    """Given a State object, generate a string representation of that object.

    Args:
      state - A State object.
      accept - MIME Type the rendering method to use.

    Returns:
      A string representing the State object.
    """
    raise NotImplementedError()

  def Upgrade(self, state, version):
    """Upgrade a State object from the given version to the current version. The
    implementation of this method should be capable of converting a State object
    from any legacy version to the current one.

    Args:
      state - State object to upgrade.
      version - Version of the given State.

    Returns:
      State object upgraded to the current version schema.
    """
    raise NotImplementedError()

  def ShouldPrune(self, state):
    """Given a State object, return a boolean indicating whether the given State
    should be deleted from the database.

    Args:
      state - A State object of the type returned by NewState().

    Returns:
      True if the State should be deleted. False otherwise.
    """
    raise NotImplementedError()
