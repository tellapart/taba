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

class TabaClientEngine(object):
  """Base class for Python Taba Client Engines. These classes handle the
  specifics of buffering and sending Events to external components (i.e. either
  a Taba Agent or Taba Server).
  """

  def __init__(self, client_id, event_post_url, flush_period):
    """
    Args:
      client_id - The Client ID string for this Taba Client. This value should
          be unique in the Taba deployment, and should be durable between
          restarts of the same logical client.
      event_post_url - URL to which to POST events.
      flush_period - Period in seconds at which to flush buffered events.
    """
    raise NotImplementedError()

  def Initialize(self):
    """Start any background operations.
    """
    raise NotImplementedError()

  def Stop(self):
    """Stop any background processes.
    """
    raise NotImplementedError()

  def RecordEvent(self, name, tab_type, value):
    """Create and buffer an Event.

    Args:
      name - Tab Name to post the event to.
      tab_type - Tab Type of the Tab Name.
      value - The value being recorded.
    """
    raise NotImplementedError()

  def Status(self):
    """Return a JSON encoded string of the Engine status.
    """
    raise NotImplementedError()
