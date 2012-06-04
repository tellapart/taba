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
Class and methods for dealing with Taba Events.
"""

import cjson

TABA_EVENT_IDX_NAME = 0
TABA_EVENT_IDX_TYPE = 1
TABA_EVENT_IDX_VALUE = 2
TABA_EVENT_IDX_TIME = 3

class TabaEvent(object):
  """Simple contained for Taba Events"""
  def __init__(self, name, type, value, timestamp):
    self.name = name
    self.type = type
    self.value = value
    self.timestamp = timestamp

def SerializeEvent(event):
  """Convert a Taba Event object into a representation that can be serialized

  Args:
    event - A TabaEvent object.

  Returns:
    A tuple of (name, type, val, timestamp) for the Event.
  """
  return (event.name, event.type, cjson.encode(event.value), event.timestamp)

def DeserializeEvent(val):
  """Convert the output of SerializeEvent() back into a TabaEvent object.

  Args:
    val - A tuple of (name, type, val, timestamp) for an Event.

  Returns:
    A corresponding TabaEvent object.
  """
  return TabaEvent(val[0], val[1], cjson.decode(val[2]), val[3])

