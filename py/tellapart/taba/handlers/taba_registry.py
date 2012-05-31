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

Module maintaining a mapping of Taba Handlers to Types.
"""

from tellapart.taba.taba_type import TabaType

from tellapart.taba.handlers.buffer import BufferTaba
from tellapart.taba.handlers.common_prefix_counter_group import \
    CommonPrefixCounterGroup
from tellapart.taba.handlers.expiry_string_taba import ExpiryStringTaba
from tellapart.taba.handlers.moving_interval_counter import \
    MovingIntervalCounter
from tellapart.taba.handlers.moving_interval_counter_group import \
    MovingIntervalCounterGroup
from tellapart.taba.handlers.percentile_counter import PercentileCounter
from tellapart.taba.handlers.string_taba import StringTaba
from tellapart.taba.handlers.totals_counter import TotalsCounter

_TABA_HANDLER_REGISTRY = {}

def GetHandler(type):
  """Retrieve the Taba Handler for the given type

  Args:
    type - The Taba Handler type identifier string.

  Returns:
    A TabaHandler object.
  """
  global _TABA_HANDLER_REGISTRY
  return _TABA_HANDLER_REGISTRY.get(type)

def _RegisterHandler(type, handler_class):
  """Registers a TabaHandler class to handle TabaEvents of the given ID.

  Args:
    type - The Taba type identifier the handler will handle.
    handler_class - A class derived from TabaHandler.
  """
  global _TABA_HANDLER_REGISTRY

  if type in _TABA_HANDLER_REGISTRY:
    raise Exception("Taba type %s already registered" % type)

  _TABA_HANDLER_REGISTRY[type] = handler_class()

_RegisterHandler(TabaType.STRING,                  StringTaba)
_RegisterHandler(TabaType.TOTALS_COUNTER,          TotalsCounter)
_RegisterHandler(TabaType.PERCENTILE_COUNTER,      PercentileCounter)
_RegisterHandler(TabaType.MOVING_INTERVAL_COUNTER, MovingIntervalCounter)
_RegisterHandler(TabaType.COMMON_PREFIX_GROUP,     CommonPrefixCounterGroup)
_RegisterHandler(TabaType.COUNTER_GROUP,           MovingIntervalCounterGroup)
_RegisterHandler(TabaType.EXPIRY_STRING,           ExpiryStringTaba)
_RegisterHandler(TabaType.BUFFER,                  BufferTaba)
