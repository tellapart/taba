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

"""Module maintaining a mapping of Tab Handlers to Types.
"""

from taba.handlers.buffer import Buffer
from taba.handlers.dynamic_percentile_counter import DynamicPercentileCounter
from taba.handlers.expiry_gauge import ExpiryGauge
from taba.handlers.gauge import Gauge
from taba.handlers.moving_interval_percentile_group import MovingIntervalPercentileGroup
from taba.handlers.pyramid_group import PyramidGroup
from taba.handlers.tab_type import TabType
from taba.handlers.totals_counter import TotalsCounter

_HANDLER_REGISTRY = {}

def GetHandler(tab_type):
  """Retrieve the Tab Handler for the given type

  Args:
    type - The Tab Handler type identifier string.

  Returns:
    A TabHandler object.
  """
  global _HANDLER_REGISTRY
  return _HANDLER_REGISTRY.get(tab_type)

def _RegisterHandler(tab_type, handler_class):
  """Registers a TabHandler class to handle Events of the given ID.

  Args:
    type - The Tab Type identifier the handler will handle.
    handler_class - A class derived from TabHandler.
  """
  global _HANDLER_REGISTRY

  if tab_type in _HANDLER_REGISTRY:
    raise Exception("Tab Type %s already registered" % tab_type)

  _HANDLER_REGISTRY[type] = handler_class()

_RegisterHandler(TabType.BUFFER, Buffer)

_RegisterHandler(TabType.GAUGE, Gauge)
_RegisterHandler(TabType.EXPIRY_GAUGE, ExpiryGauge)

_RegisterHandler(TabType.TOTALS_COUNTER, TotalsCounter)
_RegisterHandler(TabType.PERCENTILE_COUNTER, DynamicPercentileCounter)

_RegisterHandler(TabType.COUNTER_GROUP, PyramidGroup)
_RegisterHandler(TabType.PERCENTILE_GROUP, MovingIntervalPercentileGroup)
