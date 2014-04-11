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

import copy

from taba.handlers.tab_type import TabType

_DEFAULT_HANDLERS = {
    TabType.BUFFER: 'taba.handlers.buffer.Buffer',
    TabType.GAUGE: 'taba.handlers.gauge.Gauge',
    TabType.EXPIRY_GAUGE: 'taba.handlers.expiry_gauge.ExpiryGauge',
    TabType.TOTALS_COUNTER: 'taba.handlers.totals_counter.TotalsCounter',
    TabType.PERCENTILE_COUNTER: (
        'taba.handlers.dynamic_percentile_counter.DynamicPercentileCounter'),
    TabType.COUNTER_GROUP: 'taba.handlers.pyramid_group.PyramidGroup',
    TabType.PERCENTILE_GROUP: (
        'taba.handlers.moving_interval_percentile_group.'
        'MovingIntervalPercentileGroup')}

_HANDLERS = {}

def InitializeRegistry(additional_handlers=None):
  """Instantiate the Handler objects.

  Args:
    additional_handlers - Mapping from TabType name string to fully qualified
        class name of a Handler to use for that type. This will augment and/or
        override the defaults.
  """
  type_to_class_name = copy.copy(_DEFAULT_HANDLERS)
  type_to_class_name.update(additional_handlers or {})

  for tab_type, handler_class_name in type_to_class_name.iteritems():
    name_parts = handler_class_name.split('.')
    module_name = '.'.join(name_parts[:-1])
    class_name = name_parts[-1]

    module = __import__(module_name, fromlist=[name_parts[-2]])
    _HANDLERS[tab_type] = getattr(module, class_name)()

def GetHandler(tab_type):
  """Retrieve the Tab Handler for the given type.

  Args:
    tab_type - The Tab Handler type identifier string.

  Returns:
    A TabHandler object.
  """
  return _HANDLERS.get(tab_type)
