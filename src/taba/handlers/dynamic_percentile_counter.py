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

"""Tab Handler that accepts numeric input, and tracks the count and total, and
samples the values to generate cumulative distribution percentiles. Relies on
a Cython native implementation for serialization/deserialization and
calculations.
"""

from taba.handlers.tab_handler import TabHandler

from taba.handlers import dynamic_percentile_counter_native  #@UnresolvedImport

class DynamicPercentileCounter(TabHandler):
  """Tab Handler that accepts numeric input, and tracks the count and total,
  and samples the values to generate cumulative distribution percentiles. This
  is mostly a proxy to the Cython implementation 'percentile_counter_native'.
  """
  CURRENT_VERSION = 0

  def NewState(self, client_id, name, num_samples):
    """See base class definition."""
    return dynamic_percentile_counter_native.NewState(num_samples)

  def FoldEvents(self, state_buffer, events):
    """See base class definition."""
    return dynamic_percentile_counter_native.FoldEvents(state_buffer, events)

  def Reduce(self, state_buffers):
    """See base class definition."""
    return dynamic_percentile_counter_native.Reduce(state_buffers)

  def Render(self, state_buffer, accept):
    """See base class definition."""
    count, total, percentiles = \
        dynamic_percentile_counter_native.Render(state_buffer)

    pct_str = ', '.join(['%.2f' % v for v in percentiles])
    return '{"count": %d, "total": %f, "pct": [%s]}' % (count, total, pct_str)

  def Upgrade(self, state_buffer, version):
    """See base class definition."""
    if version == 0:
      return state_buffer
    else:
      raise ValueError('Unknown version %s' % version)

  def ShouldPrune(self, state_buffer):
    """See base class definition."""
    return False
