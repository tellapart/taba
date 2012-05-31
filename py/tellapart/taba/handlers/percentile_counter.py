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

Taba Handler that accepts numeric input, and tracks the count and total, and
samples the values to generate cumulative distribution percentiles.
"""

from itertools import izip

from tellapart.taba.handlers.taba_handler import TabaHandler

from tellapart.taba.handlers import percentile_counter_native

class PercentileCounter(TabaHandler):
  """Taba Handler that accepts numeric input, and tracks the count and total,
  and samples the values to generate cumulative distribution percentiles.
  """
  CURRENT_VERSION = 1

  def NewState(self, client_id, name):
    """See base class definition.
    """
    return percentile_counter_native.NewState()

  def FoldEvents(self, state, events):
    """See base class definition.
    """
    return percentile_counter_native.FoldEvents(state, events)

  def ProjectState(self, state):
    """See base class definition.
    """
    return percentile_counter_native.ProjectState(state)

  def Aggregate(self, projections):
    """See base class definition.
    """
    count = sum([p['count'] for p in projections])
    total = sum([p['total'] for p in projections])
    percentiles = self._AggregatePercentiles(projections)

    aggregate = {
        'count': count,
        'total': total,
        'average': float(total) / count if count != 0 else 0,
        'percentiles': percentiles,}

    return aggregate

  def Render(self, name, projections):
    """See base class definition.
    """
    renders = []
    for p in projections:
      pct_str = ', '.join([
          '(%.2f, %.2f)' % (pct, val)
          for pct, val
          in izip(percentile_counter_native.PERCENTILES, p['percentiles'])])

      renders.append(
          '%s\t%.2f\t%d\t[%s]' % (name, p['total'], p['count'], pct_str))

    return renders

  @staticmethod
  def _AggregatePercentiles(projections):
    """See base class definition.
    """
    count = sum([p['count'] for p in projections])
    total = sum([p['total'] for p in projections])

    weighted_percentiles = [0.0] * len(percentile_counter_native.PERCENTILES)

    if total == 0.0:
      # Avoid division-by-zero.
      return weighted_percentiles

    for p in projections:
      for i, percentile_value in enumerate(p['percentiles']):
        weight = float(p['count']) / count
        weighted_percentiles[i] += percentile_value * weight

    return weighted_percentiles

  def Upgrade(self, state, version):
    """See base class definition.
    """
    if version == 0:
      return state

    elif version == 1:
      return state

    else:
      raise ValueError('Unsupported state version %d' % version)

  def ShouldPrune(self, state):
    """See base class definition.
    """
    return False
