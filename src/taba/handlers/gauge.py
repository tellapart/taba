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

"""Tab Handler that accepts String inputs, and tracks the latest value set.
"""

from taba.handlers.expiry_gauge import ExpiryGauge
from taba.common.transport import Event

# Period after which if a Gauge hasn't been updated, it will be marked for
# pruning.
PRUNE_TIMEOUT = 60 * 60 * 48    # 48 hours

class Gauge(ExpiryGauge):
  """Tab Handler that accepts String inputs, and tracks the latest value set.
  """
  CURRENT_VERSION = 0

  def FoldEvents(self, state, events):
    """See base class definition."""
    wrapped_events = [
        Event(
            timestamp=e.timestamp,
            payload=(
                e.payload[0] if type(e.payload) in (tuple, list) else e.payload,
                e.timestamp + PRUNE_TIMEOUT))
        for e in events]

    return super(Gauge, self).FoldEvents(state, wrapped_events)

  def Upgrade(self, state, version):
    """See base class definition."""
    if version == 0:
      return state
    else:
      raise ValueError('Unknown version %s' % version)
