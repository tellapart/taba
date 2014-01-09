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

"""Enumeration of Tab Types.
"""

class TabType(object):
  """Enum of all the Tab Type IDs"""
  BUFFER = "Buffer"

  GAUGE = "Gauge"
  EXPIRY_GAUGE = "ExpiryGauge"

  TOTALS_COUNTER = "TotalsCounter"
  PERCENTILE_COUNTER = "PercentileCounter"

  COUNTER_GROUP = "CounterGroup"
  PERCENTILE_GROUP = "PercentileGroup"

class RenderMode(object):
  """Enumeration of known MIME types for rendering."""
  JSON = 'application/json'
  TEXT = 'text/plain'
  DETAIL = 'application/json-detailed'

  PYTHON_OBJ = 'application/python-obj'
  DEFAULT = JSON
