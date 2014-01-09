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

"""Data validation helpers.
"""

import re

# URL Valid, minus (#&,'[])
_ALLOWED_CHARS = 'A-Za-z0-9._~:\/?@!$()*+;=-'

_DISALLOWED_PATTERN = re.compile('[^%s]' % _ALLOWED_CHARS)

def IsNameValid(name):
  """Return whether a Tab Name is valid.

  Args:
    name - Tab Name to validate.

  Returns:
    Whether the Name is valid.
  """
  if _DISALLOWED_PATTERN.search(name) is not None:
    return False

  return True

def IsClientValid(client):
  """Return whether a Client ID is valid.

  Args:
    client - Client ID to validate.

  Returns:
    Whether the Client ID is valid.
  """
  if _DISALLOWED_PATTERN.search(client) is not None:
    return False

  return True

def Sanitize(value):
  """Replace any characters disallowed in a Tab or Client Name with '_'

  Args:
    value - String to sanitize.

  Returns:
    Original string with any disallowed characters replaced with '_'
  """
  return re.sub('[^%s]' % _ALLOWED_CHARS, '_', value)