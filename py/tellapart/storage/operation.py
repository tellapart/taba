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

Simple IO Operation descriptors.
"""

class Operation(object):
  def __init__(self, success, response_value=None, traceback=None, retries=0):
    self._success = success
    self.response_value = response_value
    self.traceback = traceback
    self._retries = retries

  @property
  def retries(self):
    return self._retries

  @property
  def success(self):
    return self._success

  def __repr__(self):
    parts = [
        'IO Operation',
        '  success: %s' % self.success]
    if self.response_value:
      parts.append('  response_value: ')
      parts.append(str(self.response_value)[:100])
    if self.traceback:
      parts.append('  traceback:\n')
      parts.append(str(self.traceback))

    return '\n'.join(parts)

class CompoundOperation(Operation):
  def __init__(self):
    self.sub_operations = []
    self.response_value = None

  def AddOp(self, sub_op):
    self.sub_operations.append(sub_op)

  @property
  def success(self):
    if len(self.sub_operations) == 0:
      return True
    else:
      return all([op.success for op in self.sub_operations])

  @property
  def retries(self):
    return sum([op.retries for op in self.sub_operations])

  def __repr__(self):
    lines = ['Compound IO Operation']
    for op in self.sub_operations:
      lines.extend(['  ' + line for line in repr(op).split('\n')])

    return '\n'.join(lines)
