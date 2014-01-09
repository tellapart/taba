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

"""Storage related utilities.
"""

import logging
import traceback

LOG = logging.getLogger(__name__)

def CheckedOp(description, engine_fn, *engine_fn_args):
  """Execute an operation which returns an Operation object, and check it for
  success or exceptions, with logging in the case of an error.

  Args:
    description - String describing the operation which will be appended to
        error messages.
    engine_fn - Callback to execute, which return an Operation object.
    engine_fn_args - List of arguments to bass to engine_fn

  Returns:
    Operation object with the query results.
  """
  return _CheckedOp(False, description, engine_fn, engine_fn_args)

def StrictOp(description, engine_fn, *engine_fn_args):
  """Execute an operation which returns an Operation object, and check it for
  success or exceptions, with logging in the case of an error. If the operation
  did not succeed, raise an Exception.

  Args:
    description - String describing the operation which will be appended to
        error messages.
    engine_fn - Callback to execute, which return an Operation object.
    engine_fn_args - List of arguments to bass to engine_fn

  Returns:
    Operation object with the query results.
  """
  return _CheckedOp(True, description, engine_fn, engine_fn_args)

def _CheckedOp(strict, description, engine_fn, engine_fn_args):
  """Internal Checked Operation implementation.

  Args:
    strict - If True, raise an exception on Operation failure.
    description - String describing the operation which will be appended to
        error messages.
    engine_fn - Callback to execute, which return an Operation object.
    engine_fn_args - List of arguments to bass to engine_fn

  Returns:
    Operation object with the query results.
  """
  try:
    op = engine_fn(*engine_fn_args)

    if not op.success:
      message = "Error %s" % description
      LOG.error(message)
      LOG.error(op)
      if strict:
        raise DatabaseException(message, op=op)

  except Exception as e:
    message = "Exception %s" % description
    LOG.error(message)
    LOG.error(traceback.format_exc())
    op = Operation(success=False, traceback=traceback.format_exc())
    if strict:
      raise DatabaseException(message, inner=e, op=op)

  return op

class DatabaseException(Exception):
  def __init__(self, message, inner=None, op=None):
    self.op = op
    super(DatabaseException, self).__init__(message, inner)

class Operation(object):
  def __init__(self, success, response_value=None, traceback=None, retries=0,
               exc=None, message=None):
    self._success = success
    self.response_value = response_value
    self.traceback = traceback
    self._retries = retries
    self.exc = exc
    self.message = message

  @property
  def retries(self):
    return self._retries

  @property
  def success(self):
    return self._success

  def _GetReprParts(self):
    parts = [
        'Operation',
        '  success: %s' % self.success]
    if self.response_value:
      parts.append('  response_value: ')
      parts.append(str(self.response_value)[:100])
    if self.traceback:
      parts.append('  traceback:\n')
      parts.append(str(self.traceback))
    if self.exc:
      parts.append('  exception:\n')
      parts.append(str(self.exc))
    return parts

  def __repr__(self):
    parts = self._GetReprParts()
    return '\n'.join(parts)

class CompoundOperation(Operation):
  def __init__(self, *sub_ops):
    self.sub_operations = list(sub_ops)
    self.response_value = None
    self._explicit_success = None

  def AddOp(self, sub_op):
    self.sub_operations.append(sub_op)

  @property
  def success(self):
    # If self._explicit_success is set, return it.
    if self._explicit_success is not None:
      return self._explicit_success
    if len(self.sub_operations) == 0:
      return True
    else:
      return all([op.success for op in self.sub_operations])

  @success.setter
  def success(self, bool_value):
    self._explicit_success = bool_value

  @property
  def retries(self):
    return sum([op.retries for op in self.sub_operations])

  def __repr__(self):
    lines = ['Compound IO Operation']
    for op in self.sub_operations:
      lines.extend(['  ' + line for line in repr(op).split('\n')])

    return '\n'.join(lines)
