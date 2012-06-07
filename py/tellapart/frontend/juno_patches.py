# Copyright 2012 TellApart, Inc.
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

"""
Patches to make Juno do the Right Thing.

To apply, import this module wherever we 'import juno'.
"""

from cStringIO import StringIO

from tellapart.third_party import juno

# Juno has a number of deficiencies, such as the method it uses to build
# response bodies, and how it handles multiple cookies. We fix this by
# sub-classing JunoResponse in TellApartJunoResponse, and monkey-patching the
# new class on top of the original JunoResponse class.

class TellApartJunoResponse(juno.JunoResponse):
  """Replacement for JunoResponse which emulates it's interface, but has
  additional functionality to:
    - Build the response body as a cStringIO instead of string appends.
    - Set headers for multiple cookies correctly

  This class is intended to be monkey patched onto the JunoRequest class in the
  juno module.
  """
  def __init__(self, configuration=None, **kwargs):
    """Matches the JunoResponse API.

    Args:
      configuration - Dictionary of attributes to initialize the response with.
      kwargs - Dictionary of attributes to initialize the response with.
    """
    # Set options and merge in user-set options
    self.config = {
        'body': StringIO(),
        'status': 200,
        'headers': { 'Content-Type': juno.config('content_type') },
        'cookie_header_values': {},}

    # If 'body' is in the initialization parameters, write it into the
    # StringIO object instead of updating self.config with the given value
    init_body = None
    if configuration and 'body' in configuration:
      init_body = configuration['body']
      del configuration['body']

    if 'body' in kwargs:
      init_body = kwargs['body']
      del kwargs['body']

    if init_body:
      init_len = len(init_body)
      self.config['body'].write(init_body)
    else:
      init_len = 0

    if configuration:
      self.config.update(configuration)
    self.config.update(kwargs)
    self.config['headers']['Content-Length'] = init_len

  def append(self, text):
    """Matches the JunoResponse API. Append to the response body.

    Args:
      text - String to append to the response body.
    """
    self.config['body'].write(text)
    self.config['headers']['Content-Length'] += len(text)
    return self

  def header(self, header, value):
    """Matches the JunoResponse API. Set a header value. If the header being set
    is 'Set-Cookie', multiple values are supported.

    Args:
      header - The name of the header to set.
      value - The value to set the header to.
    """
    if header == 'Set-Cookie':
      cookie_name = value.split('=', 1)[0]
      self.config['cookie_header_values'][cookie_name] = value
    else:
      self.config['headers'][header] = value

    return self

  def render(self):
    """Matches the JunoResponse API. Render the response.

    Returns:
      A 3-tuple (status_string, headers, body).
    """
    status_string = '%s %s' % \
        (self.config['status'], self.status_codes[self.config['status']])

    headers = [(k, str(v)) for k, v in self.config['headers'].items()]
    for header_value in self.config['cookie_header_values'].values():
      headers.append(('Set-Cookie', header_value))

    body = self.config['body'].getvalue()
    return (status_string, headers, body)

  def __getattr__(self, attr):
    if attr == 'body':
      return self.config['body'].getvalue()
    else:
      return self.config[attr]

juno.JunoResponse = TellApartJunoResponse

# Even if we configure Juno with 'use_templates' == False, it will still look
# for template files when serving 404s and 500s.  Overwrite two methods to make
# Juno return an empty body string for those two response types.

def NotFound(error='Unspecified error', file=None):
  juno.status(404)
  return juno._response.response
juno.notfound = NotFound

def ServerError(error='Unspecified error', file=None):
  juno.status(500)
  return juno._response.response
juno.servererror = ServerError

def InitializeJuno(mode):
  """Initialize Juno.

  Args:
    mode - A string describing the mode to run Juno in.  Typically, this is
           equivalent to the value of settings.JUNO_MODE.
  """
  juno.init({
    'mode' : mode,

    'use_static' : False,
    'use_db' : False,
    'use_templates' : False,

    # We want to handle exceptions ourselves so we can log them before returning
    # a 500 error.
    'raise_view_exceptions' : True,

    # Don't log Juno messages to stdout.
    'log' : False
  })
