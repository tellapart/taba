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

Utilities for Taba.
"""

from cStringIO import StringIO
import mimetools
import mimetypes
import os
import stat
import sys
import urllib
import urllib2

import cjson

class Bunch(object):
  """A simple container for named fields/values that acts like a class.
  """
  def __init__(self, from_dict=None, **kwargs):
    self.__dict__.update(kwargs)
    if from_dict:
      self.__dict__.update(from_dict)

  def __eq__(self, other):
    return isinstance(other, Bunch) and (self.__dict__ == other.__dict__)

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    import pprint
    return '%s(%s)' % (self.__class__.__name__, pprint.pformat(self.__dict__))

  def __getitem__(self, obj):
    return self.__dict__[obj]

  def __setitem__(self, obj, val):
    self.__dict__[obj] = val

  def __delitem__(self, obj):
    del self.__dict__[obj]

  def __iter__(self):
    return self.__dict__.iterkeys()

  def __contains__(self, key):
    return key in self.__dict__

  def keys(self):
    return self.__dict__.keys()

  def get(self, key):
    return self.__dict__.get(key)

  def ToDict(self):
    """Returns the fields and values as a copy of the underlying dictionary.
    """
    return dict(self.__dict__)

def MakeStreamLogger(
      logger_name,
      stream=sys.stderr,
      format='%(asctime)s - %(levelname)s: %(message)s'):
  """Create a logger that writes to a stream.

  Args:
    logger_name - The hierarchical logger name passed to logging.getLogger().
    stream - The stream object to write to (defaults to sys.stderr).
    format - The format string to use for formatting the log message.

  Returns:
    The created Logger object.
  """
  import logging

  logger = logging.getLogger(logger_name)
  logger.setLevel(logging.INFO)

  if not logger.handlers:
    # Only add the stream handler if there are no handlers already configured
    # for this logger.
    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

  return logger

class HandlerSpec(object):
  """A HTTP request handler specification.

  A HandlerSpec groups a URL pattern, a handler function object, and an
  optional iterable of supported request methods.
  """
  def __init__(self, route_url, handler_func, methods=('get',)):
    """Instantiate a HandlerSpec.
    """
    self.route_url = route_url
    self.handler_func = handler_func
    self.methods = methods

def DecodeSettingsFile(settings_path):
  """Load and parse a JSON encoded settings file.

  Args:
    settings_file - Path to the file containing the settings data.

  Returns:
    Decoded dictionary of settings data.
  """
  # Read in the settings data.
  s = open(settings_path)
  data = s.read()

  if not data:
    raise Exception('No settings data to extract')

  # Decode the settings data.
  settings = cjson.decode(data)

  return settings

def BootstrapCython(id, base_dir='~'):
  """Bootstrap Cython compilation, with a process-specific build directory. The
  specified id will be used to uniquely identify this process. Using id's that
  are durable across restarts makes the bootstrapping process more efficient.

  Args:
    id - ID unique to this process.
  """
  import shutil

  pyx_build_dir = os.path.expanduser('%s/.pyxbld-%s' % (base_dir, id))

  # Clean up stale Cython pyximport state.
  if os.path.exists(pyx_build_dir):
    shutil.rmtree(pyx_build_dir)

  os.makedirs(pyx_build_dir)

  import pyximport
  pyximport.install(build_dir=pyx_build_dir)

###########################################################
# Network Utilities
###########################################################

# How much data to read at a time in GenericFetchFromUrl() and its variants.
_HTTP_READ_BUFFER_SIZE = 1024 * 1024

class Response(object):
  def __init__(self):
    self.body = None
    self.headers = None
    self.status_code = None
    self.final_url = None
    self.exc = None

def GenericFetchFromUrlToString(
      url,
      headers=None,
      post_data=None,
      follow_redirects=True,
      keep_alive=False,
      user_agent=None,
      timeout_secs=None):
  """A more generic form of FetchFromUrlToString().

  This function supports GET/POST requests and returns response headers and
  status codes.

  Args:
    url - The URL from which to fetch.
    headers - A dictionary of request headers, or None.
    post_data - A raw data string, or a dictionary or sequence of two-element
                tuples representing POST data keys/values.  If None (default),
                makes a GET request.  To make a POST request with no data,
                submit a value of {}.  If files are included, they will be sent
                as a multipart message.  For example, if presented with
                {"modify" : "0", "file" : open(filename, "rb")}
                the "modify" parameter will be sent as a POST param, and
                "file" will be uploaded as part of a MIME message.
                This parameter can also be a raw string, in which case it will
                be sent without processing as the POST data.
    follow_redirects - Whether to automatically follow redirects (HTTP 30x).
                       Default: True.
    keep_alive - Whether to set the 'Connection' request header to 'Keep-Alive'.
                 Default: False.
    basic_auth - Whether to use basic HTTP authentication to make the request.
                 If True, the username and password params should be provided.
    user_agent - The user agent to use for this request, Uses the python
                default Python/2.6 string if None
    username - The username used for basic HTTP authentication, if needed.
    password - The password used for basic HTTP authentication, if needed.
    timeout_secs - If present, the number of seconds after which to timeout the
                   request.

  Returns
    A Bunch object representing the response, containing the attributes:
      * body - The HTTP response body string.
      * headers - An httplib.HTTPMessage (dict-like) object containing the
                  response headers.
      * status_code - The HTTP response status code (e.g., 200, 404, etc.).
      * final_url - The real URL of the page.  In some cases, the server may
                    redirect the client to a different URL, so this value may
                    differ from that of the 'url' argument.
  """
  body_buffer = StringIO()
  try:
    result = _GenericFetchFromUrl(
        url,
        body_buffer,
        headers,
        post_data,
        follow_redirects,
        keep_alive=keep_alive,
        user_agent=user_agent,
        timeout_secs=timeout_secs)

    result.body = body_buffer.getvalue()
  finally:
    body_buffer.close()

  return result

def _GenericFetchFromUrl(
      url,
      body_output,
      headers=None,
      post_data=None,
      follow_redirects=True,
      keep_alive=False,
      user_agent=None,
      timeout_secs=None):
  """A generic HTTP fetcher.

  This function supports GET/POST requests and returns response headers and
  status codes.

  Args:
    url - The URL from which to fetch.
    body_output - A file-like object to which the HTTP response body will be
                  written.
    headers - A dictionary of request headers, or None.
    post_data - A raw data string, or a dictionary or sequence of two-element
                tuples representing POST data keys/values.  If None (default),
                makes a GET request.  To make a POST request with no data,
                submit a value of {}.  If files are included, they will be sent
                as a multipart message.  For example, if presented with
                {"modify" : "0", "file" : open(filename, "rb")}
                the "modify" parameter will be sent as a POST param, and
                "file" will be uploaded as part of a MIME message.
                This parameter can also be a raw string, in which case it will
                be sent without processing as the POST data.
    follow_redirects - Whether to automatically follow redirects (HTTP 30x).
                       Default: True.
    keep_alive - Whether to set the 'Connection' request header to 'Keep-Alive'.
                 Default: False.
    basic_auth - Whether to use basic HTTP authentication to make the request.
                 If True, the username and password params should be provided.
    user_agent - The user agent to use for this request, Uses the python
                default Python/2.6 string if None
    username - The username used for basic HTTP authentication, if needed.
    password - The password used for basic HTTP authentication, if needed.
    timeout_secs - If present, the number of seconds after which to timeout the
                   request.

  Returns
    A Bunch object representing the response, containing the attributes:
      * headers - An httplib.HTTPMessage (dict-like) object containing the
                  response headers.
      * status_code - The HTTP response status code (e.g., 200, 404, etc.).
      * final_url - The real URL of the page.  In some cases, the server may
                    redirect the client to a different URL, so this value may
                    differ from that of the 'url' argument.
  """
  result = Response()

  headers = headers or {}

  if user_agent:
    # Add a dummy User Agent as many sites block requests with invalid
    # user agent
    headers['User-Agent'] = user_agent

  try:
    opener = urllib2.build_opener(_MultipartPostHandler)

    request = urllib2.Request(url, post_data, headers)
    f = opener.open(request)
    try:
      result.headers = f.info()
      result.status_code = f.code
      result.final_url = f.geturl()

      _ReadHttpBodyContent(f, body_output)

    finally:
      f.close()
  except urllib2.HTTPError as e:
    result.headers = e.info()
    result.status_code = e.code
    result.final_url = e.geturl()
    result.exc = e

    _ReadHttpBodyContent(e, body_output, catch_http_error=True)

  except IOError as e:
    result.headers = None
    result.status_code = 503
    result.final_url = None
    result.exc = e

  return result

def _ReadHttpBodyContent(input_file, output_file, catch_http_error=False):
  """Read HTTP response body content from 'input_file' in chunks of size
  _HTTP_READ_BUFFER_SIZE.

  Args:
    input_file - A file-like object from which HTTP response body content bytes
                 can be read.
    output_file - A file-like object to which the read body content bytes should
                  be written.
    catch_http_error - If True, silently catch HTTPError exceptions.  This
                       should only be set to True if _ReadHttpBodyContent() is
                       called from within an exception handler for an HTTPError
                       that already occurred. (default: False)
  """
  try:
    while True:
      body_chunk = input_file.read(_HTTP_READ_BUFFER_SIZE)
      if body_chunk:
        output_file.write(body_chunk)
      if len(body_chunk) < _HTTP_READ_BUFFER_SIZE:
        break
  except urllib2.HTTPError, e:
    if not catch_http_error:
      raise e

class _MultipartPostHandler(urllib2.BaseHandler):
  """A subclass of urllib2.BaseHandler that allows for the use of multipart
  form-data to POST files to a remote server.  This handler also supports
  generic POST key-value pairs.  If there are no values to be included, then
  a GET request will be performed instead.
  """

  # BaseHandler subclasses can change the handler_order member variable to
  # modify its position in the handler list.  The post handler should run before
  # other default handlers.
  handler_order = urllib2.HTTPHandler.handler_order - 1

  def http_request(self, request):
    """Override the http_request() processing.  Retrieve the data and files to
    be posted, and create the data blob to send to the server.

    The necessary Content-Type headers will also be added.
    """
    data = request.get_data()
    if data is not None and type(data) == dict:
      v_files = []
      v_vars = []
      try:
        for key, value in data.iteritems():
          if type(value) == file:
            v_files.append((key, value))
          else:
            v_vars.append((key, value))
      except TypeError:
        _, value, traceback = sys.exc_info()
        raise TypeError(
            "not a valid non-string sequence or mapping object",
            traceback)

      if len(v_files) == 0:
        data = urllib.urlencode(v_vars)
      else:
        boundary, data = self._MultipartEncode(v_vars, v_files)
        contenttype = 'multipart/form-data; boundary=%s' % boundary
        request.add_unredirected_header('Content-Type', contenttype)

      request.add_data(data)

    elif data is not None and isinstance(data, basestring):
      request.add_data(data)

    return request

  https_request = http_request

  def _MultipartEncode(self, vars, files, boundary=None, buf=None):
    """Given the provided POST variables, and files, will encode the data
    as a multipart message if files is present.

    Args:
      vars - List of POST keys that should be sent in request
      files - List of files that should be sent in the request
      boundary - Boundary to be used to send the MIME request
      buf - Buffer to use to construct the message data

    Returns:
      Tuple of the chosen boundary and the buffer containing the message data.
    """
    if boundary is None:
      boundary = mimetools.choose_boundary()

    if buf is None:
      buf = StringIO()

    for key, value in vars:
      buf.write('--%s\r\n' % boundary)
      buf.write('Content-Disposition: form-data; name="%s"' % key)
      buf.write('\r\n\r\n' + value + '\r\n')

    for key, fd in files:
      os.fstat(fd.fileno())[stat.ST_SIZE]
      filename = fd.name.split('/')[-1]
      contenttype = mimetypes.guess_type(filename)[0] or \
          'application/octet-stream'
      buf.write('--%s\r\n' % boundary)
      buf.write('Content-Disposition: form-data; name="%s"; filename="%s"\r\n' \
          % (key, filename))
      buf.write('Content-Type: %s\r\n' % contenttype)
      fd.seek(0)
      os.fstat(fd.fileno())[stat.ST_SIZE]

      buf.write('\r\n' + fd.read() + '\r\n')

    buf.write('--' + boundary + '--\r\n\r\n')
    buf = buf.getvalue()
    return boundary, buf
