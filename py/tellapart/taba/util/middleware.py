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

Middleware handlers for Taba.
"""

import cProfile as profile
from cStringIO import StringIO
import os

class CircumventJunoInputParsing(object):
  """WSGI middleware that copies the 'wsgi.input' file-like object to
  'request_body_bytes' so that it can be read directly, instead of having Juno
  exhaust the file by parsing it into fields.
  """
  def __init__(self, application):
    self.application = application

  def __call__(self, environ, start_response):
    environ['request_body_bytes'] = environ['wsgi.input']
    environ['wsgi.input'] = StringIO()

    return self.application(environ, start_response)

class ProfileRequests(object):
  """WSGI middleware that profiles requests with cProfile for CPU usage. Due to
  the overhead, it only runs every 100 requests, and is disabled by default.
  """
  def __init__(self, application):
    self.application = application
    self.request_counter = 0
    if not os.path.exists('/mnt/log/profiler'):
      os.mkdir('/mnt/log/profiler')
    self.run_every = 100

  def __call__(self, environ, start_response):
    self.request_counter += 1
    if self.request_counter % self.run_every == 0:
      _locals = locals()
      profiler = profile.Profile()
      profiler.runctx(
          'returned_value = self.application(environ,start_response)',
          globals(), _locals)
      output_path = os.path.join('/mnt/log/profiler',
          'prof_' + str(self.request_counter / self.run_every))
      if os.path.exists(output_path):
        os.remove(output_path)
      profiler.dump_stats(output_path)
      return _locals['returned_value']
    return self.application(environ, start_response)
