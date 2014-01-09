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

"""Middlewares
"""

import cProfile as profile
from cStringIO import StringIO
import os

import gevent
import requests

class DoubleAgent(object):
  """WSGI middleware for the Taba Agent which duplicates all requests to a
  second Agent process. Useful for bootstrapping a new cluster without
  affecting an existing production one.
  """
  def __init__(self, application, port):
    self.application = application
    self.second_port = port

  def __call__(self, environ, start_response):
    path = environ['PATH_INFO']

    query = ''
    if environ['QUERY_STRING']:
      query = '?%s' % environ['QUERY_STRING']

    post_data = None
    if environ['REQUEST_METHOD'] == 'POST':
      post_data = ''.join(environ['wsgi.input'])
      environ['wsgi.input'] = StringIO(post_data)

    gevent.spawn(
        requests.post,
        'http://localhost:%d%s%s' % (self.second_port, path, query),
        post_data=post_data)

    return self.application(environ, start_response)

class ProfileRequests(object):
  """WSGI middleware that profiles requests with cProfile. Due to the overhead,
  it only runs every 100 requests.
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
      profiler.runctx('returned_value = self.application(environ,'
                      'start_response)', globals(), _locals)
      output_path = os.path.join('/mnt/log/profiler',
          'prof_' + str(self.request_counter / self.run_every))
      if os.path.exists(output_path):
        os.remove(output_path)
      profiler.dump_stats(output_path)
      return _locals['returned_value']
    return self.application(environ, start_response)
