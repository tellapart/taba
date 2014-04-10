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

"""Trivial example of using the Taba Python client in threaded mode."""

from taba import client

# Start the Taba client, giving it a name and the URL of the local Taba Agent.
client.Initialize(
    client_id='example_client',
    event_post_url='http://localhost:8279/post',
    flush_period=1)

# Record some test data.
client.Counter('example_moving_counter', 100)
client.Counter('example_moving_counter', 1000)

client.Gauge('example_gauge', 'value')

# Make sure to stop background processing before exiting.
client.Stop()
