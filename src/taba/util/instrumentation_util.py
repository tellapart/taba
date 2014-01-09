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

"""Utilities for instrumenting blocks of code for Tabs.
"""

import math
import random
import time

from taba import client

class Timer(object):
  """Context class used to instrument arbitrary blocks of code. Records a Tab
  for the time elapsed within the context, and for the number of times  exited
  with an Exception. Also, yields on exit.
  """

  def __init__(self, label, use_millis=True, do_yield=True):
    """
    Args:
      label - Prefix for all Tabs recorded by this object.
      use_millis - If True, use milliseconds. Otherwise, use seconds.
      do_yield - If True, perform a gevent yield on exit.
    """
    self.label = label
    self.use_millis = use_millis
    self.do_yield = do_yield

    self.counter_exceptions = client.Tab('%s_exceptions' % self.label)

    pattern = '%s_time_ms' if self.use_millis else '%s_time'
    self.counter_elapsed = client.Tab(pattern % self.label)

    self.elapsed = None

  def __enter__(self):
    self.start_time_sec = time.time()
    return self

  def __exit__(self, typ, value, traceback):
    # Calculate and record the elapsed time.
    self.elapsed = time.time() - self.start_time_sec
    if self.use_millis:
      self.elapsed *= 1000.0

    self.counter_elapsed.RecordValue(self.elapsed)

    # Record whether we exited with an exception.
    if value is not None:
      self.counter_exceptions.RecordValue(1)

    # Be friendly.
    if self.do_yield:
      time.sleep(0)

    # Don't swallow exceptions.
    return None

  @property
  def Start(self):
    return self.start_time_sec

  @property
  def Elapsed(self):
    return self.elapsed

  @property
  def ElapsedMs(self):
    if self.use_millis:
      return self.elapsed
    else:
      return self.elapsed * 1000.0

  @property
  def ElapsedSec(self):
    if self.use_millis:
      return self.elapsed / 1000.0
    else:
      return self.elapsed

class Sampler(object):
  """Time bucketed reservoir sampler.
  """

  def __init__(self, bucket_width, total_time, samples_per_bucket=1000):
    """
    Args:
      bucket_width - Time slice in seconds of each independent set of samples.
      total_time - Time slice in seconds for the entire window.
      samples_per_bucket - Number of samples to keep for each time slice.
    """
    self.bucket_width = bucket_width
    self.num_buckets = math.ceil(total_time / bucket_width)
    self.samples = samples_per_bucket

    self.sample_buckets = [[]]
    self.current_bucket = time.time()

  def AddValue(self, value, now=None):
    """Add a value to the sampling stream.

    Args:
      value - Value to sample.
      now - Optionally pass a pre-determined value for the current timestamp
        (like the output of time.time()). Meant as an optimization to avoid
        making a redundant system call.
    """
    # Optimization to avoid calling time.time() system call repeatedly.
    if now is None:
      now = time.time()

    # Check if the current bucket needs rotating.
    while (self.current_bucket + self.bucket_width) < now:
      if len(self.sample_buckets) < self.num_buckets:
        self.sample_buckets = [[]] + self.sample_buckets
      else:
        self.sample_buckets = [[]] + self.sample_buckets[:-1]

      self.current_bucket += self.bucket_width

    # Put the next sample into the bucket.
    samples = self.sample_buckets[0]
    if len(samples) < self.samples:
      samples.append(value)
    else:
      index = random.randint(0, self.samples - 1)
      samples[index] = value

    self.sample_buckets[0] = samples

  def GetPercentile(self, percentile=0.50):
    """Retrieve the sampled value for a percentile. To get the average, use a
    percentile value of 0.5.

    Args:
      percentile - Sampled value percentile to retrieve. Must be a value
          between 0 and 1.

    Returns:
      Sampled value for the given percentile.
    """
    if percentile < 0.0 or percentile > 1.0:
      raise ValueError('Percentile must be between 0 and 1')

    # Combine all the samples into one list, and sort them.
    all_samples = [item for sublist in self.sample_buckets for item in sublist]
    all_samples = sorted(all_samples)

    if len(all_samples) == 0:
      return 0

    # Take an index based on the percentile desired.
    index = int((len(all_samples) - 1) * percentile)
    return all_samples[index]
