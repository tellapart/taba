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

"""Native implementation of a multiple-interval moving counters.
"""

from libc.stdlib cimport calloc, free
from libc.string cimport memcpy, memmove, memset

cdef inline double double_max(double a, double b): return a if a >= b else b

###########################################################
# Struct Definitions
###########################################################

# C struct for holding a single time-limited bucket of summed values.
cdef struct Bucket:

  # Count of values recorded into this time bucket.
  unsigned long long count

  # Sum of all values recorded into this time bucket.
  long double value

# C struct for holding a time interval consisting of several time slice buckets.
# Buckets are rotated off as the current time moves forward.
cdef struct MovingIntervalState:

  # The number of buckets in this interval.
  unsigned int bucket_count

  # The number of seconds each bucket represents.
  unsigned int bucket_width

  # UTC timestamp of the start of the last bucket. Time increases as the index
  # into the buckets array increases. e.g. the last bucket on the array
  # represents UTC values
  # (newest_bucket_start --> newest_bucket_start + bucket_width].
  double newest_bucket_start

  # Pointer to the actual array of Buckets.
  Bucket* buckets

# C struct for holding the top level group of intervals.
cdef struct PyramidState:

  # The number of intervals in this state.
  unsigned int num_intervals

  # Pointer to the array of intervals.
  MovingIntervalState* intervals

###########################################################
# State creation and serialization
###########################################################

cdef object SerializeState(PyramidState* py_state):
  """Serialize a PyramidState struct into a Python binary string.

  Args:
    py_state - Pointer to the PyramidState struct to serialize.

  Returns:
    Python binary string.
  """
  cdef int i
  cdef unsigned int buffer_size = 0
  cdef unsigned int part_size

  # Calculate the necessary size of the serialization buffer.
  buffer_size += sizeof(PyramidState) - sizeof(MovingIntervalState*)
  buffer_size += \
      (sizeof(MovingIntervalState) - sizeof(Bucket*)) * py_state.num_intervals

  for i in range(py_state.num_intervals):
    buffer_size += sizeof(Bucket) * py_state.intervals[i].bucket_count

  # Allocate a buffer and serialize the PyramidState into it.
  cdef char* buffer = <char*> calloc(1, buffer_size)
  cdef char* cursor = buffer

  # Copy the top PyramidState state (minus the intervals pointer).
  part_size = sizeof(PyramidState) - sizeof(MovingIntervalState*)
  memcpy(cursor, py_state, part_size)
  cursor += part_size

  for i in range(py_state.num_intervals):
    # Copy the next MovingIntervalState struct (minus the Bucket array pointer).
    part_size = sizeof(MovingIntervalState) - sizeof(Bucket*)
    memcpy(cursor, &py_state.intervals[i], part_size)
    cursor += part_size

    # Copy the next Bucket array.
    part_size = sizeof(Bucket) * py_state.intervals[i].bucket_count
    memcpy(cursor, py_state.intervals[i].buckets, part_size)
    cursor += part_size

  # Convert the buffer to a Python object and free the buffer,
  obj_buffer = buffer[:buffer_size]
  free(buffer)

  return obj_buffer

cdef void* DeallocateState(PyramidState* py_state):
  """Free the memory allocated for a PyramidState.

  Args
    py_state - PyramidState pointer to deallocate.
  """
  cdef int i
  for i in range(py_state.num_intervals):
    free(py_state.intervals[i].buckets)
  free(py_state.intervals)
  free(py_state)

def NewStateBuffer(interval_specs, int now):
  """Create a new, blank PyramidState buffer.

  Args:
    interval_specs - List of tuples of (bucket_width, number of buckets).
    now - Current UTC timestamp.

  Returns:
    Python binary string.
  """
  cdef PyramidState* py_state

  # Allocate and initialize the PyramidState object and its array of
  # MovingIntervalState objects.
  py_state = <PyramidState*> calloc(1, sizeof(PyramidState))
  py_state.num_intervals = len(interval_specs)
  py_state.intervals = <MovingIntervalState*> calloc(
      py_state.num_intervals,
      sizeof(MovingIntervalState))

  # Initialize each MovingIntervalState, and allocate its Bucket array.
  cdef unsigned int width, count, i
  for i in range(len(interval_specs)):
    width, count = interval_specs[i]

    py_state.intervals[i].bucket_width = width
    py_state.intervals[i].bucket_count = count

    # Round the 'now' value down to the nearest bucket boundary. This makes
    # sure that all MovingIntervalState objects with the same bucket width have
    # the same bucket boundaries. (Needed to properly Reduce States).
    py_state.intervals[i].newest_bucket_start = (now / width) * width

    py_state.intervals[i].buckets = <Bucket*> calloc(
        py_state.intervals[i].bucket_count,
        sizeof(Bucket))

  buffer = SerializeState(py_state)

  # Free all the allocated memory.
  DeallocateState(py_state)

  return buffer

###########################################################
# State Management Class
###########################################################

cdef class PyramidCounterState(object):
  """Wrapper class to provide a Python interface to native manupulations of
  PyramidState objects.
  """

  cdef PyramidState* state

  def __cinit__(self, char* buffer):
    cdef unsigned int i, part_size
    cdef char* cursor = buffer

    # Allocate the root PyramidState and copy in its non-pointer values.
    self.state = <PyramidState*>calloc(1, sizeof(PyramidState))
    part_size = sizeof(PyramidState) - sizeof(MovingIntervalState*)
    memcpy(self.state, cursor, part_size)
    cursor += part_size

    # Allocate and initialize the MovingIntervalState array.
    self.state.intervals = <MovingIntervalState*> calloc(
        self.state.num_intervals,
        sizeof(MovingIntervalState))

    for i in range(self.state.num_intervals):
      # Copy in the non-pointer values in the next MovingIntervalState.
      part_size = sizeof(MovingIntervalState) - sizeof(Bucket*)
      memcpy(&self.state.intervals[i], cursor, part_size)
      cursor += part_size

      # Allocate and copy in the next Bucket array.
      self.state.intervals[i].buckets = <Bucket*> calloc(
          self.state.intervals[i].bucket_count,
          sizeof(Bucket))

      part_size = self.state.intervals[i].bucket_count * sizeof(Bucket)
      memcpy(self.state.intervals[i].buckets, cursor, part_size)
      cursor += part_size

  def __init__(self, buffer):
    pass

  def __dealloc__(self):
    """Deallocate any dynamically allocated memory.
    """
    DeallocateState(self.state)

  def Serialize(self):
    """Serialize and return the wrapped PyramidState struct.

    Returns:
      Python binary string.
    """
    return SerializeState(self.state)

  def FoldEvents(self, events):
    """Fold an iterable of Event objects into this state.

    NOTE: Before folding events, RotateBucketsIfNeeded() should usually be
    called with the current UTC timestamp (or the effective current UTC
    timestamp adjusted by the cluster applied latency. When using an adjusted
    timestamp, make sure to also consider the timestamps on the events to be
    folded.)

    Args:
      events - Iterable of Event objects.
    """
    cdef int i, index
    cdef float value
    cdef MovingIntervalState* mis

    for event in events:
      for i in range(self.state.num_intervals):
        mis = &self.state.intervals[i]
        index = _GetIndex(event.timestamp, mis)
        if index >= 0:    # Don't consider events that missed the interval.
          if type(event.payload) in (tuple, list):
            value = event.payload[0]
          else:
            value = event.payload

          mis.buckets[index].count += 1
          mis.buckets[index].value += value

  def Reduce(self, others):
    """Reduce a group of PyramidCounterState objects into this one.

    NOTE: All the input PyramidCounterState objects _must_ have been created
    with the exact same interval_spec parameter. They must also all be rotated
    to the same start timestamp over all intervals.

    Args:
      others - Iterable of PyramidCounterState objects.
    """
    cdef int i, j
    cdef PyramidCounterState p_other
    cdef MovingIntervalState *this_mis, *that_mis

    for other in others:
      p_other = <PyramidCounterState>other

      for i in range(self.state.num_intervals):
        this_mis = &self.state.intervals[i]
        that_mis = &p_other.state.intervals[i]

        for j in range(this_mis.bucket_count):
          this_mis.buckets[j].count += that_mis.buckets[j].count
          this_mis.buckets[j].value += that_mis.buckets[j].value

  def Render(self):
    """Render this state.

    Returns:
      List of lists of Tuples, where each element in the outer list corresponds
      to an interval, the second level lists correspond to Buckets in the
      respective interval, and the Tuples are of the form:
         (Bucket Start Timestamp, Count, Total).
    """
    cdef int i, j
    cdef int offset_time
    cdef MovingIntervalState* mis

    intervals = []

    for i in range(self.state.num_intervals):
      mis = &self.state.intervals[i]
      mis_data = []

      for j in range(mis.bucket_count):
        offset_time = mis.bucket_width * (mis.bucket_count - 1 - j)
        mis_data.append((
            mis.newest_bucket_start - offset_time,
            mis.buckets[j].count,
            mis.buckets[j].value))

      intervals.append(mis_data)

    return intervals

  def RotateBucketsIfNeeded(self, double now):
    """Rotate out expired Buckets, if any, based on the given UTC timestamp.

    Args:
      now - UTC timestamp of the current (or effective) time.
    """
    cdef int i
    cdef int discard
    cdef MovingIntervalState* mis

    for i in range(self.state.num_intervals):
      mis = &self.state.intervals[i]

      if now >= mis.newest_bucket_start + mis.bucket_width:
        discard = ((<int>(now - mis.newest_bucket_start)) / mis.bucket_width)
        _RotateBuckets(mis, discard)
        mis.newest_bucket_start += <double>(<int>mis.bucket_width * discard)

  def GetMaxStart(self):
    """Retrieve the most recent Bucket start, across all intervals.

    Returns:
      UTC timestamp.
    """
    cdef int i
    cdef double max_start = 0
    for i in range(self.state.num_intervals):
      if max_start < self.state.intervals[i].newest_bucket_start:
        max_start = self.state.intervals[i].newest_bucket_start

    return max_start

  def __repr__(self):
    cdef int i, j
    cdef MovingIntervalState* mis

    interval_parts = []
    for i in range(self.state.num_intervals):
      mis = &self.state.intervals[i]
      bucket_parts = []
      for j in range(mis.bucket_count):
        bucket_parts.append('%s(%d,%d)' % (
            mis.newest_bucket_start - (j * mis.bucket_width),
            mis.buckets[j].count,
            mis.buckets[j].value))

      interval_parts.append('<%d @ %ds: %s>' % (
          mis.bucket_count, mis.bucket_width, ' '.join(bucket_parts)))

    return "Pm(%d) [%s]" % (
        self.state.num_intervals, ', '.join(interval_parts))

def SynchronizeCounters(counters, now):
  """Rotate a group of PyramidCounterState objects to the same timestamp.

  Args:
    counters - Iterable of PyramisCounterState objects.
    now - Current (effective) UTC timestamp.
  """
  cdef int i
  cdef double max_start

  max_start = <double>now
  for counter in counters:
    max_start = double_max(max_start, counter.GetMaxStart())

  for counter in counters:
    counter.RotateBucketsIfNeeded(max_start)

###########################################################
# Helpers
###########################################################

cdef inline int _GetIndex(double timestamp, MovingIntervalState* state):
  """Calculate the index into the Bucket array of a MovingIntervalState struct
  for a given timestamp.

  Args:
    timestamp - UTC timestamp to calculate the index for.
    state - MovingIntervalState struct pointer to calculate the index into,

  Returns:
    Integer index.
  """
  return (
      (<int>(timestamp - state.newest_bucket_start)) / <int>state.bucket_width
      ) + state.bucket_count - 1

cdef inline void _RotateBuckets(
    MovingIntervalState* state,
    int buckets_to_discard):
  """Discard a number of expired buckets from a MovingIntervalState object.
  Modifies the bucket array in-place.

  Args:
    state - MovingIntervalState object to update.
    buckets_to_discard - The number of buckets to discard.
  """
  cdef unsigned int bytes_to_copy, bytes_to_discard
  cdef char* start
  cdef size_t element_size = sizeof(Bucket)

  if buckets_to_discard >= state.bucket_count:
    bytes_to_copy = 0
    bytes_to_discard = state.bucket_count * element_size
  else:
    bytes_to_copy = (state.bucket_count - buckets_to_discard) * element_size
    bytes_to_discard = buckets_to_discard * element_size

  start = <char*>state.buckets
  memmove(start, start + bytes_to_discard, bytes_to_copy)
  memset(start + bytes_to_copy, 0, bytes_to_discard)
