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

Native implementation of the core Moving Interval Counter state operations.
"""

from libc.stdlib cimport malloc, calloc, free, qsort
from libc.string cimport memcpy, memset

cdef extern from "math.h" nogil:
  double ceil(double)

cdef inline int int_min(int a, int b): return a if a <= b else b

# C struct for holding the persistent state fields for MovingIntervalCounter in
# a flat memory layout, suitable for direct serialization.
cdef struct MicStateStruct:
  int     num_buckets
  int     total_interval_secs
  double  oldest_bucket
  int*    count_buckets
  float* value_buckets

cdef object SerializeMicStateStruct(MicStateStruct* state):
  """Serialize a MicStateStruct to a Python binary string.

  Args:
    state - Pointer to a MicStateStruct to be serialized.

  Returns:
    A Python binary string representing the state.
  """

  cdef char* state_bytes = <char*>state
  cdef char* count_bytes = <char*>(state.count_buckets)
  cdef char* value_bytes = <char*>(state.value_buckets)

  cdef unsigned int meta_size = 2 * sizeof(int) + sizeof(double)
  cdef unsigned int count_size = state.num_buckets * sizeof(int)
  cdef unsigned int value_size = state.num_buckets * sizeof(float)

  buff_meta = state_bytes[:meta_size]
  buff_count = count_bytes[:count_size]
  buff_value = value_bytes[:value_size]

  return buff_meta + buff_count + buff_value

def MakeMicStateBuffer(int total_interval_secs, int num_buckets, double now):
  """Create a blank serialized MicStateStruct.

  Args:
    total_interval_secs - The total time interval of the MovingItervalCounter
        (in seconds).
    num_buckets - The number of buckets to use for tracking the moving interval.
    now - The UTC timestamp to initialize the buckets for.

  Returns:
    A Python binary string containing the serialized MisStateStruct initialized
    with the given parameters.
  """
  # Allocate a MicStateStruct object.
  cdef MicStateStruct* state = <MicStateStruct*> malloc(sizeof(MicStateStruct))
  memset(state, 0, sizeof(MicStateStruct))

  # Initialize the meta-data fields
  state.num_buckets = num_buckets
  state.total_interval_secs = total_interval_secs
  state.oldest_bucket = now - state.total_interval_secs

  # Allocate the count and value buckets.
  state.count_buckets = <int*> calloc(state.num_buckets, sizeof(int))
  state.value_buckets = <float*> calloc(state.num_buckets, sizeof(float))

  # Serialize the initialized state.
  serialized = SerializeMicStateStruct(state)

  # Deallocate the MicState object, now that we have the serialized version.
  free(state.count_buckets)
  free(state.value_buckets)
  free(state)

  return serialized

cdef class MovingIntervalCounterState(object):
  """Native class for handling core manipulations of state objects for the
  MovingIntervalCounter handler.

  Note: this class must be initialized with a state buffer. To create a new
  MovingIntervalCounterState object, use MakeMicStateBuffer(...).
  """

  cdef MicStateStruct* state

  cdef float bucket_interval

  def __cinit__(self, char* buff):
    """C-Level Initialization
    """
    # Allocate the state structure.
    cdef MicStateStruct* state = <MicStateStruct*>malloc(sizeof(MicStateStruct))
    memset(state, 0, sizeof(MicStateStruct))

    # Copy num_buckets, total_interval_secs, and oldest_bucket form the buffer.
    cdef int meta_size = 2 * sizeof(int) + sizeof(double)
    cdef void* meta_offset = <void*>buff
    memcpy(state, meta_offset, meta_size)

    # Initialize the count buckets array and copy the values from the buffer.
    cdef int count_size = state.num_buckets * sizeof(int)
    cdef void* count_offset = meta_offset + meta_size
    state.count_buckets = <int*> calloc(state.num_buckets, sizeof(int))
    memcpy(state.count_buckets, count_offset, count_size)

    # Initialize the value buckets array and copy the values from the buffer.
    cdef int value_size = state.num_buckets * sizeof(float)
    cdef void* value_offset = count_offset + count_size
    state.value_buckets = <float*> calloc(state.num_buckets, sizeof(float))
    memcpy(state.value_buckets, value_offset, value_size)

    self.state = state

  def __init__(self, buffer):
    """
    Args:
      buffer - Python binary string containing a serialized MicStateStruct.
    """
    self.bucket_interval = \
        <float>self.state.total_interval_secs / self.state.num_buckets

  def __dealloc__(self):
    """Free the state allocated in __cinit__()
    """
    free(self.state.count_buckets)
    free(self.state.value_buckets)
    free(self.state)

  def Serialize(self):
    """Serialize this object to a binary string.

    Returns:
      A Python binary string.
    """
    return SerializeMicStateStruct(self.state)

  def FoldSingleEvent(self, double event_time, float value):
    """Fold a single Taba Event into this state.

    Args:
      event_time - The UTC time of the event.
      value - The value recorded for the event.
    """
    cdef int index = self._GetIndex(event_time)
    if index >= 0:
      self.state.count_buckets[index] += 1
      self.state.value_buckets[index] += value

  def Project(self):
    """Generate a projection for this state.

    Returns:
      A tuple of (Total Count, Total Value)
    """
    cdef int count_total = 0
    cdef double value_total = 0

    cdef int i
    for i in self.state.count_buckets[:self.state.num_buckets]:
      count_total += i

    cdef float f
    for f in self.state.value_buckets[:self.state.num_buckets]:
      value_total += f

    return (count_total, value_total)

  def DiscardOldestBuckets(self, double now):
    """Discard buckets older than (now - self.total_interval_secs).

    Args:
      now - The UTC timestamp at the time this method is called.
    """
    cdef double oldest_time_expected
    cdef double gap
    cdef int gap_in_buckets
    cdef int num_buckets_to_discard

    # Check whether the buckets need rotating.
    oldest_time_expected = now - self.state.total_interval_secs
    if oldest_time_expected <= self.state.oldest_bucket:
      return

    # Calculate the number of buckets to rotate.
    gap = oldest_time_expected - self.state.oldest_bucket
    gap_in_buckets = <int> ceil(gap / self.bucket_interval)
    buckets_to_discard = int_min(gap_in_buckets, self.state.num_buckets)

    # Rotate the bucket arrays.
    self._UpdateBucketArray(
        <void*>self.state.count_buckets,
        sizeof(int),
        buckets_to_discard)

    self._UpdateBucketArray(
        <void*>self.state.value_buckets,
        sizeof(float),
        buckets_to_discard)

    # Keep 'oldest_bucket_time' at bucket boundaries.
    self.state.oldest_bucket += gap_in_buckets * self.bucket_interval

  cdef inline void _UpdateBucketArray(self,
      void* arr,
      size_t element_size,
      int buckets_to_discard):
    """Update an array that stores bucketed total count/value data.

    This discards the given number of buckets by copying everything after those
    buckets to the beginning of the array and clearing the remainder of the
    array.

    Args:
      arr - Pointer to an array of buckets.
      element_size - The size of each element in the array.
      buckets_to_discard - The number of buckets to discard from the beginning
          of the array.
    """
    cdef size_t bytes_to_copy, bytes_to_discard

    bytes_to_copy = (self.state.num_buckets - buckets_to_discard) * element_size
    bytes_to_discard = buckets_to_discard * element_size

    memcpy(arr, arr + bytes_to_discard, bytes_to_copy)
    memset(arr + bytes_to_copy, 0, bytes_to_discard)

  cdef inline int _GetIndex(self, double event_time):
    """Return the index of the bucket that represents an event time.

    Args:
      event_time - Time of the event to retrieve the index for.

    Returns:
      The index in the bucket arrays for the given time, or -1 if the time is
      outside the bucket range.
    """
    if event_time < self.state.oldest_bucket:
      return -1

    cdef int index = \
        <int>((event_time - self.state.oldest_bucket) / self.bucket_interval)

    # 'index' will be exactly equal to 'self.num_buckets' if we just reset
    # 'self.oldest_bucket' from 'now'.
    index = int_min(index, self.state.num_buckets - 1)

    return index

  def __repr__(self):
    return ', '.join([
        'total_interval: %d' % self.state.total_interval_secs,
        'buckets: %d' % self.state.num_buckets,
        'bucket_interval: %f' % self.bucket_interval,
        'oldest: %f' % self.state.oldest_bucket,])
