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

"""Native Cython implementation of a percentile counter.
"""

import random

# Import C-library dependencies.
from libc.stdlib cimport calloc, free, malloc, qsort
from libc.string cimport memcpy, memset

# Declare a typedef for "const void"; used by qsort() comparator functions.
cdef extern from *:
  ctypedef void const_void "const void"

# The percentiles that should be returned by the 'percentiles' attribute.
DEF NUM_PERCENTILES = 7
PERCENTILES = (0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999)

ZERO_PERCENTILES = [0.0] * NUM_PERCENTILES
cdef float[NUM_PERCENTILES] FLOAT_PERCENTILES
cdef int i
for i in xrange(NUM_PERCENTILES):
  FLOAT_PERCENTILES[i] = PERCENTILES[i]

# The running sample size is fixed as a constant.
DEF DEFAULT_MAX_SAMPLES = 512

# C struct for holding a single sample, suitable for direct serialization.
cdef struct Sample:

  # The randomly assigned sample score, used to perform uniform sub-sampling
  # across arbitrarily reduced States.
  float score

  # Actual recorded value of the sample.
  double value

# C struct for holding a full set of samples.
cdef struct SampleHeap:

  # The maximum samples to store in the samples array,
  unsigned int max_samples

  # Count of the number of samples actually in the 'samples' array.
  unsigned int num_samples

  # Array of samples. This is maintained as a max-heap, in order to facilitate
  # quick Fold and Reduce operations.
  Sample* samples

# C struct encapsulating percentile counter state for a single counter instance.
# (Maintains contiguous memory layout to make serialization/deserialization
# fast and simple)
cdef struct PercentileCounterState:

  # The total number of values recorded (i.e., the count).
  unsigned long long num_values_recorded

  # The sum of all the values recorded.
  long double values_total

  # Container for the sampled values.
  SampleHeap sample_heap

###########################################################
# SERIALIZATION
###########################################################

cdef _SerializeState(PercentileCounterState *state):
  """Encode the given State into a Python binary string.

  Args:
    state - A pointer to a PercentileCounterState struct.

  Returns:
    An encoded Python binary string object.
  """
  # Allocate working memory for the buffer.
  cdef unsigned int buffer_size = (
      sizeof(PercentileCounterState)
      - sizeof(Sample*)
      + (sizeof(Sample) * state.sample_heap.num_samples))

  cdef char* buffer = <char*> calloc(1, buffer_size)
  cdef char* cursor = buffer
  cdef unsigned int part_size

  # Copy the statically sized parts of the state into the buffer.
  part_size = sizeof(PercentileCounterState) - sizeof(Sample*)
  memcpy(cursor, state, part_size)
  cursor += part_size

  # Copy the dynamically sized parts of the state (i.e. the Samples) into the
  # buffer.
  part_size = sizeof(Sample) * state.sample_heap.num_samples
  memcpy(cursor, state.sample_heap.samples, part_size)

  # Convert the buffer into a Python object, and free locally allocated memory.
  obj_buffer = buffer[:buffer_size]
  free(buffer)

  return obj_buffer

cdef PercentileCounterState* _DeserializeState(char* state_byte_string):
  """Decode the given State string into a PercentileCounterState object.

  Note that the string passed as an argument must remain alive as long as the
  returned PercentileCounterState is accessed/modified.

  Args:
    state_byte_string - A C-string representing the encoded percentile counter
        state.

  Returns:
    A pointer to the decoded PercentileCounterState struct.
  """
  cdef unsigned int part_size
  cdef char* cursor = state_byte_string

  # Allocate and fill in the statically defined parts of the state (i.e.
  # everything but the Samples array and its pointer).
  cdef PercentileCounterState* state = \
      <PercentileCounterState*>calloc(1, sizeof(PercentileCounterState))

  part_size = sizeof(PercentileCounterState) - sizeof(Sample*)
  memcpy(state, cursor, part_size)
  cursor += part_size

  # Allocate memory for the Samples array (up to its maximum size).
  cdef unsigned int arr_size = sizeof(Sample) * state.sample_heap.max_samples
  cdef Sample* samples = <Sample*>calloc(1, arr_size)

  # Fill in the _available_ data into the Samples array.
  part_size = sizeof(Sample) * state.sample_heap.num_samples
  memcpy(samples, cursor, part_size)

  state.sample_heap.samples = samples

  return state

cdef _DeallocateState(PercentileCounterState* state):
  """Free the memory used by a PercentileCounterState object.

  Args:
    state - PercentileCounterState object pointer.
  """
  free(state.sample_heap.samples)
  free(state)

def Repr(state_buffer):
  """Format a serialized PercentileCounterState into a human-readable form.

  Args:
    state_buffer - Serialized PercentileCounterState binary string.

  Returns:
    A human-readable string representation of the PercentileCounterState.
  """
  cdef PercentileCounterState* state = _DeserializeState(state_buffer)

  cdef int i
  hp_str = []
  for i in range(state.sample_heap.max_samples):
    hp_str.append('(%.3f, %.3f)' % (
        state.sample_heap.samples[i].score,
        state.sample_heap.samples[i].value))

  hp_str = ', '.join(hp_str)

  result = 'CNT: %d, TOT: %f, <SMP: %d, HP: %s>' % (
      state.num_values_recorded,
      state.values_total,
      state.sample_heap.num_samples,
      hp_str)

  _DeallocateState(state)
  return result

###########################################################
# HANDLER API METHODS
###########################################################

cdef PercentileCounterState* _BlankState(unsigned int max_samples):
  """Allocate and initialize a PercentileCounterState object with a specific
  sample size.

  Args:
    max_samples - The number of Samples to keep in the PercentileCounterState.

  Returns:
    Pointer to a newly allocated PercentileCounterState object.
  """
  cdef PercentileCounterState* state = \
      <PercentileCounterState*>calloc(1, sizeof(PercentileCounterState))

  cdef Sample* samples = <Sample*>calloc(1, sizeof(Sample) * max_samples)

  state.sample_heap.max_samples = max_samples
  state.sample_heap.samples = samples

  return state

def NewState(max_samples=DEFAULT_MAX_SAMPLES):
  """Generate a new state binary string representing a newly allocated counter
  instance.

  Returns:
    A new state Python binary string object.
  """
  cdef PercentileCounterState* state = _BlankState(max_samples)

  state_obj = _SerializeState(state)

  _DeallocateState(state)
  return state_obj

def FoldEvents(state_buffer, events):
  """Given a state buffer and a list of Events, fold the Events into the State,
  and return the updated State Buffer.

  Args:
    state_buffer - A state buffer of the type returned by NewState().
    events - A list of Event objects.

  Returns:
    A state Python binary string object with the events folded in.
  """
  cdef double value
  cdef Sample tmp_sample

  cdef PercentileCounterState *state = _DeserializeState(state_buffer)

  for event in events:
    if type(event.payload) in (tuple, list):
      value = event.payload[0]
    else:
      value = event.payload

    # Generate a Sample for the value, and attempt to add it to the SampleHeap.
    tmp_sample.value = value
    tmp_sample.score = random.random()
    _SampleHeap_AddSample(&(state.sample_heap), &tmp_sample)

    # Update the all-time running counts.
    state.num_values_recorded += 1
    state.values_total += value

  state_obj =  _SerializeState(state)

  _DeallocateState(state)
  return state_obj

def Reduce(state_buffers):
  """Given a list of serialized PercentileCounterState binary strings, Reduce
  them all together into a single PercentileCounterState.

  Args:
    state_buffers - List of serialized PercentileCounterState binary strings.

  Returns:
    A single serialized PercentileCounterState binary string.
  """
  cdef int n = len(state_buffers)
  if n == 0:
    return None
  elif n == 1:
    return state_buffers[0]

  cdef PercentileCounterState** states = \
      <PercentileCounterState**> calloc(n, sizeof(PercentileCounterState*))

  for i in range(n):
    states[i] = _DeserializeState(state_buffers[i])

  # Allocate and initialize the result.
  cdef unsigned int max_samples = states[0].sample_heap.max_samples
  cdef PercentileCounterState* base = _BlankState(max_samples)

  # Merge each input State into the result.
  cdef PercentileCounterState* tmp
  for i in range(n):
    _SampleHeap_Merge(&(base.sample_heap), &(states[i].sample_heap))
    base.num_values_recorded += states[i].num_values_recorded
    base.values_total += states[i].values_total
    _DeallocateState(states[i])

  # Return the serialized result.
  result = _SerializeState(base)

  _DeallocateState(base)
  free(states)

  return result

def Render(state_buffer):
  """Convert a binary PercentileCounterState into its percentiles as Python
  objects.

  Args:
    state_buffer - Serialized PercentileCounterState binary string.

  Returns:
    Tuple of (
        All-time Value Count,
        All-time Value Total,
        [(Percentile Fraction, Estimated Percentile Value), ])
  """
  cdef PercentileCounterState *state = _DeserializeState(state_buffer)

  val = (state.num_values_recorded, state.values_total, _GetPercentiles(state))

  _DeallocateState(state)
  return val

cdef _GetPercentiles(PercentileCounterState *state):
  """Compute percentile statistics for the values accumulated thus far.

  Returns:
    A list of float percentile statistics.
  """
  cdef float pct
  cdef int idx

  cdef unsigned int num_to_sort = state.sample_heap.num_samples
  if num_to_sort == 0:
    return ZERO_PERCENTILES

  cdef double* values = <double*> malloc(sizeof(double) * num_to_sort)
  for i in range(num_to_sort):
    values[i] = state.sample_heap.samples[i].value

  qsort(values, num_to_sort, sizeof(double), &_CompareDoubles)

  percentiles = []
  for pct in FLOAT_PERCENTILES:
    idx = <int> (pct * <float> num_to_sort)
    percentiles.append(values[idx])

  free(values)
  return percentiles

cdef int _CompareDoubles(const_void* a, const_void* b) nogil:
  """Comparison function for floats; used by qsort().
  """
  cdef double* af = <double*> a
  cdef double* bf = <double*> b

  if af[0] < bf[0]:
    return -1

  if af[0] > bf[0]:
    return 1

  return 0

###########################################################
# SAMPLE HEAP METHODS
###########################################################

cdef void _SampleHeap_AddSample(SampleHeap* heap, Sample* sample):
  """Add a Sample to the SampleHeap.

  Args:
    heap - SampleHeap to sample into.
    sample - Sample to add.
  """

  if heap.num_samples < heap.max_samples:
    # If the SampleHeap isn't full, always add the new Sample. Add it to the
    # bottom of the heap, and sift it up until the heap property is restored.
    heap.samples[heap.num_samples].score = sample.score
    heap.samples[heap.num_samples].value = sample.value

    _SampleHeap_HeapUp(heap, heap.num_samples)
    heap.num_samples += 1

  else:
    # When the SampleHeap if full, we only add new Samples when their randomly
    # generated score is less than the largest score currently on the heap.
    # Since the largest element is always on the top of the heap, we can check
    # it directly. If the new Sample has a lower score, replace the max element,
    # and sift the new Sample down until the heap property is restored.
    if sample.score < heap.samples[0].score:
      heap.samples[0].score = sample.score
      heap.samples[0].value = sample.value

      _SampleHeap_HeapDown(heap, 0)

cdef void _SampleHeap_Merge(SampleHeap* this, SampleHeap* that):
  """Merge two SampleHeap objects together. The first SampleHeap is modified
  in-place.

  Args:
    this - SampleHeap pointer to merge into.
    that - SampleHeap pointer to merge from.
  """
  cdef int i
  for i in range(that.num_samples):
    _SampleHeap_AddSample(this, &(that.samples[i]))

cdef void _SampleHeap_HeapUp(SampleHeap* heap, unsigned int pos):
  """Given a SampleHeap and a newly added Sample position, sift the position up
  until the heap property is restored.

  Args:
    heap - SampleHeap pointer that a new Sample was added to.
    pos - Index into the 'samples' heap to sift up.
  """
  cdef int parent_pos
  cdef float parent_score
  cdef double parent_value

  while pos > 0:
    parent_pos = (pos - 1) >> 1
    parent_score = heap.samples[parent_pos].score
    parent_value = heap.samples[parent_pos].value

    if parent_score < heap.samples[pos].score:
      heap.samples[parent_pos].score = heap.samples[pos].score
      heap.samples[parent_pos].value = heap.samples[pos].value

      heap.samples[pos].score = parent_score
      heap.samples[pos].value = parent_value

      pos = parent_pos

    else:
      break

cdef void _SampleHeap_HeapDown(SampleHeap* heap, unsigned int pos):
  """Given a SampleHeap, and a newly added Sample position, sift the position
  down until the heap property is restored.

  Args:
    heap - SampleHeap pointer that a new Sample was added to.
    pos - Index into the 'samples' heap to sift down.
  """
  cdef int child_pos

  cdef float score
  cdef double value

  cdef unsigned int max_pos = (heap.num_samples >> 1) - 1
  while pos < max_pos:
    score = heap.samples[pos].score
    value = heap.samples[pos].value

    # Pick the child with the higher score (left or right).
    child_pos = (pos << 1) + 1
    if heap.samples[child_pos + 1].score > heap.samples[child_pos].score:
      child_pos += 1

    if score < heap.samples[child_pos].score:
      heap.samples[pos].score = heap.samples[child_pos].score
      heap.samples[pos].value = heap.samples[child_pos].value

      heap.samples[child_pos].score = score
      heap.samples[child_pos].value = value

      pos = child_pos

    else:
      break
