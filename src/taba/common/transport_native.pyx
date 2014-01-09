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

"""Native implementation of Taba transport decoding. The main purpose of using
a native implementation, other than the improvement in CPU usage, is to manage
memory usage manually.

The equivalent non-native decoding implementation creates a large number of
tuples and lists which, while all individually small, get placed arbitrarily
throughout the heap. As large numbers of these objects are created and garbage
collected, memory fragmentation becomes a serious problem (especially when large
contiguous blocks also have to be allocated to store incoming requests).

Instead of creating many small container objects, this implementation allocates
page-sized blocks for storing pointers to the decoded event data. While this
seems less efficient on the surface, it means that when a bundle is deallocated,
all the pages it used can be deallocated simultaneously, without any possibility
of fragmentation holding mostly empty pages.

The memory layout of the block chain looks like the following:

head  ->  EventBlock   ->   EventBlock   ->   NULL
          |                 |
          |> [void*, ...]   |> [void*, ...]
              | | |> event     | | |> event
              | |> event       | |> event
              |> event         |> event

Each EventBlock consists of a header indicating the number of events in the
block, a pointer to the next block, and a pointer to an array of pointers to
the actual event data. The header and the array of event pointers occupy a
single page (and are the only memory allocated to that page).

The actual event pointers point to one of two possible types of objects. First,
they may point to the string representation of the event, as pulled from the
encoded request body text. To further improve performance and memory usage,
the pointers point directly to the strings used by the Python interpreter. This
means that the strings are not copied, and have all the benefits of optimized
way in which Python deals with string. In order to do this, we interact with
the Python reference counter to make sure the background strings are not
garbage collected before the block chain is deallocated.

The other possible type the event pointers can point to is an Event Python
object (from transport.py). The string representations are decoded to Event
objects dynamically as the data is accessed (through an iterator), when decoding
is enabled. The original string are replaced to allow multiple passes though the
data without having to decode the events multiple times.
"""

from libc.stdlib cimport calloc, free
from libc.string cimport memcpy, memset

# Imports from cpython to interact with the reference counter.
from cpython cimport Py_INCREF, Py_DECREF

import cjson

###########################################################
# Event Block chain manipulations
###########################################################

import resource
cdef unsigned int PAGE_SIZE = resource.getpagesize()

# Each block contains an EventBlock object at the beginning, followed by as
# many pointers to event buffers as will fit.
cdef unsigned int EVENTS_PER_BLOCK = \
    ((PAGE_SIZE - sizeof(EventBlock)) / sizeof(void*))

# C-struct that serves as the header for an event block.
cdef struct EventBlock:

  # Pointer to the array of pointers to event objects. In the page layout this
  # will point to the memory location directly after the header.
  void** data

  # Number of event pointers in this block.
  unsigned int num_events

  # Pointer to the next block in the block chain.
  EventBlock* next


cdef EventBlock* GenerateBlock():
  # Create a new EventBlock. Allocates a page-sized block of memory and places
  # an EventBlock struct at the beginning, with the pointer array located just
  # after it in the page. The returned pointer also corresponds to the return
  # from the malloc call, so it can be used to free the allocated page.
  #
  # Returns:
  #   Pointer to a newly allocated EventBlock memory range.
  cdef void* allocation = calloc(1, PAGE_SIZE)

  cdef EventBlock* p_block = <EventBlock*>allocation
  p_block.data = <void**>(allocation + sizeof(EventBlock))
  p_block.num_events = 0
  p_block.next = NULL

  return p_block

cdef void DeallocBlockChain(EventBlock* head):
  # Free the memory for an entire EventBlock chain. Decrements the reference
  # counter for each referred event object.
  #
  # Args:
  #   head - Pointer to the head of a EventBlock chain.
  cdef EventBlock* block = head
  cdef EventBlock* block_next
  cdef unsigned int i

  while block != NULL:
    block_next = block.next

    for i in range(block.num_events):
      if block.data[i] != NULL:
        Py_DECREF(<object>block.data[i])

    free(block)

    block = block_next

###########################################################
# Python interface class.
###########################################################

cdef class EventBundle(object):
  """Wrapper class for building and accessing a bundle of Event objects for a
  Client and Name. Events are added to the bundle by calling AddEventLine, and
  can be accessed by using the class as an iterable.
  """
  cdef object _client
  cdef object _name
  cdef object _type

  cdef EventBlock* _block_chain_head
  cdef EventBlock* _block_chain_tail

  cdef unsigned int _num_events

  cdef object _decode_events

  def __cinit__(self, decode_events):
    # Args:
    #   decode_events - Boolean indicating whether events should be decoded into
    #       Event objects on access (instead of returning the raw string).
    self._block_chain_head = GenerateBlock()
    self._block_chain_tail = self._block_chain_head

    self._num_events = 0
    self._client = None
    self._name = None
    self._type = None

  def __init__(self, decode_events):
    """
    Args:
      decode_events - Boolean indicating whether events should be decoded into
          Event objects on access (instead of returning the raw string).
    """
    self._decode_events = decode_events

  def __dealloc__(self):
    DeallocBlockChain(self._block_chain_head)

  #########################################################
  # Bundle Building API
  #########################################################

  def AddEventLine(self, event_line):
    """Add an encoded event string to the bundle.

    Args:
      event_line - Encoded event string.
    """
    cdef EventBlock* new_block
    cdef EventBlock* block

    # check if we need to add a new EventBlock to the chain.
    if self._block_chain_tail.num_events == EVENTS_PER_BLOCK:
      new_block = GenerateBlock()
      self._block_chain_tail.next = new_block
      self._block_chain_tail = new_block

    # Add a pointer to the event to the block chain.
    block = self._block_chain_tail
    block.data[block.num_events] = <void*>event_line
    block.num_events += 1
    self._num_events += 1

    # Increment the reference counter on the string so that it isn't garbage
    # collected before the block chain is deallocated.
    Py_INCREF(event_line)

  #########################################################
  # Property Accessors
  #########################################################

  property client:
    def __get__(self):
      return self._client

    def __set__(self, value):
      self._client = value

  property name:
    def __get__(self):
      return self._name

    def __set__(self, value):
      self._name = value

  property num_events:
    def __get__(self):
      return self._num_events

  property tab_type:
    def __get__(self):
      self._MaybeParseType()
      return self._type

  def _MaybeParseType(self):
    """Check whether self._type has been set yet, and if not try to parse it
    from an event on the block chain.
    """
    if self._type == None and self._block_chain_head.num_events > 0:
      event = <object>self._block_chain_head.data[0]
      self._type = cjson.decode(event)[0]

  def __iter__(self):
    """Create an iterator of Events. If decode_events was set to True, the
    iterator will yield Event() objects. Otherwise it will yield the encoded
    event string values.
    """
    self._MaybeParseType()

    state = EventIteratorState()
    state.set_head(self._block_chain_head)

    return EventIterator(state, self._decode_events)

  def __repr__(self):
    return 'EvBu(%s, %s, %s, %s)' % (
        self.client, self.name, self.tab_type, [e for e in self])

###########################################################
# Event Container
###########################################################

class Event(object):
  __slots__ = ['_timestamp', '_payload']

  def __init__(self, timestamp, payload):
    self._timestamp = timestamp
    self._payload = payload

  @property
  def timestamp(self):
    return self._timestamp

  @property
  def payload(self):
    return self._payload

  def __repr__(self):
    return 'Event(%s, %s)' % (self.timestamp, self.payload)

###########################################################
# Event Iterator Objects
###########################################################

cdef class EventIteratorState(object):
  # Native wrapper class to store the state of an Event iterator. Acts like a
  # Python iterator, but yields a sequence of pointers to event objects from
  # a block chain. Also supports replacing the pointer at the current position.

  cdef EventBlock* block
  cdef unsigned int offset

  def __cinit__(self):
    self.block = NULL
    self.offset = 0

  def __init__(self):
    pass

  cdef void set_head(self, EventBlock* head):
    self.block = head

  cdef void* next(self):

    # We're past the end: return the equivalent of StopIteration.
    if self.block == NULL:
      return NULL

    # Check if we're at the end of the current block.
    if self.offset == self.block.num_events:
      self.block = self.block.next
      self.offset = 0

    if self.block == NULL:
      return NULL

    # Return the next pointer and increment our cursor.
    cdef void* val = self.block.data[self.offset]
    self.offset += 1

    return val

  cdef void replace_position(self, object new_val):
    # Replace the pointer at the previously returned index with a pointer to
    # the provided Python object.
    #
    # Args:
    #   new_val - Python object to point to.

    # Set the new pointer.
    cdef void* old_val = self.block.data[self.offset - 1]
    self.block.data[self.offset - 1] = <void*>new_val

    # Update the Python reference counter.
    Py_INCREF(new_val)
    Py_DECREF(<object>old_val)

class EventIterator(object):
  """Event iterator Python class.
  """

  def __init__(self, state, decode_events):
    """
    Args:
      state - EventIteratorState object.
      decode_events - If True, yields Event objects, otherwise yields encoded
          event strings.
    """
    self.state = state
    self.decode_events = decode_events

  def __iter__(self):
    return self

  def next(self):
    """Get the next item"""

    # Get the next pointer from the EventIteratorState.
    cdef void* p_event_buffer = (<EventIteratorState>self.state).next()

    # A NULL pointer means StopIteration.
    if p_event_buffer == NULL:
      raise StopIteration()

    cdef object event_buffer = <object>p_event_buffer

    # Possibly decode the event, depending in the settings and the current type
    # of the event object.
    if self.decode_events:

      if isinstance(event_buffer, str):
        # Decode the event string into an Event object and replace the original
        # pointer (so that future passes don't have to re-decode the events).
        event_tuple = cjson.decode(event_buffer)
        event = Event(
            float(event_tuple[1]),
            cjson.decode(event_tuple[2]))

        (<EventIteratorState>self.state).replace_position(event)

      else:
        event = event_buffer

    else:
      event = event_buffer

    return event
