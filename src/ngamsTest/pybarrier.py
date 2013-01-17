# pybarrier.py  used by other Python programs that need barrier
#             from pybarrier import *
#
# Defines classes that provide synchronization objects.  Note that use of
# this module requires that your Python support threads.
#
#    condition()   # a POSIX-like condition-variable object
#    barrier(n)    # an n-thread barrier
#    event()       # an event object
#
# BARRIERS
#
# A barrier object is created via
#   import this_module
#   your_barrier = this_module.barrier(num_threads)
#
# Methods:
#   .enter()
#      the thread blocks until num_threads threads in all have done
#      .enter().  Then the num_threads threads that .enter'ed resume,
#      and the barrier resets to capture the next num_threads threads
#      that .enter it.
#
# EVENTS
#
# An event object is created via
#   import this_module
#   your_event = this_module.event()
#
# An event has two states, `posted' and `cleared'.  An event is
# created in the cleared state.
#
# Methods:
#
#   .post()
#      Put the event in the posted state, and resume all threads
#      .wait'ing on the event (if any).
#
#   .clear()
#      Put the event in the cleared state.
#
#   .is_posted()
#      Returns 0 if the event is in the cleared state, or 1 if the event
#      is in the posted state.
#
#   .wait()
#      If the event is in the posted state, returns immediately.
#      If the event is in the cleared state, blocks the calling thread
#      until the event is .post'ed by another thread.
#
# Note that an event, once posted, remains posted until explicitly
# cleared.  Relative to conditions, this is both the strength & weakness
# of events.  It's a strength because the .post'ing thread doesn't have to
# worry about whether the threads it's trying to communicate with have
# already done a .wait (a condition .signal is seen only by threads that
# do a .wait _prior_ to the .signal; a .signal does not persist).  But
# it's a weakness because .clear'ing an event is error-prone:  it's easy
# to mistakenly .clear an event before all the threads you intended to
# see the event get around to .wait'ing on it.  But so long as you don't
# need to .clear an event, events are easy to use safely.

import thread

class condition:
    def __init__(self):
        # the lock actually used by .acquire() and .release()
        self.mutex = thread.allocate_lock()

        # lock used to block threads until a signal
        self.checkout = thread.allocate_lock()
        self.checkout.acquire()

        # internal critical-section lock, & the data it protects
        self.idlock = thread.allocate_lock()
        self.id = 0
        self.waiting = 0  # num waiters subject to current release
        self.pending = 0  # num waiters awaiting next signal
        self.torelease = 0      # num waiters to release
        self.releasing = 0      # 1 iff release is in progress

    def acquire(self):
        self.mutex.acquire()

    def release(self):
        self.mutex.release()

    def wait(self):
        mutex, checkout, idlock = self.mutex, self.checkout, self.idlock
        if not mutex.locked():
            raise ValueError, \
                  "condition must be .acquire'd when .wait() invoked"

        idlock.acquire()
        myid = self.id
        self.pending = self.pending + 1
        idlock.release()

        mutex.release()

        while 1:
            checkout.acquire(); idlock.acquire()
            if myid < self.id:
                break
            checkout.release(); idlock.release()

        self.waiting = self.waiting - 1
        self.torelease = self.torelease - 1
        if self.torelease:
            checkout.release()
        else:
            self.releasing = 0
            if self.waiting == self.pending == 0:
                self.id = 0
        idlock.release()
        mutex.acquire()

    def signal(self):
        self.broadcast(1)

    def broadcast(self, num = -1):
        if num < -1:
            raise ValueError, '.broadcast called with num ' + `num`
        if num == 0:
            return
        self.idlock.acquire()
        if self.pending:
            self.waiting = self.waiting + self.pending
            self.pending = 0
            self.id = self.id + 1
        if num == -1:
            self.torelease = self.waiting
        else:
            self.torelease = min( self.waiting,
                                  self.torelease + num )
        if self.torelease and not self.releasing:
            self.releasing = 1
            self.checkout.release()
        self.idlock.release()

class barrier:
    def __init__(self, n):
        self.n = n
        self.togo = n
        self.full = condition()

    def enter(self):
        full = self.full
        full.acquire()
        self.togo = self.togo - 1
        if self.togo:
            full.wait()
        else:
            self.togo = self.n
            full.broadcast()
        full.release()
    wait = enter

class event:
    def __init__(self):
        self.state  = 0
        self.posted = condition()

    def post(self):
        self.posted.acquire()
        self.state = 1
        self.posted.broadcast()
        self.posted.release()

    def clear(self):
        self.posted.acquire()
        self.state = 0
        self.posted.release()

    def is_posted(self):
        self.posted.acquire()
        answer = self.state
        self.posted.release()
        return answer

    def wait(self):
        self.posted.acquire()
        while not self.state:
            self.posted.wait()
        self.posted.release()

# end of barrier.py
