use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use crossbeam_utils::{Backoff, CachePadded};

use crate::queues::queues::{BQueue, Queue, QueueError, SizableQueue};
use crate::queues::queues::QueueError::MalformedInputVec;
use crate::utils::backoff;
use crate::utils::backoff::{Rooms};
use crate::utils::memory_access::UnsafeWrapper;

const SIZE_ROOM: i32 = 1;
const ADD_ROOM: i32 = 2;
const REM_ROOM: i32 = 3;

///A bounded, blocking queue with exponential backoff to prevent over contention when
///Working with many concurrent threads
///TODO: Fix the issue that is caused by the head and tail being strictly ascending
///Therefore we will reach a point of overflow with continued use.
pub struct LFArrayQueue<T> {
    array: UnsafeWrapper<Vec<Option<T>>>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    rooms: Rooms,
    is_full: AtomicBool,
    capacity: usize,
}

impl<T> LFArrayQueue<T> {
    pub fn new(expected_size: usize) -> Self {
        let mut vec = Vec::with_capacity(expected_size);

        for _ in 0..expected_size {
            vec.push(Option::None);
        }

        Self {
            array: UnsafeWrapper::new(vec),
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            rooms: Rooms::new(3),
            is_full: AtomicBool::new(false),
            capacity: expected_size,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> SizableQueue for LFArrayQueue<T> {
    fn size(&self) -> usize {
        self.rooms.enter_blk(SIZE_ROOM);

        let size = self.tail.load(Ordering::SeqCst) - self.head.load(Ordering::SeqCst);

        self.rooms.leave_blk(SIZE_ROOM);

        size
    }
}

impl<T> Queue<T> for LFArrayQueue<T> where T: Debug {
    fn enqueue(&self, elem: T) -> Result<(), QueueError> {
        if self.is_full.load(Ordering::Relaxed) {
            //if the array is already full, we don't have to try to enter the room,
            //which could have caused a lot of contention and therefore could have
            //made removing harder in a multi producer single consumer scenario
            return Result::Err(QueueError::QueueFull);
        }

        self.rooms.enter_blk(ADD_ROOM);

        let prev_tail = self.tail.fetch_add(1, Ordering::SeqCst);

        let head = self.head.load(Ordering::SeqCst);

        if prev_tail - head < self.capacity() {
            unsafe {
                let array_mut = &mut *self.array.get();

                array_mut.get_mut(prev_tail as usize % self.capacity()).unwrap().insert(elem);
            }

            //In case the element we have inserted is the last one,
            //Close the door behind us
            if (prev_tail - head) + 1 >= self.capacity() {
                self.is_full.store(true, Ordering::Relaxed);
            }

            self.rooms.leave_blk(ADD_ROOM);

            return Result::Ok(());
        }

        self.tail.fetch_sub(1, Ordering::SeqCst);

        self.rooms.leave_blk(ADD_ROOM);

        Err(QueueError::QueueFull)
    }

    fn pop(&self) -> Option<T> {
        let t: T;

        self.rooms.enter_blk(REM_ROOM);

        let prev_head = self.head.fetch_add(1, Ordering::SeqCst);

        if prev_head < self.tail.load(Ordering::SeqCst) {
            let pos = prev_head % self.capacity();

            unsafe {
                let array_mut = &mut *self.array.get();

                t = array_mut.get_mut(pos).unwrap().take().unwrap();
            }

            self.is_full.store(false, Ordering::Relaxed);

            self.rooms.leave_blk(REM_ROOM);

            Some(t)
        } else {
            self.head.fetch_sub(1, Ordering::Relaxed);

            self.rooms.leave_blk(REM_ROOM);

            None
        }
    }

    fn dump(&self, count: usize, vec: &mut Vec<T>) -> Result<usize, QueueError> {
        if vec.capacity() < count {
            return Err(MalformedInputVec);
        }

        loop {
            self.rooms.enter_blk(REM_ROOM);

            //Since we are in a remove room we know the tail is not going to be altered
            let current_tail = self.tail.load(Ordering::SeqCst);

            let prev_head = self.head.swap(current_tail, Ordering::SeqCst);

            let count = current_tail - prev_head;

            if count > 0 {
                unsafe {
                    let x = &mut *self.array.get();

                    //Move the values into the new vector
                    for pos in prev_head..current_tail {
                        vec.push(x.get_mut(pos as usize).unwrap().take().unwrap());
                    }
                }
            }

            self.rooms.leave_blk(REM_ROOM);

            return Ok(count);
        }
    }
}

impl<T> BQueue<T> for LFArrayQueue<T> {
    fn enqueue_blk(&self, elem: T) {
        let backoff = Backoff::new();

        loop {
            if self.is_full.load(Ordering::Relaxed) {
                //if the array is already full, we don't have to try to enter the room,
                //which could have caused a lot of contention and therefore could have
                //made removing harder in a multi producer single consumer scenario
                backoff.snooze();
                continue;
            }

            self.rooms.enter_blk(ADD_ROOM);

            let prev_tail = self.tail.fetch_add(1, Ordering::SeqCst);

            let head = self.head.load(Ordering::SeqCst);

            if prev_tail - head < self.capacity() {
                unsafe {
                    let array_mut = &mut *self.array.get();

                    array_mut.get_mut(prev_tail as usize % self.capacity()).unwrap().insert(elem);
                }

                //In case the element we have inserted is the last one,
                //Close the door behind us
                if (prev_tail - head) + 1 >= self.capacity() {
                    self.is_full.store(true, Ordering::Relaxed);
                }

                break;
            }

            self.tail.fetch_sub(1, Ordering::SeqCst);

            self.rooms.leave_blk(ADD_ROOM);

            backoff.snooze();
        }

        self.rooms.leave_blk(ADD_ROOM);
    }

    fn pop_blk(&self) -> T {
        let t: T;
        let backoff = Backoff::new();

        loop {
            self.rooms.enter_blk(REM_ROOM);

            let prev_head = self.head.fetch_add(1, Ordering::SeqCst);

            if prev_head < self.tail.load(Ordering::SeqCst) {
                let pos = prev_head as usize % self.capacity();

                unsafe {
                    let array_mut = &mut *self.array.get();

                    t = array_mut.get_mut(pos).unwrap().take().unwrap();
                }

                self.is_full.store(false, Ordering::Relaxed);

                self.rooms.leave_blk(REM_ROOM);

                break;
            } else {
                self.head.fetch_sub(1, Ordering::Relaxed);

                self.rooms.leave_blk(REM_ROOM);

                backoff.snooze();
            }
        }

        return t;
    }

    fn dump_blk(&self, count: usize) -> Vec<T> {
        //Pre allocate the vector to limit to the max the
        //amount of time we will spend in the critical section
        let mut new_vec = Vec::with_capacity(count);

        let mut left_to_collect = count;

        let backoff = Backoff::new();

        loop {
            self.rooms.enter_blk(REM_ROOM);

            let mut last_element = self.tail.load(Ordering::SeqCst);

            let prev_head = self.head.swap(last_element, Ordering::SeqCst);

            let mut count = last_element - prev_head;

            if count > 0 {
                if count > left_to_collect {
                    let excess = count - left_to_collect;

                    //rewind the uncollected
                    self.head.fetch_sub(excess, Ordering::SeqCst);

                    last_element -= excess;

                    count = left_to_collect;
                }

                left_to_collect -= count;

                unsafe {
                    let x = &mut *self.array.get();

                    //Move the values into the new vector
                    for pos in prev_head..last_element {
                        new_vec.push(x.get_mut(pos as usize).unwrap().take().unwrap());
                    }
                }
            }

            self.rooms.leave_blk(REM_ROOM);

            if left_to_collect <= 0 {
                return new_vec;
            }

            backoff.snooze();
        }
    }
}