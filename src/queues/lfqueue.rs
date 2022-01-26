use std::cell::{Cell, RefCell, UnsafeCell};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::queues::queues::{BQueue, Queue, QueueError, SizableQueue};
use crate::utils::backoff::{Backoff, Rooms};

const SIZE_ROOM: i32 = 1;
const ADD_ROOM: i32 = 2;
const REM_ROOM: i32 = 3;

///A bounded, blocking queue with exponential backoff to prevent over contention when
///Working with many concurrent threads
///TODO: Fix the issue that is caused by the head and tail being strictly ascending
///Therefore we will reach a point of overflow with continued use.
struct LFQueue<T> {
    array: UnsafeCell<Vec<T>>,
    head: AtomicU32,
    tail: AtomicU32,
    rooms: Rooms,
    is_full: AtomicBool,
    capacity: usize,
}

impl<T> LFQueue<T> {
    pub fn new(expected_size: usize) -> Self {
        Self {
            array: UnsafeCell::new(Vec::with_capacity(expected_size)),
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
            rooms: Rooms::new(3),
            is_full: AtomicBool::new(false),
            capacity: expected_size,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> SizableQueue for LFQueue<T> {
    fn size(&self) -> u32 {
        self.rooms.enter(SIZE_ROOM);

        let size = self.tail.load(Ordering::SeqCst) - self.head.load(Ordering::SeqCst);

        self.rooms.leave(SIZE_ROOM);

        size
    }
}

impl<T> Queue<T> for LFQueue<T> {
    fn enqueue(&self, elem: T) -> Result<(), QueueError> {
        if self.is_full.load(Ordering::Relaxed) {
            //if the array is already full, we don't have to try to enter the room,
            //which could have caused a lot of contention and therefore could have
            //made removing harder in a multi producer single consumer scenario
            return Result::Err(QueueError::QueueFull);
        }

        self.rooms.enter(ADD_ROOM);

        let prev_tail = self.tail.fetch_add(1, Ordering::SeqCst);

        let head = self.head.load(Ordering::SeqCst);

        if prev_tail - head < self.capacity() as u32 {
            unsafe {
                let array_mut = &mut *self.array.get();

                array_mut.insert((prev_tail as usize % self.capacity()), elem);
            }

            //In case the element we have inserted is the last one,
            //Close the door behind us
            if (prev_tail - head) + 1 >= self.capacity() as u32 {
                self.is_full.store(true, Ordering::Relaxed);
            }

            self.rooms.leave(ADD_ROOM);

            return Result::Ok(());
        }


        self.tail.fetch_sub(1, Ordering::SeqCst);

        self.rooms.leave(ADD_ROOM);

        Err(QueueError::QueueFull)
    }

    fn pop(&self) -> Option<T> {
        let t: T;

        self.rooms.enter(REM_ROOM);

        let prev_head = self.head.fetch_add(1, Ordering::SeqCst);

        if prev_head < self.tail.load(Ordering::SeqCst) {
            let pos = prev_head as usize % self.capacity();

            unsafe {
                let array_mut = &mut *self.array.get();

                t = array_mut.remove(pos);
            }

            if self.is_full.load(Ordering::SeqCst) {
                self.is_full.store(false, Ordering::Relaxed);
            }

            self.rooms.leave(REM_ROOM);

            Some(t)
        } else {
            self.head.fetch_sub(1, Ordering::Relaxed);

            self.rooms.leave(REM_ROOM);

            None
        }
    }

    fn dump(&self, count: usize) -> Vec<T> {
        //Pre allocate the vector to limit to the max the
        //amount of time we will spend in the critical section
        let mut new_vec = Vec::with_capacity(count);

        loop {
            self.rooms.enter(REM_ROOM);

            let current_tail = self.tail.load(Ordering::SeqCst);

            let prev_head = self.head.swap(current_tail, Ordering::SeqCst);

            let count = current_tail - prev_head;

            if count > 0 {
                unsafe {
                    let x = &mut *self.array.get();

                    //Move the values into the new vector
                    for pos in prev_head..current_tail {
                        new_vec.push(x.remove(pos as usize));
                    }
                }
            }

            self.rooms.leave(REM_ROOM);

            return new_vec;
        }
    }
}

impl<T> BQueue<T> for LFQueue<T> {
    fn enqueue_blk(&self, elem: T) {
        loop {
            if self.is_full.load(Ordering::Relaxed) {
                //if the array is already full, we don't have to try to enter the room,
                //which could have caused a lot of contention and therefore could have
                //made removing harder in a multi producer single consumer scenario
                Backoff::backoff();
                continue;
            }

            self.rooms.enter(ADD_ROOM);

            let prev_tail = self.tail.fetch_add(1, Ordering::SeqCst);

            let head = self.head.load(Ordering::SeqCst);

            if prev_tail - head < self.capacity() as u32 {
                unsafe {
                    let array_mut = &mut *self.array.get();

                    array_mut.insert(prev_tail as usize % self.capacity(), elem);
                }

                //In case the element we have inserted is the last one,
                //Close the door behind us
                if (prev_tail - head) + 1 >= self.capacity() as u32 {
                    self.is_full.store(true, Ordering::Relaxed);
                }

                break;
            }

            {
                self.tail.fetch_sub(1, Ordering::SeqCst);

                self.rooms.leave(ADD_ROOM);

                Backoff::backoff();
            }
        }

        self.rooms.leave(ADD_ROOM);
        Backoff::reset();
    }

    fn pop_blk(&self) -> T {
        let t: T;

        loop {
            self.rooms.enter(REM_ROOM);

            let prev_head = self.head.fetch_add(1, Ordering::SeqCst);

            if prev_head < self.tail.load(Ordering::SeqCst) {
                let pos = prev_head as usize % self.capacity();

                unsafe {
                    let array_mut = &mut *self.array.get();

                    t = array_mut.remove(pos);
                }

                if self.is_full.load(Ordering::SeqCst) {
                    self.is_full.store(false, Ordering::Relaxed);
                }

                self.rooms.leave(REM_ROOM);

                break;
            } else {
                self.head.fetch_sub(1, Ordering::Relaxed);

                self.rooms.leave(REM_ROOM);

                Backoff::backoff();
            }
        }

        Backoff::reset();

        return t;
    }

    fn dump_blk(&self, count: usize) -> Vec<T> {
        ///Pre allocate the vector to limit to the max the
        /// amount of time we will spend in the critical section
        let mut new_vec = Vec::with_capacity(count);

        let mut left_to_collect = count as i32;

        loop {
            self.rooms.enter(REM_ROOM);

            let mut last_element = self.tail.load(Ordering::SeqCst);

            let prev_head = self.head.swap(last_element, Ordering::SeqCst);

            let mut count = last_element - prev_head;

            if count > 0 {

                if count > left_to_collect as u32 {
                    let excess = count - left_to_collect as u32;

                    //rewind the uncollected
                    self.head.fetch_sub(excess, Ordering::SeqCst);

                    last_element -= excess;

                    count = left_to_collect as u32;
                }

                left_to_collect -= count as i32;

                unsafe {
                    let x = &mut *self.array.get();

                    //Move the values into the new vector
                    for pos in prev_head..last_element {
                        new_vec.push(x.remove(pos as usize));
                    }
                }
            }

            self.rooms.leave(REM_ROOM);

            if left_to_collect <= 0 {

                Backoff::reset();

                return new_vec;
            }

            Backoff::backoff();
        }
    }
}


