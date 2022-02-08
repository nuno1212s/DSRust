use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{Acquire, Relaxed};

use crossbeam_utils::{Backoff, CachePadded};

use crate::queues::queues::{BQueue, Queue, QueueError, SizableQueue};
use crate::queues::queues::QueueError::MalformedInputVec;
use crate::utils::memory_access::UnsafeWrapper;
use crate::utils::rooms::Rooms;

const SIZE_ROOM: i32 = 1;
const ADD_ROOM: i32 = 2;
const REM_ROOM: i32 = 3;

///A bounded queue with rooms and exponential backoff to prevent over contention when
///Working with many concurrent threads
///TODO: Fix the issue that is caused by the head and tail being strictly ascending
///Therefore we will reach a point of overflow with continued use.
pub struct LFBRArrayQueue<T> {
    array: UnsafeWrapper<Vec<Option<T>>>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    rooms: Rooms,
    is_full: AtomicBool,
    capacity: usize,
    one_lap: usize,
}

impl<T> LFBRArrayQueue<T> {
    pub fn new(expected_size: usize) -> Self {
        let mut vec = Vec::with_capacity(expected_size);

        for _ in 0..expected_size {
            vec.push(Option::None);
        }

        let one_lap = expected_size.next_power_of_two();

        Self {
            array: UnsafeWrapper::new(vec),
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            rooms: Rooms::new(3),
            is_full: AtomicBool::new(false),
            capacity: expected_size,
            one_lap,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn extract_index(&self, lapped_ind: usize) -> usize {
        lapped_ind & (self.one_lap - 1)
    }

    fn extract_lap(&self, lapped_ind: usize) -> usize {
        lapped_ind & !(self.one_lap - 1)
    }

    fn one_lap(&self) -> usize {
        self.one_lap
    }
}

impl<T> SizableQueue for LFBRArrayQueue<T> {
    fn size(&self) -> usize {
        //We use acquire because we want to get the latest values from the other threads, but we won't change anything
        //So the store part can be Relaxed
        self.rooms.enter_blk_ordered(SIZE_ROOM, Ordering::Acquire).expect("Failed to enter room?");

        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);

        //Since we perform no changes, we can leave using the relaxed state
        self.rooms.leave_blk_ordered(SIZE_ROOM, Ordering::Relaxed).expect("Failed to exit room");

        let tail_index = self.extract_index(tail);
        let head_index = self.extract_index(head);

        if head_index < tail_index {
            return tail_index - head_index;
        } else if head_index > tail_index {
            return (self.capacity() - head_index) + tail_index;
        } else if tail == head {
            return 0;
        } else {
            return self.capacity();
        }
    }

    fn capacity(&self) -> Option<usize> {
        Some(self.capacity)
    }

    fn is_empty(&self) -> bool {
        self.rooms.enter_blk_ordered(SIZE_ROOM, Ordering::Acquire);

        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);

        //Since we perform no changes, we can leave using the relaxed state
        self.rooms.leave_blk_ordered(SIZE_ROOM, Ordering::Relaxed).expect("Failed to exit room");

        head == tail
    }

    fn is_full(&self) -> bool {
        self.rooms.enter_blk_ordered(SIZE_ROOM, Ordering::Acquire);

        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);

        self.rooms.leave_blk_ordered(SIZE_ROOM, Ordering::Relaxed);

        head.wrapping_add(self.one_lap()) == tail
    }
}

///The non blocking queue implementation
///We only use relaxed operations for every atomic access/modification
///Because we have the rooms implementation. Rooms works as follows: Any n users can be inside a room
/// But there can only be one room occupied at a time
///Since there can only be one room occupied at a time, we know that we can either have n writers or n readers,
/// never both. Writers do not need to see the changes that are being made by other writers since
/// Each writer has his own slot for writing that will not interfere with other writers
/// We only need memory synchronization when we are moving from readers to writers and vice-versa
/// So, by using Acquire when entering the room and release when leaving the rooms,
/// We know for a fact that readers will have access to all changes performed by writers and vice versa
/// And we have relaxed the memory barriers to improve performance
impl<T> Queue<T> for LFBRArrayQueue<T> where T: Debug {
    fn enqueue(&self, elem: T) -> Result<(), QueueError<T>> {
        if self.is_full.load(Ordering::Relaxed) {
            //if the array is already full, we don't have to try to enter the room,
            //which could have caused a lot of contention and therefore could have
            //made removing harder in a multi producer single consumer scenario
            return Result::Err(QueueError::QueueFull { 0: elem });
        }

        //Enter the room using acquire as we want all changes done before we have entered
        //The room to be propagated into this thread
        self.rooms.enter_blk_ordered(ADD_ROOM, Ordering::Acquire).expect("Failed to enter room?");

        //Claim the spot we want to add to as ours
        let prev_tail = self.tail.fetch_add(1, Ordering::Relaxed);

        let head = self.head.load(Ordering::Relaxed);

        //We have not exceeded capacity so we can still add the value
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

            //Leave the room using release as we don't want to receive memory updates from other threads
            //But we do want other threads to receive the updates we have written
            self.rooms.leave_blk_ordered(ADD_ROOM, Ordering::Release).expect("Failed to exit room");

            return Result::Ok(());
        }

        self.tail.fetch_sub(1, Ordering::Relaxed);

        //Since we have written nothing, we don't have to synchronize our memory with anyone
        self.rooms.leave_blk_ordered(ADD_ROOM, Ordering::Relaxed);

        Err(QueueError::QueueFull { 0: elem })
    }

    fn pop(&self) -> Option<T> {
        let t: T;

        //Acquire the operations performed by other threads before we have entered the room
        self.rooms.enter_blk_ordered(REM_ROOM, Ordering::Acquire);

        //Claim the spot that we want to remove
        let prev_head = self.head.fetch_add(1, Ordering::Relaxed);

        if prev_head < self.tail.load(Ordering::Relaxed) {
            let pos = prev_head % self.capacity();

            unsafe {
                let array_mut = &mut *self.array.get();

                t = array_mut.get_mut(pos).unwrap().take().unwrap();
            }

            //Register all our changes into other threads
            self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Release);

            self.is_full.store(false, Ordering::Relaxed);

            Some(t)
        } else {
            //The location does not contain an element so we release our hold onto that location
            self.head.fetch_sub(1, Ordering::Relaxed);

            self.rooms.leave_blk(REM_ROOM);

            None
        }
    }

    fn dump(&self, vec: &mut Vec<T>) -> Result<usize, QueueError<T>> {
        if vec.capacity() < self.capacity() {
            return Err(MalformedInputVec);
        }

        //Acquire the
        self.rooms.enter_blk_ordered(REM_ROOM, Ordering::Acquire);

        //Since we are in a remove room we know the tail is not going to be altered
        let current_tail = self.tail.load(Ordering::Relaxed);

        let prev_head = self.head.swap(current_tail, Ordering::Relaxed);

        let count = current_tail - prev_head;

        if count > 0 {
            unsafe {
                let x = &mut *self.array.get();

                //Move the values into the new vector
                for pos in prev_head..current_tail {
                    vec.push(x.get_mut(pos % self.capacity()).unwrap().take().unwrap());
                }
            }

            self.is_full.store(false, Relaxed);

            self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Release);
        } else {
            self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Relaxed);
        }


        return Ok(count);
    }
}

impl<T> BQueue<T> for LFBRArrayQueue<T> where T: Debug {
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

            self.rooms.enter_blk_ordered(ADD_ROOM, Ordering::Acquire);

            //Claim the spot we want to add to as ours
            //Use Release since we want the modifications made to the array to be visible to
            //Readers, not particularly writers (As in we don't need to know the updated state of the
            //Previous writer, since we aren't going to be accessing that memory anyways)
            let prev_tail = self.tail.fetch_add(1, Ordering::Relaxed);

            //Use acquire to perform acquire-release ordering with the previous reader,
            //So we have access to the modifications the previous reader did to the memory
            let head = self.head.load(Ordering::Relaxed);

            if prev_tail - head < self.capacity() {
                unsafe {
                    let array_mut = &mut *self.array.get();

                    array_mut.get_mut(prev_tail as usize % self.capacity()).unwrap().insert(elem);
                }

                self.rooms.leave_blk_ordered(ADD_ROOM, Ordering::Release);

                //In case the element we have inserted is the last one,
                //Close the door behind us
                if (prev_tail - head) + 1 >= self.capacity() {
                    self.is_full.store(true, Ordering::Relaxed);
                }

                break;
            }

            self.tail.fetch_sub(1, Ordering::Relaxed);

            //We do not write anything so we don't have to synchronize any memory
            self.rooms.leave_blk_ordered(ADD_ROOM, Ordering::Relaxed);

            backoff.snooze();
        }
    }

    fn pop_blk(&self) -> T {
        let t: T;
        let backoff = Backoff::new();

        loop {
            self.rooms.enter_blk_ordered(REM_ROOM, Ordering::Acquire);

            let prev_head = self.head.fetch_add(1, Ordering::Relaxed);

            if prev_head < self.tail.load(Ordering::Relaxed) {
                let pos = prev_head % self.capacity();

                unsafe {
                    let array_mut = &mut *self.array.get();

                    t = array_mut.get_mut(pos).unwrap().take().unwrap();
                }

                self.is_full.store(false, Ordering::Relaxed);

                self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Release);

                break;
            } else {
                self.head.fetch_sub(1, Ordering::Relaxed);

                self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Relaxed);

                backoff.snooze();
            }
        }

        return t;
    }
}