use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_utils::{Backoff, CachePadded};

use crate::queues::queues::{AsyncQueue, BQueue, Queue, QueueError, SizableQueue};
use crate::queues::queues::QueueError::MalformedInputVec;
use crate::utils::memory_access::UnsafeWrapper;
use crate::utils::rooms::Rooms;

const SIZE_ROOM: i32 = 1;
const ADD_ROOM: i32 = 2;
const REM_ROOM: i32 = 3;

///A bounded queue with rooms and exponential backoff to prevent over contention when
///Working with many concurrent threads
///
/// The capacity can only be a power of two, because of the overflow in the head and tail indexes,
/// which are strictly ascending. By making capacity always a power of two, when the overflow happens
/// the index % capacity will stay the same and therefore the queue will continue to function
/// as expected without adding a performance penalty with another load operation and spin lock
/// to utilize the laps alternative.
pub struct LFBRArrayQueue<T> {
    array: UnsafeWrapper<Vec<Option<T>>>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    rooms: Rooms,
    is_full: AtomicBool,
    capacity: usize,
}

unsafe impl<T> Sync for LFBRArrayQueue<T> {}

impl<T> LFBRArrayQueue<T> {
    pub fn new(expected_size: usize) -> Self {
        //Always use the next power of two, so when we have overflow on the
        //Head and tail, since we always % capacity, the indexes won't be
        //Screwed up!
        let capacity = if !expected_size.is_power_of_two() {
            expected_size.next_power_of_two()
        } else {
            expected_size
        };

        let mut vec = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            vec.push(Option::None);
        }

        Self {
            array: UnsafeWrapper::new(vec),
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            rooms: Rooms::new(3),
            is_full: AtomicBool::new(false),
            capacity,
        }
    }

    fn inner_cap(&self) -> usize {
        self.capacity
    }
}

impl<T> SizableQueue for LFBRArrayQueue<T> {
    fn size(&self) -> usize {
        //We use acquire because we want to get the latest values from the other threads, but we won't change anything
        //So the store part can be Relaxed
        self.rooms.enter_blk_ordered(SIZE_ROOM, Ordering::Relaxed).expect("Failed to enter room?");

        let size = self.tail.load(Ordering::Relaxed) - self.head.load(Ordering::Relaxed);

        //Since we perform no changes, we can leave using the relaxed state
        self.rooms.leave_blk_ordered(SIZE_ROOM, Ordering::Relaxed).expect("Failed to exit room");

        size
    }

    fn capacity(&self) -> Option<usize> {
        Some(self.capacity)
    }

    fn is_empty(&self) -> bool {
        self.rooms.enter_blk_ordered(SIZE_ROOM, Ordering::Relaxed).unwrap();

        let size = self.tail.load(Ordering::Relaxed) - self.head.load(Ordering::Relaxed);

        //Since we perform no changes, we can leave using the relaxed state
        self.rooms.leave_blk_ordered(SIZE_ROOM, Ordering::Relaxed).expect("Failed to exit room");

        size == 0
    }

    fn is_full(&self) -> bool {
        self.rooms.enter_blk_ordered(SIZE_ROOM, Ordering::Relaxed).unwrap();

        let size = self.tail.load(Ordering::Relaxed) - self.head.load(Ordering::Relaxed);

        self.rooms.leave_blk_ordered(SIZE_ROOM, Ordering::Relaxed).unwrap();

        size == self.inner_cap()
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
impl<T> Queue<T> for LFBRArrayQueue<T> {
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
        if prev_tail - head < self.inner_cap() {
            unsafe {
                let array_mut = &mut *self.array.get();

                *array_mut.get_mut(prev_tail as usize % self.inner_cap()).unwrap() = Some(elem);
            }

            //In case the element we have inserted is the last one,
            //Close the door behind us
            if (prev_tail - head) + 1 >= self.inner_cap() {
                self.is_full.store(true, Ordering::Relaxed);
            }

            //Leave the room using release as we don't want to receive memory updates from other threads
            //But we do want other threads to receive the updates we have written
            self.rooms.leave_blk_ordered(ADD_ROOM, Ordering::Release).expect("Failed to exit room");

            return Result::Ok(());
        }

        self.tail.fetch_sub(1, Ordering::Relaxed);

        //Since we have written nothing, we don't have to synchronize our memory with anyone
        self.rooms.leave_blk_ordered(ADD_ROOM, Ordering::Relaxed).unwrap();

        Err(QueueError::QueueFull { 0: elem })
    }

    fn pop(&self) -> Option<T> {
        let t: T;

        //Acquire the operations performed by other threads before we have entered the room
        self.rooms.enter_blk_ordered(REM_ROOM, Ordering::Acquire).unwrap();

        //Claim the spot that we want to remove
        let prev_head = self.head.fetch_add(1, Ordering::Relaxed);

        if prev_head < self.tail.load(Ordering::Relaxed) {
            let pos = prev_head % self.inner_cap();

            unsafe {
                let array_mut = &mut *self.array.get();

                t = array_mut.get_mut(pos).unwrap().take().unwrap();
            }

            //Register all our changes into other threads
            self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Release).unwrap();

            self.is_full.store(false, Ordering::Relaxed);

            Some(t)
        } else {
            //The location does not contain an element so we release our hold onto that location
            self.head.fetch_sub(1, Ordering::Relaxed);

            self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Relaxed).unwrap();

            None
        }
    }

    fn dump(&self, vec: &mut Vec<T>) -> Result<usize, QueueError<T>> {
        if vec.capacity() < self.inner_cap() {
            return Err(MalformedInputVec);
        }

        //Acquire the room
        self.rooms.enter_blk_ordered(REM_ROOM, Ordering::Acquire).unwrap();

        //Since we are in a remove room we know the tail is not going to be altered
        let current_tail = self.tail.load(Ordering::Relaxed);

        let prev_head = self.head.swap(current_tail, Ordering::Relaxed);

        let count = current_tail - prev_head;

        if count > 0 {
            unsafe {
                let x = &mut *self.array.get();

                //Move the values into the new vector
                for pos in prev_head..current_tail {
                    vec.push(x.get_mut(pos % self.inner_cap()).unwrap().take().unwrap());
                }
            }

            self.is_full.store(false, Ordering::Relaxed);

            self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Release).unwrap();
        } else {
            self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Relaxed).unwrap();
        }


        return Ok(count);
    }
}

impl<T> AsyncQueue<T> for LFBRArrayQueue<T> {
    async fn enqueue_async(&self, elem: T) -> Result<(), QueueError<T>> {
        if self.is_full.load(Ordering::Relaxed) {
            return Result::Err(QueueError::QueueFull { 0: elem });
        }

        self.rooms.enter_async_ordered(ADD_ROOM, Ordering::Acquire).await.unwrap();

        //Claim the spot we want to add to as ours
        let prev_tail = self.tail.fetch_add(1, Ordering::Relaxed);

        let head = self.head.load(Ordering::Relaxed);

        //We have not exceeded capacity so we can still add the value
        if prev_tail - head < self.inner_cap() {
            unsafe {
                let array_mut = &mut *self.array.get();

                *array_mut.get_mut(prev_tail as usize % self.inner_cap()).unwrap() = Some(elem);
            }

            //In case the element we have inserted is the last one,
            //Close the door behind us
            if (prev_tail - head) + 1 >= self.inner_cap() {
                self.is_full.store(true, Ordering::Relaxed);
            }

            //Leave the room using release as we don't want to receive memory updates from other threads
            //But we do want other threads to receive the updates we have written
            self.rooms.leave_async_ordered(ADD_ROOM, Ordering::Release)
                .await.expect("Failed to exit room");

            return Result::Ok(());
        }

        self.tail.fetch_sub(1, Ordering::Relaxed);

        //Since we have written nothing, we don't have to synchronize our memory with anyone
        self.rooms.leave_async_ordered(ADD_ROOM, Ordering::Relaxed).await.unwrap();

        Err(QueueError::QueueFull { 0: elem })
    }

    async fn pop_async(&self) -> Option<T> {
        let t: T;

        //Acquire the operations performed by other threads before we have entered the room
        self.rooms.enter_async_ordered(REM_ROOM, Ordering::Acquire).await.unwrap();

        //Claim the spot that we want to remove
        let prev_head = self.head.fetch_add(1, Ordering::Relaxed);

        if prev_head < self.tail.load(Ordering::Relaxed) {
            let pos = prev_head % self.inner_cap();

            unsafe {
                let array_mut = &mut *self.array.get();

                t = array_mut.get_mut(pos).unwrap().take().unwrap();
            }

            //Register all our changes into other threads
            self.rooms.leave_async_ordered(REM_ROOM, Ordering::Release).await.unwrap();

            self.is_full.store(false, Ordering::Relaxed);

            Some(t)
        } else {
            //The location does not contain an element so we release our hold onto that location
            self.head.fetch_sub(1, Ordering::Relaxed);

            self.rooms.leave_async_ordered(REM_ROOM, Ordering::Relaxed).await.unwrap();

            None
        }
    }

    async fn dump_async(&self, vec: &mut Vec<T>) -> Result<usize, QueueError<T>> {
        if vec.capacity() < self.inner_cap() {
            return Err(MalformedInputVec);
        }

        //Acquire the room
        self.rooms.enter_async_ordered(REM_ROOM, Ordering::Acquire).await.unwrap();

        //Since we are in a remove room we know the tail is not going to be altered
        let current_tail = self.tail.load(Ordering::Relaxed);

        let prev_head = self.head.swap(current_tail, Ordering::Relaxed);

        let count = current_tail - prev_head;

        if count > 0 {
            unsafe {
                let x = &mut *self.array.get();

                //Move the values into the new vector
                for pos in prev_head..current_tail {
                    vec.push(x.get_mut(pos % self.inner_cap()).unwrap().take().unwrap());
                }
            }

            self.is_full.store(false, Ordering::Relaxed);

            self.rooms.leave_async_ordered(REM_ROOM, Ordering::Release).await.unwrap();
        } else {
            self.rooms.leave_async_ordered(REM_ROOM, Ordering::Relaxed).await.unwrap();
        }

        return Ok(count);
    }
}

impl<T> BQueue<T> for LFBRArrayQueue<T> {
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

            self.rooms.enter_blk_ordered(ADD_ROOM, Ordering::Acquire).unwrap();

            //Claim the spot we want to add to as ours
            //Use Release since we want the modifications made to the array to be visible to
            //Readers, not particularly writers (As in we don't need to know the updated state of the
            //Previous writer, since we aren't going to be accessing that memory anyways)
            let prev_tail = self.tail.fetch_add(1, Ordering::Relaxed);

            //Use acquire to perform acquire-release ordering with the previous reader,
            //So we have access to the modifications the previous reader did to the memory
            let head = self.head.load(Ordering::Relaxed);

            if prev_tail - head < self.inner_cap() {
                unsafe {
                    let array_mut = &mut *self.array.get();

                    *array_mut.get_mut(prev_tail as usize % self.inner_cap()).unwrap() = Some(elem);
                }

                self.rooms.leave_blk_ordered(ADD_ROOM, Ordering::Release).unwrap();

                //In case the element we have inserted is the last one,
                //Close the door behind us
                if (prev_tail - head) + 1 >= self.inner_cap() {
                    self.is_full.store(true, Ordering::Relaxed);
                }

                break;
            }

            self.tail.fetch_sub(1, Ordering::Relaxed);

            //We do not write anything so we don't have to synchronize any memory
            self.rooms.leave_blk_ordered(ADD_ROOM, Ordering::Relaxed).unwrap();

            backoff.snooze();
        }
    }

    fn pop_blk(&self) -> T {
        let t: T;
        let backoff = Backoff::new();

        loop {
            self.rooms.enter_blk_ordered(REM_ROOM, Ordering::Acquire).unwrap();

            let prev_head = self.head.fetch_add(1, Ordering::Relaxed);

            if prev_head < self.tail.load(Ordering::Relaxed) {
                let pos = prev_head % self.inner_cap();

                unsafe {
                    let array_mut = &mut *self.array.get();

                    t = array_mut.get_mut(pos).unwrap().take().unwrap();
                }

                self.is_full.store(false, Ordering::Relaxed);

                self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Release).unwrap();

                break;
            } else {
                self.head.fetch_sub(1, Ordering::Relaxed);

                self.rooms.leave_blk_ordered(REM_ROOM, Ordering::Relaxed).unwrap();

                backoff.snooze();
            }
        }

        return t;
    }
}