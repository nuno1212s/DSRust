use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};

use crossbeam_utils::{Backoff, CachePadded};

use crate::queues::queues::{BQueue, Queue, QueueError, SizableQueue};

struct StorageSlot<T> {
    /// The current sequence.
    ///
    /// If the sequence equals the tail, this node will be next written to. If it equals head + 1,
    /// this node will be next read from.
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> StorageSlot<T> {
    pub fn new(index: usize) -> Self {
        Self {
            sequence: AtomicUsize::new(index),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

///An array based queue with no blocking (unlike the rooms in the rooms_array_queue)
///! Source:
///!   - <http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>
/// This implementation is based on -
/// <https://github.com/crossbeam-rs/crossbeam/blob/master/crossbeam-queue/src/array_queue.rs>
/// But we alter it by adding a safe dump() method which attempts to dump a lot of
/// Items contained in the queue at a time to improve performance in mpsc scenarios like batching
pub struct LFBQueue<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    array: Box<[StorageSlot<T>]>,
    capacity: usize,
    one_lap: usize,
}

unsafe impl<T> Sync for LFBQueue<T> {}

unsafe impl<T> Send for LFBQueue<T> {}

impl<T> LFBQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let buffer: Box<[StorageSlot<T>]> = (0..capacity)
            .map(|i| { StorageSlot::new(i) })
            .collect();

        let one_lap = (capacity + 1).next_power_of_two();

        Self {
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            array: buffer,
            one_lap,
            capacity,
        }
    }

    fn extract_index_of(&self, stamp: usize) -> usize {
        //The reasoning behind self.one_lap -1 is as follows
        //one_lap is the next power of two over the capacity
        //So basically it allocates the necessary bits for index
        //And the rest of the bits are for laps
        //one_lap contains the first bit of the lap bits set to 1
        //When we subtract one, what we get is all bits before that
        //bit set to 1, and all index bits set to 0, meaning we get the
        //index we want
        stamp & (self.one_lap() - 1)
    }

    fn extract_lap_of(&self, stamp: usize) -> usize {
        stamp & !(self.one_lap() - 1)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn one_lap(&self) -> usize {
        self.one_lap
    }
}

impl<T> SizableQueue for LFBQueue<T> {
    fn size(&self) -> usize {
        loop {
            let tail = self.tail.load(SeqCst);
            let head = self.head.load(SeqCst);

            if self.tail.load(SeqCst) == tail {
                let head_index = self.extract_index_of(head);
                let tail_index = self.extract_index_of(tail);

                if head_index < tail_index {
                    return tail_index - head_index;
                } else if head_index > tail_index {
                    return (self.capacity - head_index) + tail_index;
                } else if tail == head {
                    return 0;
                } else {
                    //This means the tail_index == head_index but
                    //The lap of tail is one more than the lap of the head,
                    //So we have capacity members inside the buffer
                    return self.capacity;
                }
            }
        }
    }

    fn capacity(&self) -> Option<usize> {
        Some(self.capacity)
    }

    fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);

        tail == head
    }

    fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);

        head.wrapping_add(self.one_lap()) == tail
    }
}

///Non blocking implementation for ArrayBQueue
impl<T> Queue<T> for LFBQueue<T> where {
    fn enqueue(&self, elem: T) -> Result<(), QueueError<T>> {
        let backoff = Backoff::new();

        let mut tail = self.tail.load(Relaxed);

        loop {
            let tail_index = self.extract_index_of(tail);
            let tail_lap = self.extract_lap_of(tail);

            let slot = unsafe { self.array.get_unchecked(tail_index) };

            let slot_sequence = slot.sequence.load(Ordering::Acquire);

            //If the slot stamp == tail, it's the current slot we want to write to
            if slot_sequence == tail {
                let new_tail = if tail_index + 1 < self.capacity() {
                    tail + 1
                } else {
                    //Add one lap to the tail lap
                    //This will set the index back to 0
                    tail_lap.wrapping_add(self.one_lap)
                };

                match self.tail.compare_exchange_weak(tail, new_tail, SeqCst, Relaxed) {
                    Ok(_) => {
                        unsafe {
                            slot.value.get().write(MaybeUninit::new(elem));
                        }

                        //This slot will be read from when the head reaches the tail + 1 value,
                        //With the same laps as the tail had at this moment

                        //We use tail + 1 instead of the wrapped new_tail
                        //Because when we pop we check if head + 1 == sequence,
                        //So if the head is
                        slot.sequence.store(tail + 1, Release);

                        return Ok(());
                    }
                    Err(current_t) => {
                        tail = current_t;
                        backoff.spin();
                    }
                }
            } else if slot_sequence.wrapping_add(self.one_lap()) == tail + 1 {
                atomic::fence(Ordering::SeqCst);
                //
                let head = self.head.load(Relaxed);

                if head.wrapping_add(self.one_lap()) == tail {
                    //This means that head_index == tail_index and that the head is one lap ahead of the tail
                    //Meaning the queue is completely full
                    return Err(QueueError::QueueFull { 0: elem });
                }

                backoff.spin();
                tail = self.tail.load(Relaxed);
            } else {
                backoff.snooze();
                tail = self.tail.load(Relaxed);
            }
        }
    }

    fn pop(&self) -> Option<T> {
        let backoff = Backoff::new();

        let mut head = self.head.load(Relaxed);

        loop {
            let head_index = self.extract_index_of(head);
            let head_lap = self.extract_lap_of(head);

            let slot = unsafe { self.array.get_unchecked(head_index) };

            let sequence = slot.sequence.load(Acquire);

            if head + 1 == sequence {
                let new_head = if head_index + 1 < self.capacity() {
                    head + 1
                } else {
                    head_lap.wrapping_add(self.one_lap)
                };

                match self.head.compare_exchange_weak(
                    head, new_head,
                    SeqCst,
                    Relaxed,
                ) {
                    Ok(_) => {
                        let elem = unsafe { slot.value.get().read().assume_init() };

                        //This slot will now only be modified when the tail reaches around to it
                        slot.sequence.store(head.wrapping_add(self.one_lap()), Release);

                        return Some(elem);
                    }
                    Err(current_head) => {
                        head = current_head;

                        backoff.spin();
                    }
                }
            } else if sequence == head {
                //We have marked the value as being read but we have actually
                //Still not removed the value from the memory location
                //(this is evident as when the item is moved, we store the correct
                //sequence number)

                atomic::fence(Ordering::SeqCst);

                let tail = self.tail.load(Relaxed);

                //Queue is empty
                if tail == head {
                    return None;
                }

                //Wait for the thread to finish reading the value and then try again to read a value
                backoff.spin();
                head = self.head.load(Relaxed);
            } else {
                backoff.snooze();

                head = self.head.load(Relaxed);
            }
        }
    }

    fn dump(&self, vec: &mut Vec<T>) -> Result<usize, QueueError<T>> {
        let backoff = Backoff::new();

        let mut prev_head = self.head.load(Relaxed);

        loop {
            let tail = self.tail.load(Relaxed);

            //This is safe since the head can never be ahead of the tail as it has not moved
            //What can happen is that we might not capture all of the items that are contained
            //At this moment since tail could have been incremented since we have loaded it
            //But by ensuring the head is still the same, we assure that
            //We can never have a situation where we move the head backwards.
            match self.head.compare_exchange_weak(prev_head, tail,
                                                  SeqCst,
                                                  Relaxed) {
                Ok(_) => {}
                Err(curr_head) => {
                    prev_head = curr_head;
                    backoff.spin();

                    continue;
                }
            }

            if prev_head == tail {
                return Ok(0);
            }

            let mut collected = 0;

            loop {
                if prev_head == tail {
                    break;
                }

                let head_ind = self.extract_index_of(prev_head);
                let head_lap = self.extract_lap_of(prev_head);

                let slot = unsafe { self.array.get_unchecked(head_ind) };

                let sequence = slot.sequence.load(Acquire);


                if prev_head + 1 == sequence {
                    let new_head = if head_ind + 1 < self.capacity() {
                        prev_head + 1
                    } else {
                        head_lap.wrapping_add(self.one_lap())
                    };

                    let elem = unsafe { slot.value.get().read().assume_init() };

                    slot.sequence.store(prev_head.wrapping_add(self.one_lap()), Ordering::Release);

                    vec.push(elem);

                    collected += 1;

                    prev_head = new_head;
                } else if sequence == prev_head {
                    atomic::fence(SeqCst);

                    //Wait for the other threads to finish writing in this slot
                    backoff.spin();
                } else {
                    //Wait for the other threads to update the sequence number
                    backoff.snooze();
                }
            }

            return Ok(collected);
        }
    }
}

impl<T> BQueue<T> for LFBQueue<T> where {
    fn enqueue_blk(&self, elem: T) {
        let backoff = Backoff::new();

        'outer: loop {
            let mut tail = self.tail.load(Relaxed);

            loop {
                let tail_index = self.extract_index_of(tail);
                let tail_lap = self.extract_lap_of(tail);

                let slot = unsafe { self.array.get_unchecked(tail_index) };

                let slot_stamp = slot.sequence.load(Ordering::Acquire);

                //If the slot stamp == tail, it's the current slot we want to write to
                if slot_stamp == tail {
                    let new_tail = if tail_index + 1 < self.capacity {
                        tail + 1
                    } else {
                        //Add one lap to the tail lap
                        //This will set the index back to 0
                        tail_lap.wrapping_add(self.one_lap)
                    };

                    match self.tail.compare_exchange_weak(tail, new_tail, SeqCst, Relaxed) {
                        Ok(_) => {
                            unsafe {
                                slot.value.get().write(MaybeUninit::new(elem));
                            }

                            //This slot will be read from when the head reaches the tail + 1 value,
                            //With the same laps as the tail had at this moment

                            //We use tail + 1 instead of the wrapped new_tail
                            //Because when we pop we check if head + 1 == sequence,
                            //So if the head is
                            slot.sequence.store(tail + 1, Release);

                            break 'outer;
                        }
                        Err(current_t) => {
                            tail = current_t;
                            backoff.spin();
                        }
                    }
                } else if slot_stamp.wrapping_add(self.one_lap()) == tail + 1 {
                    atomic::fence(Ordering::SeqCst);
                    //
                    let head = self.head.load(Relaxed);

                    if head.wrapping_add(self.one_lap()) == tail {
                        //This means that head_index == tail_index and that the head is one lap ahead of the tail
                        //Meaning the queue is completely full

                        //We block here until there is an available space to place our item
                        backoff.snooze();
                        continue 'outer;
                    }

                    backoff.spin();
                    tail = self.tail.load(Relaxed);
                } else {
                    backoff.snooze();
                    tail = self.tail.load(Relaxed);
                }
            }
        }
    }

    fn pop_blk(&self) -> T {
        let backoff = Backoff::new();

        'outer: loop {
            let mut head = self.head.load(Relaxed);

            loop {
                let head_index = self.extract_index_of(head);
                let head_lap = self.extract_lap_of(head);

                let slot = unsafe { self.array.get_unchecked(head_index) };

                let sequence = slot.sequence.load(Acquire);

                if head + 1 == sequence {
                    let new_head = if head_index + 1 < self.capacity() {
                        head + 1
                    } else {
                        head_lap.wrapping_add(self.one_lap)
                    };

                    match self.head.compare_exchange_weak(
                        head, new_head,
                        SeqCst,
                        Relaxed,
                    ) {
                        Ok(_) => {
                            let elem = unsafe { slot.value.get().read().assume_init() };

                            //This slot will now only be modified when the tail reaches around to it
                            slot.sequence.store(head.wrapping_add(self.one_lap()), Release);

                            return elem;
                        }
                        Err(current_head) => {
                            head = current_head;

                            backoff.spin();
                        }
                    }
                } else if sequence == head {
                    //We have marked the value as being read but we have actually
                    //Still not removed the value from the memory location
                    //(this is evident as when the item is moved, we store the correct
                    //sequence number)

                    atomic::fence(Ordering::SeqCst);

                    let tail = self.tail.load(Relaxed);

                    //Queue is empty
                    if tail == head {
                        //We block here until there is a member available to be popped
                        backoff.snooze();

                        continue 'outer;
                    }

                    //Wait for the thread to finish reading the value and then try again to read a value
                    backoff.spin();
                    head = self.head.load(Relaxed);
                } else {
                    backoff.snooze();

                    head = self.head.load(Relaxed);
                }
            }
        }
    }
}

impl<T> Drop for LFBQueue<T> {
    fn drop(&mut self) {
        let head_index = self.extract_index_of(self.head.load(Relaxed));

        // Loop over all items currently in the queue and drop them.
        for i in 0..self.size() {
            // Compute the index of the next slot holding a message.
            let index = if head_index + i < self.capacity() {
                head_index + i
            } else {
                head_index + i - self.capacity()
            };

            unsafe {
                let slot = self.array.get_unchecked_mut(index);
                let value = &mut *slot.value.get();
                value.as_mut_ptr().drop_in_place();
            }
        }
    }
}