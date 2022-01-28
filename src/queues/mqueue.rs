use std::sync::{Condvar, Mutex, TryLockResult};
use std::sync::atomic::{AtomicI64, Ordering};

use crate::queues::queues::{BQueue, Queue, QueueError, SizableQueue};
use crate::utils::backoff::Backoff;

pub struct MQueue<T> {
    capacity: usize,
    size: AtomicI64,
    array: Mutex<(Vec<T>, i64)>,
    lock_notifier: Condvar,
    backoff: bool,
}

impl<T> MQueue<T> {
    pub fn new(capacity: usize, use_backoff: bool) -> Self {
        Self {
            capacity,
            size: AtomicI64::new(0),
            array: Mutex::new((Vec::with_capacity(capacity), 0)),
            lock_notifier: Condvar::new(),
            backoff: use_backoff,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn backoff(&self) -> bool {
        self.backoff
    }
}

impl<T> SizableQueue for MQueue<T> {
    fn size(&self) -> u32 {
        return self.size.load(Ordering::Relaxed) as u32;
    }
}

impl<T> Queue<T> for MQueue<T> {
    fn enqueue(&self, elem: T) -> Result<(), QueueError> {
        if self.backoff() {
            loop {
                let lock_res = self.array.try_lock();

                match lock_res {
                    Ok(mut lock_guard) => {
                        //We increment and then decrement in case it overflows because most of the time we
                        //Assume the operation is going to be completed successfully, so
                        //The size will not need to be decremented, so a single item is fine
                        let size = self.size.fetch_add(1, Ordering::SeqCst);

                        if size >= self.capacity() as i64 {
                            Backoff::reset();

                            self.size.fetch_sub(1, Ordering::Relaxed);

                            return Err(QueueError::QueueFull);
                        }

                        let head = lock_guard.1;

                        let mut vector = &mut lock_guard.0;

                        vector.insert((head + size) as usize % self.capacity(),
                                      elem);

                        break;
                    }
                    Err(_er) => {}
                }

                Backoff::backoff();
            }

            Backoff::reset();

            Ok(())
        } else {
            let mut lock_guard = self.array.lock().unwrap();

            //Attempt to reserve the slot in the size.
            //We increment and then decrement in case it overflows because most of the time we
            //Assume the operation is going to be completed successfully, so
            //The size will not need to be decremented, so a single item is fine
            let size = self.size.fetch_add(1, Ordering::SeqCst);

            if size >= self.capacity() as i64 {
                self.size.fetch_sub(1, Ordering::Relaxed);

                return Err(QueueError::QueueFull);
            }

            let head = lock_guard.1;

            (&mut lock_guard.0).insert((size + head) as usize % self.capacity(), elem);

            Ok(())
        }
    }

    fn pop(&self) -> Option<T> {
        if self.backoff() {
            loop {
                let mut lock_result = self.array.try_lock();

                match lock_result {
                    Ok(mut lock_guard) => {

                        //We increment and then decrement in case it overflows because most of the time we
                        //Assume the operation is going to be completed successfully, so
                        //The size will not need to be decremented, so a single item is fine
                        let size = self.size.fetch_sub(1, Ordering::SeqCst);

                        if size <= 0 {
                            self.size.fetch_add(1, Ordering::Relaxed);

                            Backoff::reset();

                            return None;
                        }

                        let head = lock_guard.1;

                        lock_guard.1 = (head + 1) % self.capacity() as i64;

                        let elem = lock_guard.0.remove((head + size) as usize % self.capacity());

                        drop(lock_guard);

                        Backoff::reset();

                        return Some(elem);
                    }
                    Err(_er) => {}
                }

                Backoff::backoff();
            }
        } else {
            let mut lock_guard = self.array.lock().unwrap();

            //We increment and then decrement in case it overflows because most of the time we
            //Assume the operation is going to be completed successfully, so
            //The size will not need to be decremented, so a single item is fine
            let size = self.size.fetch_sub(1, Ordering::SeqCst);

            if size <= 0 {
                self.size.fetch_add(1, Ordering::Relaxed);

                return None;
            }

            let head = lock_guard.1;

            lock_guard.1 = (head + 1) % self.capacity() as i64;

            Some(lock_guard.0.remove((head + size) as usize % self.capacity()))
        }
    }

    fn dump(&self, count: usize) -> Vec<T> {
        if self.backoff() {
            let mut vec = Vec::with_capacity(count);

            match self.array.try_lock() {
                Ok(mut lock_guard) => {
                    let size = self.size.load(Ordering::SeqCst);

                    if size <= 0 {
                        return vec;
                    }

                    let mut head = lock_guard.1;

                    let array = &mut lock_guard.0;

                    let mut current_size: i64 = self.size.fetch_sub(1, Ordering::Relaxed);

                    loop {
                        vec.push(array.remove((head + current_size) as usize % self.capacity()));

                        head = (head + 1) % self.capacity() as i64;
                        current_size = self.size.fetch_sub(1, Ordering::Relaxed);

                        if current_size <= 1 {
                            break;
                        }
                    }

                    return vec;
                }
                Err(_) => {}
            }

            Backoff::backoff();
        }

        Vec::new()
    }
}

impl<T> BQueue<T> for MQueue<T> {
    fn enqueue_blk(&self, elem: T) {
        if self.backoff() {
            loop {
                let mut lock_res = self.array.try_lock();

                match lock_res {
                    Ok(mut lock_guard) => {
                        let size = self.size.fetch_add(1, Ordering::SeqCst);

                        if size >= self.capacity() as i64 {
                            self.size.fetch_sub(1, Ordering::Relaxed);

                            Backoff::backoff();

                            continue;
                        }

                        let head = lock_guard.1;

                        lock_guard.0.insert(((head + size) as usize % self.capacity()),
                                            elem);

                        break;
                    }
                    Err(_er) => {}
                }

                Backoff::backoff();
            }

            Backoff::reset();
        } else {
            let mut lock_guard = self.array.lock().unwrap();

            let mut size = self.size.fetch_add(1, Ordering::Relaxed);

            while size >= self.capacity() as i64 {
                self.size.fetch_sub(1, Ordering::Relaxed);

                lock_guard = self.lock_notifier.wait(lock_guard).unwrap();

                size = self.size.fetch_add(1, Ordering::Relaxed);
            }

            let head = lock_guard.1;

            (&mut lock_guard.0).insert((size + head) as usize % self.capacity(), elem);

            //Notify the threads that are waiting for the notification to pop an item
            //We notify_all because since the OS is not deterministic and if it only wakes up a thread
            //That is waiting for space to enqueue we will be here forever
            self.lock_notifier.notify_all();
        }
    }

    fn pop_blk(&self) -> T {
        if self.backoff() {
            loop {
                let mut lock_result = self.array.try_lock();

                match lock_result {
                    Ok(mut lock_guard) => {

                        //We increment and then decrement in case it overflows because most of the time we
                        //Assume the operation is going to be completed successfully, so
                        //The size will not need to be decremented, so a single item is fine
                        let size = self.size.fetch_sub(1, Ordering::SeqCst);

                        if size <= 0 {
                            self.size.fetch_add(1, Ordering::Relaxed);

                            Backoff::backoff();

                            continue;
                        }

                        let head = lock_guard.1;

                        lock_guard.1 = (head + 1) % self.capacity() as i64;

                        let elem = lock_guard.0.remove((head + size) as usize % self.capacity());

                        drop(lock_guard);

                        Backoff::reset();

                        return elem;
                    }

                    Err(_er) => {}
                }

                Backoff::backoff();
            }
        } else {
            let mut lock_guard = self.array.lock().unwrap();

            //We increment and then decrement in case it overflows because most of the time we
            //Assume the operation is going to be completed successfully, so
            //The size will not need to be decremented, so a single item is fine
            let mut size = self.size.fetch_sub(1, Ordering::Relaxed);

            while size <= 0 {
                self.size.fetch_add(1, Ordering::Relaxed);

                lock_guard = self.lock_notifier.wait(lock_guard).unwrap();

                size = self.size.fetch_sub(1, Ordering::Relaxed);
            }

            let head = lock_guard.1;

            lock_guard.1 = (head + 1) % self.capacity() as i64;

            let result = lock_guard.0.remove((head + size) as usize % self.capacity());

            self.lock_notifier.notify_all();

            result
        }
    }

    fn dump_blk(&self, count: usize) -> Vec<T> {
        todo!()
    }
}