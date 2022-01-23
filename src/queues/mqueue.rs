use std::sync::{Mutex, TryLockResult};
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU32, Ordering};

use crate::queues::queues::{Queue, QueueError, SizableQueue};
use crate::utils::backoff::Backoff;

pub struct MQueue<T> {
    capacity: usize,
    size: AtomicI64,
    array: Mutex<(Vec<T>, u32)>,
    backoff: bool,
}

impl<T> MQueue<T> {
    pub fn new(capacity: usize, use_backoff: bool) -> Self {
        Self {
            capacity,
            size: AtomicI64::new(0),
            array: Mutex::new((Vec::with_capacity(capacity), 0)),
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
                    Ok(lock_guard) => {
                        let size = self.size.fetch_add(1, Ordering::SeqCst);

                        if size >= self.capacity() as i64 {
                            Backoff::reset();

                            self.size.fetch_sub(1, Ordering::Relaxed);

                            return Err(QueueError::QueueFull);
                        }

                        lock_guard.0.insert((lock_guard.1 + size) % self.capacity(),
                                            elem);

                        break;
                    }
                    Err(Err) => {}
                }

                Backoff::backoff();
            }

            Backoff::reset();

            Ok(())
        } else {
            let size = self.size.fetch_add(1, Ordering::SeqCst);

            if size >= self.capacity() as i64 {
                self.size.fetch_sub(1, Ordering::Relaxed);

                return Err(QueueError::QueueFull);
            }

            let lock_guard = self.array.lock().unwrap();

            lock_guard.0.insert((size + lock_guard.1) % self.capacity(), elem);

            Ok(())
        }
    }

    fn pop(&self) -> Option<T> {
        if self.backoff() {
            loop {
                let lock_result = self.array.try_lock();

                match lock_result {
                    Ok(lock_guard) => {
                        let size = self.size.fetch_sub(1, Ordering::SeqCst);

                        if size <= 0 {
                            self.size.fetch_add(1, Ordering::Relaxed);

                            Backoff::reset();

                            return None;
                        }

                        let head = lock_guard.1;

                        *lock_guard.1 = (head + 1) % self.capacity();

                        let elem = lock_guard.0.remove((head + size) % self.capacity());

                        drop(lock_guard);

                        Backoff::reset();

                        return Some(elem);
                    }
                    Err(er) => {}
                }

                Backoff::backoff();
            }
        } else {
            let lock_guard = self.array.lock().unwrap();

            let size = self.size.fetch_sub(1, Ordering::SeqCst);

            if size <= 0 {
                self.size.fetch_add(1, Ordering::Relaxed);

                return None;
            }

            let head = lock_guard.1;

            *lock_guard.1 = (head + 1) % self.capacity();

            lock_guard.0.remove((head + size) % self.capacity());

            None
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

                    let array = &mut lock_guard.0;

                    let mut head = lock_guard.1;

                    let mut current_size: i64 = self.size.fetch_sub(1, Ordering::Relaxed) ;

                    loop {
                        vec.push(array.remove((head + current_size) % self.capacity()));

                        head = (head + 1) % self.capacity();
                        current_size = self.size.fetch_sub(1, Ordering::Relaxed);

                        if current_size <= 1 {
                            break
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