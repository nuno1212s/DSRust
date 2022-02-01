use std::fmt::Debug;

use crossbeam_utils::Backoff;
use parking_lot::{Condvar, Mutex};
use crate::queues::queues::{BQueue, Queue, QueueError, SizableQueue};

struct QueueData<T> {
    array: Vec<Option<T>>,
    head: usize,
    size: usize,
}

impl<T> QueueData<T> {
    fn new(vec: Vec<Option<T>>) -> Self {
        Self {
            array: vec,
            head: 0,
            size: 0,
        }
    }

    fn head(&self) -> usize {
        self.head
    }

    fn size(&self) -> usize {
        self.size
    }

    fn array(&mut self) -> &mut Vec<Option<T>> {
        &mut self.array
    }
}

pub struct MQueue<T> {
    capacity: usize,
    array: Mutex<QueueData<T>>,
    //Notifier for when the queue is full and we are trying to insert
    full_notifier: Condvar,
    //Notifier for when the queue is empty and we are trying to remove
    empty_notifier: Condvar,
    backoff: bool,
}

impl<T> MQueue<T> {
    pub fn new(capacity: usize, use_backoff: bool) -> Self {
        let mut vec = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            vec.push(Option::None);
        }

        Self {
            capacity,
            array: Mutex::new(QueueData::new(vec)),
            full_notifier: Condvar::new(),
            empty_notifier: Condvar::new(),
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
    fn size(&self) -> usize {
        if self.backoff() {
            let backoff = Backoff::new();

            loop {
                let result = self.array.try_lock();

                match result {
                    Some(lock_guard) => {
                        return lock_guard.size();
                    }

                    None => {}
                }

                backoff.spin();
            }
        } else {
            let guard = self.array.lock();

            return guard.size();
        }
    }
}

impl<T> Queue<T> for MQueue<T> where T: Debug {
    fn enqueue(&self, elem: T) -> Result<(), QueueError<T>> {
        if self.backoff() {
            let backoff = Backoff::new();

            loop {
                let lock_res = self.array.try_lock();

                match lock_res {
                    Some(mut lock_guard) => {
                        //We increment and then decrement in case it overflows because most of the time we
                        //Assume the operation is going to be completed successfully, so
                        //The size will not need to be decremented, so a single item is fine
                        let size = lock_guard.size();

                        if size >= self.capacity() {
                            return Err(QueueError::QueueFull{0: elem});
                        }

                        let head = lock_guard.head();

                        lock_guard.size += 1;

                        let vector = lock_guard.array();

                        let index = (head + size) as usize % self.capacity();

                        vector.get_mut(index).unwrap()
                            .insert(elem);

                        drop(lock_guard);

                        break;
                    }
                    None => {}
                }

                backoff.spin();
            }

            Ok(())
        } else {
            let mut lock_guard = self.array.lock();

            let size = lock_guard.size();

            if size >= self.capacity() {
                return Err(QueueError::QueueFull{0: elem});
            }

            let head = lock_guard.head();

            lock_guard.size += 1;

            lock_guard.array().get_mut((size + head) % self.capacity()).unwrap()
                .insert(elem);

            //Since we have added more members to the queue, notify the threads that
            //Are waiting for members to remove
            self.empty_notifier.notify_all();

            Ok(())
        }
    }

    fn pop(&self) -> Option<T> {
        if self.backoff() {
            let backoff = Backoff::new();

            loop {
                let lock_result = self.array.try_lock();

                match lock_result {
                    Some(mut lock_guard) => {

                        //We increment and then decrement in case it overflows because most of the time we
                        //Assume the operation is going to be completed successfully, so
                        //The size will not need to be decremented, so a single item is fine
                        let size = lock_guard.size();

                        if size <= 0 {
                            return None;
                        }

                        let head = lock_guard.head();

                        lock_guard.head = (head + 1) % self.capacity();
                        lock_guard.size -= 1;

                        let elem = lock_guard.array().get_mut(head).unwrap().take();

                        drop(lock_guard);

                        return elem;
                    }
                    None => {}
                }

                backoff.spin();
            }
        } else {
            let mut lock_guard = self.array.lock();

            let size = lock_guard.size();

            if size <= 0 {
                return None;
            }

            let head = lock_guard.head();

            lock_guard.head = (head + 1) % self.capacity();
            lock_guard.size -= 1;

            let result = lock_guard.array().get_mut(head)
                .unwrap().take();

            //Since we have removed members from the queue
            self.full_notifier.notify_all();

            result
        }
    }

    fn dump(&self, vec: &mut Vec<T>) -> Result<usize, QueueError<T>> {
        if vec.capacity() < self.capacity() {
            return Err(QueueError::MalformedInputVec);
        }

        if self.backoff() {
            let backoff = Backoff::new();

            loop {
                match self.array.try_lock() {
                    Some(mut lock_guard) => {
                        let size = lock_guard.size();

                        if size <= 0 {
                            return Ok(0);
                        }

                        let mut head = lock_guard.head();

                        let to_remove= lock_guard.size;

                        let mut cur_size = size;

                        for _ in 0..to_remove {
                            vec.push(lock_guard.array().get_mut(head).unwrap().take().unwrap());

                            head = (head + 1) % self.capacity();
                            cur_size -= 1;
                        }

                        lock_guard.head = head;
                        lock_guard.size = cur_size;

                        drop(lock_guard);

                        return Ok(to_remove);
                    }
                    None => {}
                }

                backoff.spin();
            }
        } else {
            let mut lock_guard = self.array.lock();

            let size = lock_guard.size();

            if size <= 0 {
                return Ok(0);
            }

            let mut head = lock_guard.head();

            let to_remove = lock_guard.size();
            let mut cur_size = size;

            for _ in 0..to_remove {
                vec.push(lock_guard.array().get_mut(head).unwrap().take().unwrap());

                head = (head + 1) % self.capacity();
                cur_size -= 1;
            }

            lock_guard.head = head;
            lock_guard.size = cur_size;

            self.full_notifier.notify_all();

            return Ok(to_remove);
        }
    }
}

impl<T> BQueue<T> for MQueue<T> where T:Debug{
    fn enqueue_blk(&self, elem: T) {
        if self.backoff() {
            let backoff = Backoff::new();

            loop {
                let lock_res = self.array.try_lock();

                match lock_res {
                    Some(mut lock_guard) => {
                        let size = lock_guard.size();

                        if size >= self.capacity() {
                            drop(lock_guard);

                            backoff.snooze();

                            continue;
                        }

                        lock_guard.size += 1;

                        let head = lock_guard.head();

                        lock_guard.array().get_mut((head + size) % self.capacity()).unwrap().insert(elem);

                        drop(lock_guard);
                        break;
                    }
                    None => {}
                }

                backoff.spin();
            }
        } else {
            let mut lock_guard = self.array.lock();

            let mut size = lock_guard.size();

            while size >= self.capacity() {
                self.full_notifier.wait(&mut lock_guard);

                size = lock_guard.size();
            }

            let head = lock_guard.head();
            lock_guard.size += 1;

            lock_guard.array().get_mut((size + head) % self.capacity()).unwrap().insert(elem);

            //Notify the threads that are waiting for the notification to pop an item
            //We notify_all because since the OS is not deterministic and if it only wakes up a thread
            //That is waiting for space to enqueue we will be here forever
            //Notify the threads that are waiting to remove items
            self.empty_notifier.notify_all();
        }
    }

    fn pop_blk(&self) -> T {
        if self.backoff() {
            let backoff = Backoff::new();

            loop {
                let lock_result = self.array.try_lock();

                match lock_result {
                    Some(mut lock_guard) => {
                        let size = lock_guard.size();

                        if size <= 0 {
                            drop(lock_guard);

                            backoff.snooze();

                            continue;
                        }

                        let head = lock_guard.head();

                        lock_guard.head = (head + 1) % self.capacity();
                        lock_guard.size -= 1;

                        let elem = lock_guard.array().get_mut(head).unwrap().take();

                        drop(lock_guard);

                        return elem.unwrap();
                    }

                   None => {}
                }

                backoff.spin();
            }
        } else {
            let mut lock_guard = self.array.lock();

            //We increment and then decrement in case it overflows because most of the time we
            //Assume the operation is going to be completed successfully, so
            //The size will not need to be decremented, so a single item is fine
            let mut size = lock_guard.size();

            while size <= 0 {
                self.empty_notifier.wait(&mut lock_guard);

                size = lock_guard.size();
            }

            let head = lock_guard.head();

            lock_guard.head = (head + 1) % self.capacity();
            lock_guard.size -= 1;

            let result = lock_guard.array().get_mut(head)
                .unwrap().take();

            //Notify the threads that are waiting to enqueue an item
            self.full_notifier.notify_all();

            result.unwrap()
        }
    }
}