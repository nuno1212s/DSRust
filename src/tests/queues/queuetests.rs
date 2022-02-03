use crossbeam_queue::ArrayQueue;

use crate::queues::queues::{Queue, QueueError, SizableQueue};

impl SizableQueue for ArrayQueue<u32> {
    fn size(&self) -> usize {
        self.len()
    }

    fn capacity(&self) -> Option<usize> {
        Some(self.capacity())
    }
}

impl Queue<u32> for ArrayQueue<u32> {
    fn enqueue(&self, elem: u32) -> Result<(), QueueError<u32>> {
        match self.push(elem) {
            Ok(_) => {
                Ok(())
            }
            Err(e) => {
                Err(QueueError::QueueFull { 0: e })
            }
        }
    }

    fn pop(&self) -> Option<u32> {
        self.pop()
    }

    fn dump(&self, vec: &mut Vec<u32>) -> Result<usize, QueueError<u32>> {
        match self.pop() {
            None => { Ok(0) }
            Some(ele) => {
                vec.push(ele);

                Ok(1)
            }
        }
    }
}

#[cfg(test)]
pub mod queue_tests {
    use std::sync::Arc;
    use std::time::{Instant};

    use crossbeam_queue::ArrayQueue;

    use crate::queues::lf_array_queue::LFBQueue;
    use crate::queues::mqueue::MQueue;
    use crate::queues::queues::{BQueue, Queue};
    use crate::queues::rooms_array_queue::LFBRArrayQueue;

    fn test_single_thread_ordering_for<T>(queue: T, count: u32) where T: Queue<u32> {
        for i in 0..count {
            queue.enqueue(i).unwrap();
        }

        let mut current: u32 = 0;

        while queue.size() > 0 {
            let option = queue.pop();

            assert!(option.is_some());

            assert_eq!(option.unwrap(), current);

            current += 1;
        }

        assert_eq!(current, count);
    }

    fn test_single_thread_capacity_for<T>(queue: T, count: u32) where T: Queue<u32> {
        for i in 0..count {
            queue.enqueue(i).unwrap();
        }

        //Should not be able to insert
        assert!(queue.enqueue(count).is_err());

        let popped = queue.pop();

        let mut current = 0;

        assert!(popped.is_some() && popped.unwrap() == current);

        current += 1;

        assert!(queue.enqueue(count).is_ok());

        while queue.size() > 0 {
            let option = queue.pop();

            assert!(option.is_some());

            assert_eq!(option.unwrap(), current);

            current += 1;
        }

        assert_eq!(current, count + 1);
    }

    fn test_spsc<T>(queue: T, capacity: usize, operation_count: u32) where T: Queue<u32> + Send + Sync + 'static {
        let queue_arc = Arc::new(queue);

        let queue_prod = queue_arc.clone();

        let _producer_handle = std::thread::spawn(move || {

            //producer thread
            let mut current = 0;
            let mut count = 0;

            loop {
                if count > operation_count {
                    break;
                }

                match queue_prod.enqueue(current as u32) {
                    Ok(_) => {
                        current = (current + 1) % capacity as u32;
                        count += 1;
                    }
                    Err(_) => {}
                };
            }
        });

        let start = Instant::now();

        //consumer thread
        let mut current = 0;
        let mut count = 0;

        loop {
            if count > operation_count {
                break;
            }

            match queue_arc.pop() {
                None => {}
                Some(popped) => {
                    assert_eq!(popped, current);

                    current = (current + 1) % capacity as u32;
                    count = count + 1;
                }
            };
        }

        println!("Performed all remove operations in {} millis", start.elapsed().as_millis());
    }

    fn test_spsc_blocking<T>(queue: T, capacity: usize, operation_count: u32) where T: BQueue<u32> + Send + Sync + 'static {
        let queue_arc = Arc::new(queue);

        let queue_prod = queue_arc.clone();

        let _producer_handle = std::thread::spawn(move || {

            //producer thread
            let mut current = 0;
            let mut count = 0;

            loop {
                if count > operation_count {
                    break;
                }

                queue_prod.enqueue_blk(current as u32);

                current = (current + 1) % capacity as u32;
                count += 1;
            }
        });

        let start = Instant::now();

        //consumer thread
        let mut current = 0;
        let mut count = 0;

        loop {
            if count > operation_count {
                break;
            }

            let popped = queue_arc.pop_blk();

            assert_eq!(popped, current);

            current = (current + 1) % capacity as u32;
            count = count + 1;
        }

        println!("Performed all remove operations in {} millis", start.elapsed().as_millis());
    }

    fn test_mpsc_crossbeam(capacity: usize, operations: usize, producer_threads: usize) {
        let operations_per_producer = operations / producer_threads;

        let queue_arc = Arc::new(ArrayQueue::new(capacity));

        for _ in 0..producer_threads {
            let queue_prod = queue_arc.clone();
            std::thread::spawn(move || {
                let mut current = 0;
                let mut count = 0;

                loop {
                    if count > operations_per_producer {
                        break;
                    }

                    match queue_prod.push(current as u32) {
                        Ok(_) => {
                            current = (current + 1) % capacity as u32;
                            count += 1;
                        }
                        Err(_) => {}
                    }
                }
            });
        }

        let start = Instant::now();

        //consumer thread
        let mut count = 0;

        loop {
            if count > operations {
                break;
            }

            match queue_arc.pop() {
                None => {}
                Some(_) => {
                    count = count + 1;
                }
            };
        }

        let i = start.elapsed().as_millis();

        println!("Performed all remove operations in {} millis", i);
    }

    fn test_mpsc_blocking<T>(queue: T, capacity: usize, operations: usize, producer_threads: usize) where T: BQueue<u32> + Queue<u32> + Send + Sync + 'static {
        let operations_per_producer = operations / producer_threads;

        let queue_arc = Arc::new(queue);

        for _ in 0..producer_threads {
            let queue_prod = queue_arc.clone();

            std::thread::spawn(move || {
                let mut current = 0;
                let mut count = 0;

                loop {
                    if count > operations_per_producer {
                        break;
                    }

                    queue_prod.enqueue_blk(current as u32);

                    current = (current + 1) % capacity as u32;
                    count += 1;
                }
            });
        }

        let start = Instant::now();

        //consumer thread
        let mut count = 0;

        loop {
            if count >= operations {
                break;
            }

            let _popped = queue_arc.pop_blk();

            count += 1;
        }
    }

    #[test]
    fn test_single_thread_ordering() {
        let limit = 10;

        test_single_thread_ordering_for(MQueue::new(limit, true), limit as u32);

        test_single_thread_ordering_for(MQueue::new(limit, false), limit as u32);

        test_single_thread_ordering_for(LFBRArrayQueue::new(limit), limit as u32);
    }

    #[test]
    fn test_single_thread_capacity() {
        let limit = 10;

        test_single_thread_capacity_for(MQueue::new(limit, true), limit as u32);

        test_single_thread_capacity_for(MQueue::new(limit, false), limit as u32);

        test_single_thread_capacity_for(LFBRArrayQueue::new(limit), limit as u32);
    }

    #[test]
    fn test_two_thread_spsc_blocking() {
        let limit = 10;
        let operations = 100000;

        println!("Testing LFRoomArrayQueue");

        test_spsc_blocking(LFBRArrayQueue::new(limit), limit, operations);

        println!("Testing LFArrayQueue");

        test_spsc_blocking(LFBQueue::new(limit), limit, operations);

        println!("Testing MQueue with backoff");

        test_spsc_blocking(MQueue::new(limit, true), limit, operations);

        println!("Testing MQueue with no backoff");

        test_spsc_blocking(MQueue::new(limit, false), limit, operations);

        println!("Testing crossbeam");

        test_spsc(ArrayQueue::new(limit), limit, operations);
    }

    #[test]
    fn test_spsc_() {
        let limit = 10;
        let operations = 100000;

        println!("Testing non blocking queues");

        println!("Testing LFArrayQueue");

        test_spsc(LFBQueue::new(limit), limit, operations);

        println!("Testing LFRoomArrayQueue");

        test_spsc(LFBRArrayQueue::new(limit), limit, operations);

        println!("Testing MQueue with backoff");

        test_spsc(MQueue::new(limit, true), limit, operations);

        println!("Testing MQueue with no backoff");

        test_spsc(MQueue::new(limit, false), limit, operations);

        println!("Testing crossbeam");
        test_spsc(ArrayQueue::new(limit), limit, operations);
    }

    #[test]
    fn test_mpsc() {
        let capacity = 1000;
        let operations = 3200000;

        let producer_threads = 32;

        println!("Testing blocking queues multiple producer single consumer");

        println!("Testing LFRoomArrayQueue");
        test_mpsc_blocking(LFBRArrayQueue::new(capacity), capacity, operations, producer_threads);

        println!("Testing LFArrayQueue");
        test_mpsc_blocking(LFBQueue::new(capacity), capacity, operations, producer_threads);

        println!("Testing Mutex queue with backoff");
        test_mpsc_blocking(MQueue::new(capacity, true), capacity, operations, producer_threads);

        println!("Testing Mutex queue with no backoff");
        test_mpsc_blocking(MQueue::new(capacity, false), capacity, operations, producer_threads);

        println!("Testing crossbeam");
        test_mpsc_crossbeam(capacity, operations, producer_threads);
    }
}