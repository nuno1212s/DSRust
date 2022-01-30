use crossbeam_queue::ArrayQueue;
use crate::queues::queues::{BQueue, Queue, QueueError, SizableQueue};

impl SizableQueue for ArrayQueue<u32> {
    fn size(&self) -> usize {
        self.len()
    }
}

impl Queue<u32> for ArrayQueue<u32> {
    fn enqueue(&self, elem: u32) -> Result<(), QueueError> {
        match self.push(elem) {
            Ok(_) => {
                Ok(())
            }
            Err(_) => {
                Err(QueueError::QueueFull)
            }
        }
    }

    fn pop(&self) -> Option<u32> {
        self.pop()
    }

    fn dump(&self, count: usize, vec: &mut Vec<u32>) -> Result<usize, QueueError> {
        todo!()
    }
}

#[cfg(test)]
pub mod queue_tests {
    use std::sync::Arc;
    use std::time::Instant;
    use crossbeam_queue::ArrayQueue;

    use crate::queues::lfarrayqueue::LFArrayQueue;
    use crate::queues::mqueue::MQueue;
    use crate::queues::queues::{BQueue, Queue, QueueError};

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

    fn test_spsc_crossbeam(capacity: usize, operation_count: u32) {
        let queue = ArrayQueue::new(capacity);

        let queue_arc = Arc::new(queue);

        let queue_prod = queue_arc.clone();

        let producer_handle = std::thread::spawn(move || {

            //producer thread
            let mut current = 0;
            let mut count = 0;

            loop {
                if count > operation_count {
                    break;
                }

                match queue_prod.push(current) {
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

            let popped_opt = queue_arc.pop();

            match popped_opt {
                None => {}
                Some(popped) => {
                    assert_eq!(popped, current);

                    current = (current + 1) % capacity as u32;
                    count = count + 1;
                }
            }
        }

        println!("Performed all remove operations in {} millis", start.elapsed().as_millis());
    }

    fn test_spsc<T>(queue: T, capacity: usize, operation_count: u32) where T: Queue<u32> + Send + Sync + 'static {
        let queue_arc = Arc::new(queue);

        let queue_prod = queue_arc.clone();

        let producer_handle = std::thread::spawn(move || {

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

        let producer_handle = std::thread::spawn(move || {

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

            let popped = match queue_arc.pop() {
                None => {}
                Some(_) => {
                    count = count + 1;
                }
            };
        }

        let i = start.elapsed().as_millis();

        println!("Performed all remove operations in {} millis", i);
    }

    fn test_mpsc_blocking<T>(queue: T, capacity: usize, operations: usize, producer_threads: usize) where T: BQueue<u32> + Send + Sync + 'static {
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
            if count > operations {
                break;
            }

            let popped = queue_arc.pop_blk();

            count = count + 1;
        }

        let i = start.elapsed().as_millis();

        println!("Performed all remove operations in {} millis", i);
    }

    #[test]
    fn test_single_thread_ordering() {
        let limit = 10;

        test_single_thread_ordering_for(MQueue::new(limit, true), limit as u32);

        test_single_thread_ordering_for(MQueue::new(limit, false), limit as u32);

        test_single_thread_ordering_for(LFArrayQueue::new(limit), limit as u32);
    }

    #[test]
    fn test_single_thread_capacity() {
        let limit = 10;

        test_single_thread_capacity_for(MQueue::new(limit, true), limit as u32);

        test_single_thread_capacity_for(MQueue::new(limit, false), limit as u32);

        test_single_thread_capacity_for(LFArrayQueue::new(limit), limit as u32);
    }

    #[test]
    fn test_two_thread_spsc_blocking() {
        let limit = 10;
        let operations = 100000;

        println!("Testing LFArrayQueue");

        test_spsc_blocking(LFArrayQueue::new(limit), limit, operations);

        println!("Testing MQueue with backoff");

        test_spsc_blocking(MQueue::new(limit, true), limit, operations);

        println!("Testing MQueue with no backoff");

        test_spsc_blocking(MQueue::new(limit, false), limit, operations);

        println!("Testing crossbeam");

        test_spsc_crossbeam(limit, operations);
    }

    #[test]
    fn test_two_thread_spsc() {
        let limit = 10;
        let operations = 100000;

        println!("Testing non blocking queues");

        println!("Testing LFArrayQueue");

        test_spsc(LFArrayQueue::new(limit), limit, operations);

        println!("Testing MQueue with backoff");

        test_spsc(MQueue::new(limit, true), limit, operations);

        println!("Testing MQueue with no backoff");

        test_spsc(MQueue::new(limit, false), limit, operations);

        println!("Testing crossbeam");
        test_spsc_crossbeam(limit, operations);
    }

    #[test]
    fn test_mpsc() {
        let capacity = 100;
        let operations = 1000000;

        let producer_threads = 10;
        println!("Testing blocking queues multiple producer single consumer");

        println!("Testing LFArrayQueue");
        test_mpsc_blocking(LFArrayQueue::new(capacity), capacity, operations, producer_threads);

        println!("Testing Mutex queue with backoff");
        test_mpsc_blocking(MQueue::new(capacity, true), capacity, operations, producer_threads);

        println!("Testing Mutex queue with no backoff");
        test_mpsc_blocking(MQueue::new(capacity, false), capacity, operations, producer_threads);

        println!("Testing crossbeam");
        test_mpsc_crossbeam(capacity, operations, producer_threads);
    }
}