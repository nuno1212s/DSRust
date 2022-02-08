#[cfg(test)]
pub mod queue_bench {
    use std::fmt::Debug;
    use std::slice::Iter;
    use std::sync::Arc;

    use crate::queues::lf_array_queue::LFBQueue;
    use crate::queues::mqueue::MQueue;
    use crate::queues::queues::{BQueue, Queue, QueueError};
    use crate::queues::rooms_array_queue::LFBRArrayQueue;
    use crate::utils::benchmark::Benchmark;

    pub enum BenchedQueues {
        LFBQueue,
        LFRoomQueue,
        MutexQueueBackoff,
        MutexQueueNoBackoff,
    }

    pub trait QueueGenerator {
        fn iterator() -> Iter<'static, BenchedQueues>;

        fn gen_queue<K>(&self, capacity: usize) -> Box<dyn Queue<K> + Send + Sync> where K: Debug + Send + 'static;
        fn gen_blk_queue<K>(&self, capacity: usize) -> Box<dyn BQueue<K> + Send + Sync> where K: Debug + Send + 'static;

        fn name(&self) -> String;
    }

    impl QueueGenerator for BenchedQueues {
        fn iterator() -> Iter<'static, BenchedQueues> {
            static QUEUES: [BenchedQueues; 4] = [BenchedQueues::LFBQueue, BenchedQueues::LFRoomQueue,
                BenchedQueues::MutexQueueBackoff, BenchedQueues::MutexQueueNoBackoff];

            QUEUES.iter()
        }

        fn gen_queue<K>(&self, capacity: usize) -> Box<dyn Queue<K> + Send + Sync> where K: Debug + Send + 'static {
            return match self {
                BenchedQueues::LFBQueue => {
                    Box::new(LFBQueue::new(capacity))
                }
                BenchedQueues::LFRoomQueue => {
                    Box::new(LFBRArrayQueue::new(capacity))
                }
                BenchedQueues::MutexQueueBackoff => {
                    Box::new(MQueue::new(capacity, true))
                }
                BenchedQueues::MutexQueueNoBackoff => {
                    Box::new(MQueue::new(capacity, false))
                }
            };
        }

        fn gen_blk_queue<K>(&self, capacity: usize) -> Box<dyn BQueue<K> + Send + Sync> where K: Debug + Send + 'static {
            return match self {
                BenchedQueues::LFBQueue => {
                    Box::new(LFBQueue::new(capacity))
                }
                BenchedQueues::LFRoomQueue => {
                    Box::new(LFBRArrayQueue::new(capacity))
                }
                BenchedQueues::MutexQueueBackoff => {
                    Box::new(MQueue::new(capacity, true))
                }
                BenchedQueues::MutexQueueNoBackoff => {
                    Box::new(MQueue::new(capacity, false))
                }
            };
        }

        fn name(&self) -> String {
            return match self {
                BenchedQueues::LFBQueue => {
                    String::from("LF Blocking Array Queue")
                }
                BenchedQueues::LFRoomQueue => {
                    String::from("LF Room Blocking Array Queue")
                }
                BenchedQueues::MutexQueueBackoff => {
                    String::from("Mutex Blocking Array Queue with backoff")
                }
                BenchedQueues::MutexQueueNoBackoff => {
                    String::from("Mutex Blocking Array Queue with no backoff")
                }
            };
        }
    }

    fn start_blocking_producer(queue: Arc<Box<dyn BQueue<u32> + Send + Sync>>, operations: usize, bencher: Arc<Benchmark>, thread: usize) {
        let mut bench = bencher.iter_start(Option::Some(String::from(format!("Thread {}, ADD", thread))));

        let mut current = 0;
        let mut count = 0;

        let capacity = match queue.capacity() {
            None => { 1024 }
            Some(cap) => { cap }
        };

        loop {
            if count >= operations {
                break;
            }

            queue.enqueue_blk(current as u32);

            bench.iter_count();

            current = (current + 1) % queue.capacity().unwrap() as u32;
            count += 1;
        }

        bencher.register_results(bench.iter_end());
    }

    fn start_producer(queue: Arc<Box<dyn Queue<u32> + Send + Sync>>, operations: usize, bencher: Arc<Benchmark>, thread: usize) {
        let mut bench = bencher.iter_start(Option::Some(String::from(format!("Thread {}, ADD", thread))));

        let mut current = 0;
        let mut count = 0;

        let capacity = match queue.capacity() {
            None => { 1024 }
            Some(cap) => { cap }
        };

        loop {
            if count >= operations {
                break;
            }

            match queue.enqueue(current as u32) {
                Ok(_) => {
                    current = (current + 1) % queue.capacity().unwrap() as u32;
                    count += 1;

                    bench.iter_count();
                }
                Err(_) => {}
            };
        }

        bencher.register_results(bench.iter_end());
    }


    fn start_dump_consumer(queue: Arc<Box<dyn Queue<u32> + Send + Sync>>, operations: usize, bencher: Arc<Benchmark>, thread: usize) {
        let mut current_bench = bencher.iter_start(Some(String::from(format!("Thread {} CONSUMER", thread))));

        //consumer thread
        let mut count = 0;

        let option = queue.capacity();

        let capacity = match option {
            None => { 1024 }
            Some(cap) => { cap }
        };

        let mut storage = Vec::with_capacity(capacity);

        let mut batches = 0;

        let mut max_batch = 0;

        loop {
            if count >= operations {
                break;
            }

            match queue.dump(&mut storage) {
                Ok(crr_cout) => {
                    count += crr_cout;

                    batches += 1;

                    max_batch = std::cmp::max(max_batch, crr_cout);

                    current_bench.iter_count_mult(crr_cout as u128);
                }
                Err(_) => {
                    panic!()
                }
            }
        }

        bencher.register_results(current_bench.iter_end());

        println!("The average batch size for the currently analysed array is {} \
        and the largest batch was {}", (count / batches), max_batch);
    }

    fn start_blocking_consumer(queue: Arc<Box<dyn BQueue<u32> + Send + Sync>>, operations: usize, bencher: Arc<Benchmark>, thread: usize) {
        let mut current_bench = bencher.iter_start(Some(String::from(format!("Thread CONSUMER"))));

        //consumer thread
        let mut count = 0;

        loop {
            if count >= operations {
                break;
            }

            let _popped = queue.pop_blk();

            count += 1;
            current_bench.iter_count();
        }

        bencher.register_results(current_bench.iter_end());
    }

    fn test_mpsc_blocking(queue: Box<dyn BQueue<u32> + Send + Sync>, capacity: usize, operations: usize, producer_threads: usize,
                          bencher: Arc<Benchmark>) {
        let operations_per_producer = operations / producer_threads;

        let queue_arc = Arc::new(queue);

        let mut threads = Vec::with_capacity(producer_threads);

        for thread in 0..producer_threads {
            let queue_prod = queue_arc.clone();

            let bench_ref = bencher.clone();

            let handles = std::thread::spawn(move || {
                start_blocking_producer(queue_prod, operations_per_producer, bench_ref, thread)
            });

            threads.push(handles);
        }

        start_blocking_consumer(queue_arc, operations, bencher, 0);

        for x in threads {
            x.join().unwrap();
        }
    }

    fn test_mpsc_dump(queue: Box<dyn Queue<u32> + Send + Sync>, capacity: usize, operations: usize, producer_threads: usize,
                      bencher: Arc<Benchmark>) {
        let operations_per_producer = operations / producer_threads;

        let queue_arc = Arc::new(queue);

        let mut threads = Vec::with_capacity(producer_threads);

        for thread in 0..producer_threads {
            let queue_prod = queue_arc.clone();

            let bench_ref = bencher.clone();

            let handles = std::thread::spawn(move || {
                start_producer(queue_prod, operations_per_producer, bench_ref, thread)
            });

            threads.push(handles);
        }

        start_dump_consumer(queue_arc, operations, bencher, 0);

        for x in threads {
            x.join().unwrap();
        }
    }

    fn test_mpmc(queue: Box<dyn BQueue<u32> + Send + Sync>, capacity: usize, operations: usize, producer_threads: usize,
                 consumer_threads: usize, bencher: Arc<Benchmark>) {
        let operations_per_producer = operations / producer_threads;

        let operations_per_consumer = operations / consumer_threads;

        let queue_arc = Arc::new(queue);

        let mut threads = Vec::with_capacity(producer_threads);

        for thread in 0..producer_threads {
            let queue_prod = queue_arc.clone();

            let bench_ref = bencher.clone();

            let handles = std::thread::spawn(move || {
                start_blocking_producer(queue_prod, operations_per_producer, bench_ref, thread)
            });

            threads.push(handles);
        }

        for thread in 0..consumer_threads {
            let queue_prod = queue_arc.clone();

            let bench_ref = bencher.clone();

            let handles = std::thread::spawn(move || {
                start_blocking_consumer(queue_prod, operations, bench_ref, 0);
            });

            threads.push(handles);
        }

        for x in threads {
            x.join().unwrap();
        }
    }

    #[test]
    pub fn bench_mpsc_blocking() {
        let capacity = 10000;

        let operations = 10_000_000;

        let mut producer_threads = 2;

        while producer_threads <= 16 {
            println!("Testing for {} threads: ", producer_threads);

            let bencher = Arc::new(Benchmark::new());

            for a in BenchedQueues::iterator() {
                println!("Testing {}", a.name());

                let queue = a.gen_blk_queue(capacity);

                test_mpsc_blocking(queue, capacity, operations, producer_threads, bencher.clone());

                let vec = bencher.take_results();

                Benchmark::show_results(vec);
            }

            producer_threads *= 2;
        }
    }

    #[test]
    pub fn bench_mpsc_polling_dump() {
        let capacity = 10000;

        let operations = 10_000_000;

        let mut producer_threads = 2;

        let bencher = Arc::new(Benchmark::new());

        while producer_threads <= 16 {
            println!("Testing for {} threads: ", producer_threads);

            for a in BenchedQueues::iterator() {
                println!("Testing {}", a.name());

                let queue = a.gen_queue(capacity);

                test_mpsc_dump(queue, capacity, operations, producer_threads, bencher.clone());

                let vec = bencher.take_results();

                Benchmark::show_results(vec);
            }

            producer_threads *= 2;
        }
    }

    #[test]
    pub fn bench_mpmc() {
        let capacity = 10000;

        let operations = 10_000_000;

        let mut threads = 2;

        let bencher = Arc::new(Benchmark::new());

        while threads <= 16 {
            println!("Testing for {} threads: ", threads);

            for a in BenchedQueues::iterator() {
                println!("Testing {}", a.name());

                let queue = a.gen_blk_queue(capacity);

                test_mpmc(queue, capacity, operations, threads,
                          threads, bencher.clone());

                let vec = bencher.take_results();

                Benchmark::show_results(vec);
            }

            threads *= 2;
        }
    }
}