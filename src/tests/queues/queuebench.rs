#[cfg(test)]
pub mod queue_bench {
    use std::sync::Arc;

    use crate::queues::lf_array_queue::LFBQueue;
    use crate::queues::queues::{BQueue, Queue};
    use crate::queues::rooms_array_queue::LFBRArrayQueue;
    use crate::utils::benchmark::Benchmark;

    fn test_mpsc_blocking<T>(queue: T, capacity: usize, operations: usize, producer_threads: usize,
                             bencher: Arc<Benchmark>)
        where T: BQueue<u32> + Queue<u32> + Send + Sync + 'static {
        let operations_per_producer = operations / producer_threads;

        let queue_arc = Arc::new(queue);

        let mut threads = Vec::with_capacity(producer_threads);

        for thread in 0..producer_threads {
            let queue_prod = queue_arc.clone();

            let bench_ref = bencher.clone();

            let handles = std::thread::spawn(move || {
                let mut bench = bench_ref.iter_start(Option::Some(String::from(format!("Thread {}, ADD", thread))));

                let mut current = 0;
                let mut count = 0;

                loop {
                    if count >= operations_per_producer {
                        break;
                    }

                    queue_prod.enqueue_blk(current as u32);

                    bench.iter_count();

                    current = (current + 1) % capacity as u32;
                    count += 1;
                }

                bench_ref.register_results(bench.iter_end());
            });

            threads.push(handles);
        }

        let mut current_bench = bencher.iter_start(Some(String::from(format!("Thread CONSUMER"))));

        //consumer thread
        let mut count = 0;

        loop {
            if count >= operations {
                break;
            }

            let popped = queue_arc.pop_blk();

            count += 1;
            current_bench.iter_count();
        }

        bencher.register_results(current_bench.iter_end());

        for x in threads {
            x.join();
        }
    }


    #[test]
    pub fn bench_mpsc() {
        let capacity = 100;

        let operations = 1_000_000;

        let producer_threads = 4;

        let bencher = Arc::new(Benchmark::new());

        println!("Testing LF queue");

        test_mpsc_blocking(LFBQueue::new(capacity), capacity, operations, producer_threads, bencher.clone());

        let vec = bencher.take_results();

        Benchmark::show_results(vec);

        println!("Testing room queue");
        test_mpsc_blocking(LFBRArrayQueue::new(capacity), capacity, operations, producer_threads, bencher.clone());

        let vec = bencher.take_results();

        Benchmark::show_results(vec);
    }
}