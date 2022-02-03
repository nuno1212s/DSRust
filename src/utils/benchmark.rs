use std::cmp::max;
use std::ops::Add;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct ThreadBench {
    thread_name: Option<String>,
    iterations: u128,
    time_taken: Duration,
}

pub struct CurrentBench {
    thread_name: Option<String>,
    current_iteration_count: u128,
    start_time: Instant,
}

impl CurrentBench {
    pub fn iter_count(&mut self) {
        self.current_iteration_count += 1;
    }

    pub fn iter_count_mult(&mut self, amount: u128) {
        self.current_iteration_count += amount;
    }

    pub fn iter_end(self) -> ThreadBench {
        let duration = Instant::now().duration_since(self.start_time);

        ThreadBench::new(self.thread_name, self.current_iteration_count, duration)
    }
}

impl ThreadBench {
    pub fn new(name: Option<String>, iterations: u128, duration: Duration) -> Self {
        Self {
            thread_name: name,
            iterations,
            time_taken: duration,
        }
    }

    pub fn thread_name(&self) -> &Option<String> {
        &self.thread_name
    }
    pub fn iterations(&self) -> u128 {
        self.iterations
    }
    pub fn time_taken(&self) -> Duration {
        self.time_taken
    }
}


///A multi-threaded benchmarking framework
///Works with thread local variables to keep performance to the best possible
///Might not work with setups like tasks
pub struct Benchmark {
    results: Mutex<Vec<ThreadBench>>,
}

impl Benchmark {
    pub fn new() -> Self {
        Self {
            results: Mutex::new(Vec::new())
        }
    }

    pub fn take_results(&self) -> Vec<ThreadBench> {
        let mut lock_guard = self.results.lock().unwrap();

        let new_results = Vec::new();

        std::mem::replace(&mut *lock_guard, new_results)
    }

    pub fn iter_start(&self, name: Option<String>) -> CurrentBench {
        CurrentBench {
            thread_name: name,
            current_iteration_count: 0,
            start_time: Instant::now()
        }
    }

    pub fn register_results(&self, results: ThreadBench) {
        let mut guard = self.results.lock().unwrap();

        guard.push(results);
    }

    pub fn show_results(results: Vec<ThreadBench>) {
        let mut total_iterations = 0;
        let mut total_duration = Duration::default();

        for thread_result in results.iter() {
            let name = match thread_result.thread_name() {
                None => { "Unnamed thread" }
                Some(name) => { name.as_str() }
            };
            println!("    - {} -     ", name);

            let duration = thread_result.time_taken();

            let iterations_micros = thread_result.iterations() / max(duration.as_micros(), 1);
            let iterations_ns = thread_result.iterations() / max(duration.as_nanos(), 1);
            let iterations_ms = thread_result.iterations() / max(duration.as_millis(), 1);

            println!("Did a total of {} iterations in {:?}", thread_result.iterations(), duration);
            println!("That's around {} iterations per nanosecond", iterations_ns);
            println!("And around {} iterations per µs", iterations_micros);
            println!("And around {} iterations per millisecond", iterations_ms);
            println!("    - {} -    ", name);

            total_duration = total_duration.add(duration);
            total_iterations += thread_result.iterations();
        }

        println!("    - IN TOTAL USING {} THREADS -    ", results.len());

        let iterations_micros = total_iterations / max(total_duration.as_micros(), 1);
        let iterations_ns = total_iterations / max(total_duration.as_nanos(), 1);
        let iterations_ms = total_iterations / max(total_duration.as_millis(), 1);

        println!("Did a total of {} iterations in {:?}", total_iterations, total_duration);
        println!("That's around {} iterations per nanosecond", iterations_ns);
        println!("And around {} iterations per µs", iterations_micros);
        println!("And around {} iterations per millisecond", iterations_ms);

        println!("-------------------------");
    }
}
