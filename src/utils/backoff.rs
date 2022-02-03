use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

use fastrand::Rng;

const SPIN_LIMIT: u16 = 6;
const YIELD_LIMIT: u16 = 10;

pub struct BackoffN {
    current_mult: Cell<u16>,
    random_ng: Rng,
}

impl BackoffN {
    pub fn new() -> Self {
        let rng = Rng::new();

        fastrand::seed(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());

        Self {
            current_mult: Cell::new(1),
            random_ng: rng,
        }
    }

    #[inline]
    pub fn spin(&self) {
        let result = self.random_ng.u32(0..(1 <<
            self.current_mult.get().min(SPIN_LIMIT)));

        for _ in 0..result {
            std::hint::spin_loop();
        }

        if self.current_mult.get() <= SPIN_LIMIT {
            self.current_mult.set(self.current_mult.get() + 1);
        }
    }

    #[inline]
    pub fn snooze(&self) {
        if self.current_mult.get() <= SPIN_LIMIT {
            //The same as spin, but here we want the step
            //To go over SPIN_LIMIT, so we can yield when
            //We have been waiting for a while
            let result = self.random_ng.u32(0..(1 <<
                self.current_mult.get().min(SPIN_LIMIT)));

            for _ in 0..result {
                std::hint::spin_loop();
            }

        } else {
            std::thread::yield_now();
        }

        if self.current_mult.get() <= YIELD_LIMIT {
            self.current_mult.set(self.current_mult.get() + 1);
        }
    }

    pub fn reset(&self) {
        self.current_mult.set(0);
    }
}

#[cfg(test)]
mod util_tests {
    use std::time::Instant;

}