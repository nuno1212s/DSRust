#![feature(fn_traits)]

pub mod tests {
    pub mod queues {
        pub mod queuetests;
        pub mod queuebench;
    }

    pub mod utils {
        pub mod roomsbench;
    }
}

pub mod utils {
    pub mod backoff;
    pub mod memory_access;
    pub mod benchmark;
    pub mod rooms;
}

pub mod queues {
    pub mod rooms_array_queue;
    pub mod queues;
    pub mod mqueue;
    pub mod lf_array_queue;
}