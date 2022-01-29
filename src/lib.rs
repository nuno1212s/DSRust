#![feature(fn_traits)]

pub mod utils {
    pub mod backoff;
    pub mod memory_access;
}

pub mod queues {
    pub mod lfarrayqueue;
    pub mod queues;
    pub mod mqueue;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
