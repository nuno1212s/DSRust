#![feature(fn_traits)]

pub mod utils {
    pub mod backoff;
}

pub mod queues {
    pub mod lfqueue;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
