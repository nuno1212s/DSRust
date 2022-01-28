use std::error::Error;
use std::fmt::{Display, Formatter};



pub trait SizableQueue {
    fn size(&self) -> u32;
}

/// FIFO blocking queue trait
pub trait BQueue<T>: SizableQueue {
    ///Enqueue an element into the tail of the queue
    /// Will block if there is no available space in the queue
    fn enqueue_blk(&self, elem: T);

    ///Pop the first element in the queue
    /// Will block if the queue is empty until there is an element
    fn pop_blk(&self) -> T;

    /// Dump the first count elements from the queue
    /// If the queue has less than count elements it will block until
    /// it has finished getting all the elements
    fn dump_blk(&self, count: usize) -> Vec<T>;
}

///FIFO non blocking queue trait
pub trait Queue<T>: SizableQueue {
    ///Attempts to enqueue an element at the tail of the queue
    /// If the queue is already full and does not support any more elements,
    /// the function will not block
    fn enqueue(&self, elem: T) -> Result<(), QueueError>;

    ///Attempt to pop the first element in the queue
    /// Will not block if the queue is empty
    fn pop(&self) -> Option<T>;

    /// Dump the first count elements from the queue
    /// If the queue has less than count elements it will return as many
    /// elements as currently available
    fn dump(&self, count: usize) -> Vec<T>;
}

#[derive(Debug)]
pub enum QueueError {
    QueueFull
}

impl Error for QueueError {}

impl Display for QueueError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to add element, queue is already full")
    }
}

#[cfg(test)]
mod tests {
    use crate::queues::lfqueue::LFQueue;
    use crate::queues::mqueue::MQueue;
    use crate::queues::queues::Queue;

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

    #[test]
    fn test_single_thread_ordering() {

        let limit = 10;

        test_single_thread_ordering_for(MQueue::new(limit, true), limit as u32);

        test_single_thread_ordering_for(MQueue::new(limit, false), limit as u32);

        test_single_thread_ordering_for(LFQueue::new(limit), limit as u32);
    }

    #[test]
    fn test_single_thread_capacity() {

        let limit = 10;

        test_single_thread_capacity_for(MQueue::new(limit, true), limit as u32);

        test_single_thread_capacity_for(MQueue::new(limit, false), limit as u32);

        test_single_thread_capacity_for(LFQueue::new(limit), limit as u32);
    }
}