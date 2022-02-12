use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub trait SizableQueue {
    fn size(&self) -> usize;

    fn capacity(&self) -> Option<usize>;

    fn is_empty(&self) -> bool;

    fn is_full(&self) -> bool;
}

/// FIFO blocking queue trait
pub trait BQueue<T>: SizableQueue where  {
    ///Enqueue an element into the tail of the queue
    /// Will block if there is no available space in the queue
    fn enqueue_blk(&self, elem: T);

    ///Pop the first element in the queue
    /// Will block if the queue is empty until there is an element
    fn pop_blk(&self) -> T;
}

///FIFO non blocking queue trait
pub trait Queue<T>: SizableQueue where  {
    ///Attempts to enqueue an element at the tail of the queue
    /// If the queue is already full and does not support any more elements,
    /// the function will not block
    fn enqueue(&self, elem: T) -> Result<(), QueueError<T>>;

    ///Attempt to pop the first element in the queue
    /// Will not block if the queue is empty
    fn pop(&self) -> Option<T>;

    /// Dump all the elements that are in the queue into the given vector
    /// The vector should be == to the capacity of the queue
    fn dump(&self, vec: &mut Vec<T>) -> Result<usize, QueueError<T>>;
}

pub enum QueueError<T> where  {
    QueueFull(T),
    MalformedInputVec,
}

impl<T> Debug for QueueError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::QueueFull(_elem) => {
                write!(f, "The queue is already full!")
            }
            QueueError::MalformedInputVec => {
                write!(f, "The input vector is malformed")
            }
        }
    }
}

impl<T> Error for QueueError<T> where  {}

impl<T> Display for QueueError<T> where  {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to add element, queue is already full")
    }
}