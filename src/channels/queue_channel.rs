use std::error::Error;
use std::fmt::{Debug, Formatter, write};
use std::ops::Deref;
use std::path::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::sync::mpsc::{RecvError, SendError, TryRecvError, TrySendError};

use crossbeam_utils::{Backoff, CachePadded};
use event_listener::{Event, EventListener};
use fastrand::usize;

use crate::queues::lf_array_queue::LFBQueue;
use crate::queues::mqueue::MQueue;
use crate::queues::queues::{BQueue, Queue, QueueError, SizableQueue};
use crate::queues::rooms_array_queue::LFBRArrayQueue;

struct Sender<T, Z> where T: Send,
                          Z: BQueue<T> + Queue<T> + Send + Sync {
    inner: Arc<SendingInner<T, Z>>,
    listener: EventListener,
}

struct Receiver<T, Z> where T: Send,
                            Z: BQueue<T> + Queue<T> + Send + Sync {
    inner: Arc<ReceivingInner<T, Z>>,
    listener: EventListener,
}

impl<T, Z> Sender<T, Z> where T: Send,
                              Z: BQueue<T> + Queue<T> + Send + Sync {
    fn new(inner: Arc<SendingInner<T, Z>>) -> Self {
        let listener = inner.waiting_reception.listen();

        Self {
            inner,
            listener,
        }
    }

    ///Only notifies the threads if there were any listeners registered
    fn notify_if_necessary(&self) {
        if self.inner.awaiting_reception.load(Ordering::Acquire) > 0 {
            self.inner.waiting_reception.notify_relaxed(usize::MAX);
        }
    }

    fn try_send(&self, obj: T) -> Result<(), TrySendError<T>> {
        if self.inner.is_dc.load(Relaxed) {
            return Err(TrySendError::Disconnected(obj));
        }

        match self.inner.queue.enqueue(obj) {
            Ok(_) => {
                self.notify_if_necessary();
                Ok(())
            }
            Err(obj) => {
                match obj {
                    QueueError::QueueFull(elem) => {
                        Err(TrySendError::Full(elem))
                    }
                    _ => {}
                }
            }
        }
    }

    fn send(&self, mut obj: T) -> Result<(), SendError<T>> {
        let backoff = Backoff::new();

        loop {
            match self.try_send(obj) {
                Ok(_) => {
                    return Ok(());
                }
                Err(err) => {
                    match err {
                        TrySendError::Full(elem) => {
                            obj = elem;
                        }
                        TrySendError::Disconnected(elem) => {
                            obj = elem;
                        }
                    }
                }
            }

            if backoff.is_completed() {
                self.inner.awaiting_sending.fetch_add(1, Ordering::Release);

                loop {
                    match self.try_send(obj) {
                        Ok(_) => {
                            break;
                        }
                        Err(err) => {
                            match err {
                                TrySendError::Full(elem) => {
                                    obj = elem;
                                }
                                TrySendError::Disconnected(elem) => {
                                    return Err(SendError(elem));
                                }
                            }
                        }
                    }

                    self.listener.wait();
                }

                self.inner.awaiting_sending.fetch_sub(1, Ordering::Release);

                return Ok(());
            } else {
                backoff.snooze();
            }
        }
    }
}

impl<T, Z> Receiver<T, Z> where T: Send,
                                Z: BQueue<T> + Queue<T> + Send + Sync {
    fn new(inner: Arc<ReceivingInner<T, Z>>) -> Self {
        let listener = inner.waiting_sending.listen();

        Self {
            inner,
            listener,
        }
    }

    fn notify_if_necessary(&self) {
        if self.inner.awaiting_reception.load(Ordering::Acquire) > 0 {
            self.inner.waiting_reception.notify_relaxed(usize::MAX);
        }
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        if self.inner.is_dc.load(Ordering::Relaxed) && self.inner.queue.is_empty() {
            //Test if the queue is empty so if the senders are disconnected,
            //We can still receive all the elements that are lacking in the list
            return Err(TryRecvError::Disconnected);
        }

        return match self.inner.queue.pop() {
            None => {
                Err(TryRecvError::Empty)
            }
            Some(element) => {
                self.notify_if_necessary();

                Ok(element)
            }
        };
    }

    fn recv(&self) -> Result<T, RecvError> {
        let backoff = Backoff::new();

        loop {
            match self.try_recv() {
                Ok(elem) => {
                    return Ok(elem);
                }
                Err(try_recv_err) => {
                    match try_recv_err {
                        TryRecvError::Disconnected => {
                            return Err(RecvError);
                        }
                        _ => {}
                    }
                }
            }

            if backoff.is_completed() {
                self.inner.awaiting_sending.fetch_add(1, Ordering::Release);

                let ret;

                loop {
                    match self.try_recv() {
                        Ok(elem) => {
                            ret = Ok(elem);
                            break;
                        }
                        Err(try_recv_err) => {
                            match try_recv_err {
                                TryRecvError::Disconnected => {
                                    ret = Err(RecvError);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }

                    self.listener.wait();
                }

                self.inner.awaiting_sending.fetch_sub(1, Ordering::Release);

                return ret;
            } else {
                backoff.snooze();
            }
        }
    }

    async fn recv_async(&self) -> Result<T, RecvError> {
        let backoff = Backoff::new();

        loop {
            match self.try_recv() {
                Ok(elem) => {
                    return Ok(elem);
                }
                Err(try_recv_err) => {
                    match try_recv_err {
                        TryRecvError::Disconnected => {
                            return Err(RecvError);
                        }
                        _ => {}
                    }
                }
            }

            if backoff.is_completed() {
                self.inner.awaiting_sending.fetch_add(1, Ordering::Release);

                let ret;

                loop {
                    match self.try_recv() {
                        Ok(elem) => {
                            ret = Ok(elem);
                            break;
                        }
                        Err(try_recv_err) => {
                            match try_recv_err {
                                TryRecvError::Disconnected => {
                                    ret = Err(RecvError);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }

                    self.listener.await;
                }

                self.inner.awaiting_sending.fetch_sub(1, Ordering::Release);

                return ret;
            } else {
                backoff.snooze();
            }
        }
    }

    fn recv_mult(&self, vec: &mut Vec<T>) -> Result<usize, RecvMultError> {
        if self.inner.is_dc.load(Ordering::Relaxed) {
            return Err(RecvMultError::Disconnected);
        }

        return match self.inner.queue.dump(vec) {
            Ok(amount) => { Ok(amount) }
            Err(_) => { Err(RecvMultError::MalformedInputVec) }
        };
    }
}

impl<T, Z> Clone for Sender<T, Z> {
    fn clone(&self) -> Self {
        return Self::new(self.inner.clone());
    }
}

impl<T, Z> Clone for Receiver<T, Z> {
    fn clone(&self) -> Self {
        return Self::new(self.inner.clone());
    }
}

struct SendingInner<T, Z> where T: Send,
                                Z: BQueue<T> + Queue<T> + Send + Sync {
    inner: Arc<Inner<T, Z>>,
}

struct ReceivingInner<T, Z> where T: Send,
                                  Z: BQueue<T> + Queue<T> + Send + Sync {
    inner: Arc<Inner<T, Z>>,
}

impl<T, Z> SendingInner<T, Z> where T: Send,
                                    Z: BQueue<T> + Queue<T> + Send + Sync {
    fn new(inner: Arc<Inner<T, Z>>) -> Self {
        Self {
            inner
        }
    }
}

impl<T, Z> ReceivingInner<T, Z> where T: Send,
                                      Z: BQueue<T> + Queue<T> + Send + Sync {
    fn new(inner: Arc<Inner<T, Z>>) -> Self {
        Self {
            inner
        }
    }
}

impl<T, Z> Deref for ReceivingInner<T, Z> {
    type Target = Arc<Inner<T, Z>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, Z> Drop for ReceivingInner<T, Z> {
    fn drop(&mut self) {
        self.inner.is_dc.store(true, Ordering::Relaxed);
    }
}

impl<T, Z> Deref for SendingInner<T, Z> {
    type Target = Arc<Inner<T, Z>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, Z> Drop for SendingInner<T, Z> {
    fn drop(&mut self) {
        self.inner.is_dc.store(true, Ordering::Relaxed);
    }
}

struct Inner<T, Z> where T: Send,
                         Z: BQueue<T> + Queue<T> + Send + Sync {
    queue: Z,
    //Is the channel disconnected
    is_dc: AtomicBool,
    //Sleeping event to allow threads that are waiting for a request
    //To go to sleep efficiently
    awaiting_reception: CachePadded<AtomicU32>,
    awaiting_sending: CachePadded<AtomicU32>,
    waiting_reception: Event,
    waiting_sending: Event,
}

impl<T, Z> Inner<T, Z> where T: Send,
                             Z: Queue<T> + Send + Sync {
    fn new(queue: Z) -> Self {
        Self {
            queue,
            is_dc: AtomicBool::new(false),
            awaiting_reception: CachePadded::new(AtomicU32::new(0)),
            awaiting_sending: CachePadded::new(AtomicU32::new(0)),
            waiting_reception: Event::new(),
            waiting_sending: Event::new(),
        }
    }
}

enum RecvMultError {
    MalformedInputVec,
    Disconnected,
}

impl Debug for RecvMultError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvMultError::MalformedInputVec => {
                write!(f, "Malformed input vec")
            }
            RecvMultError::Disconnected => {
                write!(f, "Disconnected")
            }
        }
    }
}

impl std::fmt::Display for RecvMultError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvMultError::MalformedInputVec => {
                write!(f, "Malformed input vec")
            }
            RecvMultError::Disconnected => {
                write!(f, "Disconnected")
            }
        }
    }
}

impl Error for RecvMultError {}

pub fn bounded_lf_queue<T>(capacity: usize) -> (Sender<T, LFBQueue<T>>, Receiver<T, LFBQueue<T>>) {
    let inner = Inner::new(LFBQueue::new(capacity));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (Sender::new(Arc::new(sending_arc)), Receiver::new(Arc::new(receiving_arc)))
}

pub fn bounded_lf_room_queue<T>(capacity: usize) -> (Sender<T, LFBRArrayQueue<T>>, Receiver<T, LFBRArrayQueue<T>>) {
    let inner = Inner::new(LFBRArrayQueue::new(capacity));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (Sender::new(Arc::new(sending_arc)), Receiver::new(Arc::new(receiving_arc)))
}

pub fn bounded_mutex_backoff_queue<T>(capacity: usize) -> (Sender<T, MQueue<T>>, Receiver<T, MQueue<T>>) {
    let inner = Inner::new(MQueue::new(capacity, true));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (Sender::new(Arc::new(sending_arc)), Receiver::new(Arc::new(receiving_arc)))
}

pub fn bounded_mutex_no_backoff_queue<T>(capacity: usize) -> (Sender<T, MQueue<T>>, Receiver<T, MQueue<T>>) {
    let inner = Inner::new(MQueue::new(capacity, false));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (Sender::new(Arc::new(sending_arc)), Receiver::new(Arc::new(receiving_arc)))
}
