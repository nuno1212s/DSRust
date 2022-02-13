use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{RecvError, SendError, TryRecvError, TrySendError};

use crossbeam_utils::{Backoff, CachePadded};
use event_listener::{Event, EventListener};

use crate::queues::lf_array_queue::LFBQueue;
use crate::queues::mqueue::MQueue;
use crate::queues::queues::{Queue, QueueError};
use crate::queues::rooms_array_queue::LFBRArrayQueue;

///Inner classes, handle the futures abstractions
pub struct Sender<T, Z> where
    Z: Queue<T>  {
    inner: Arc<SendingInner<T, Z>>,
    phantom: PhantomData<fn() -> T>,
}

pub struct Receiver<T, Z> where
    Z: Queue<T>  {
    pub(crate) inner: Arc<ReceivingInner<T, Z>>,
    phantom: PhantomData<fn() -> T>,
    pub(crate) listener: Option<EventListener>,
}

pub struct ReceiverMult<T, Z> where
    Z: Queue<T>  {
    pub(crate) inner: Arc<ReceivingInner<T, Z>>,
    pub(crate) listener: Option<EventListener>,
    pub(crate) allocated: Option<Vec<T>>,
}

///Sender implementation
impl<T, Z> Sender<T, Z> where
    Z: Queue<T>  {
    fn new(inner: Arc<SendingInner<T, Z>>) -> Self {
        Self {
            inner,
            phantom: PhantomData::default(),
        }
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.queue.capacity()
    }

    ///Only notifies the threads if there were any listeners registered
    fn notify_if_necessary(&self) {
        if self.inner.awaiting_reception.load(Ordering::Acquire) > 0 {
            self.inner.waiting_reception.notify_relaxed(usize::MAX);
        }
    }

    pub fn try_send(&self, obj: T) -> Result<(), TrySendError<T>> {
        if self.inner.is_dc.load(Ordering::Relaxed) {
            return Err(TrySendError::Disconnected(obj));
        }

        match self.inner.queue.enqueue(obj) {
            Ok(_) => {
                self.notify_if_necessary();
                Ok(())
            }
            Err(err) => {
                match err {
                    QueueError::QueueFull(elem) => {
                        Err(TrySendError::Full(elem))
                    }
                    _ => {
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn send(&self, mut obj: T) -> Result<(), SendError<T>> {
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
                self.inner.awaiting_reception.fetch_add(1, Ordering::Release);

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

                    self.inner.waiting_reception.listen().wait();
                }

                self.inner.awaiting_reception.fetch_sub(1, Ordering::Release);

                return Ok(());
            } else {
                backoff.snooze();
            }
        }
    }

    pub async fn send_async(&self, elem: T) -> Result<(), SendError<T>> {
        let backoff = Backoff::new();

        let mut obj = elem;

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
                self.inner.awaiting_reception.fetch_add(1, Ordering::Release);

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

                    self.inner.waiting_reception.listen().await;
                }

                self.inner.awaiting_reception.fetch_sub(1, Ordering::Release);

                return Ok(());
            } else {
                backoff.snooze();
            }
        }
    }
}

impl<T, Z> Clone for Sender<T, Z> where
    Z: Queue<T>  {
    fn clone(&self) -> Self {
        return Self::new(self.inner.clone());
    }
}

///Standard one by one receiver
impl<T, Z> Receiver<T, Z> where
    Z: Queue<T>  {
    fn new(inner: Arc<ReceivingInner<T, Z>>) -> Self {
        Self {
            inner,
            phantom: PhantomData::default(),
            listener: Option::None,
        }
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.queue.capacity()
    }

    fn notify_if_necessary(&self) {
        if self.inner.awaiting_reception.load(Ordering::Acquire) > 0 {
            self.inner.waiting_reception.notify_relaxed(usize::MAX);
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
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

    pub fn recv_blk(&self) -> Result<T, RecvError> {
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

                    self.inner.waiting_sending.listen().wait();
                }

                self.inner.awaiting_sending.fetch_sub(1, Ordering::Release);

                return ret;
            } else {
                backoff.snooze();
            }
        }
    }

    pub async fn recv_async_basic(&self) -> Result<T, RecvError> {
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

                    self.inner.waiting_sending.listen().await;
                }

                self.inner.awaiting_sending.fetch_sub(1, Ordering::Release);

                return ret;
            } else {
                backoff.snooze();
            }
        }
    }
}

impl<T, Z> Unpin for Receiver<T, Z> where
    Z: Queue<T>  {}

impl<T, Z> Clone for Receiver<T, Z> where
    Z: Queue<T>  {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone())
    }
}

//Custom receiver that will receive multiple elements at a time
impl<T, Z> ReceiverMult<T, Z> where Z: Queue<T> {
    fn new(inner: Arc<ReceivingInner<T, Z>>) -> Self {
        Self {
            inner,
            listener: None,
            allocated: None,
        }
    }

    pub fn try_recv_mult(&self, vec: &mut Vec<T>) -> Result<usize, RecvMultError> {
        if self.inner.is_dc.load(Ordering::Relaxed) {
            return Err(RecvMultError::Disconnected);
        }

        loop {
            match self.inner.queue.dump(vec) {
                Ok(amount) => {
                    return Ok(amount);
                }
                Err(_) => {
                    return Err(RecvMultError::MalformedInputVec);
                }
            };
        }
    }

    pub fn recv_mult(&self, vec: &mut Vec<T>) -> Result<usize, RecvMultError> {
        if self.inner.is_dc.load(Ordering::Relaxed) {
            return Err(RecvMultError::Disconnected);
        }

        let backoff = Backoff::new();

        loop {
            match self.inner.queue.dump(vec) {
                Ok(amount) => {
                    if amount > 0 {
                        return Ok(amount);
                    }
                }
                Err(_) => {
                    return Err(RecvMultError::MalformedInputVec);
                }
            };

            if backoff.is_completed() {
                self.inner.awaiting_sending.fetch_add(1, Ordering::Release);
                let ret;

                loop {
                    match self.inner.queue.dump(vec) {
                        Ok(elem) => {
                            if elem > 0 {
                                ret = Ok(elem);
                                break;
                            }
                        }
                        Err(_) => {
                            ret = Err(RecvMultError::MalformedInputVec);
                            break;
                        }
                    }

                    self.inner.waiting_sending.listen().wait();
                }

                self.inner.awaiting_sending.fetch_sub(1, Ordering::Release);

                return ret;
            } else {
                backoff.snooze();
            }
        }
    }

    pub async fn recv_mult_async(&self, vec: &mut Vec<T>) -> Result<usize, RecvMultError> {
        if self.inner.is_dc.load(Ordering::Relaxed) {
            return Err(RecvMultError::Disconnected);
        }

        let backoff = Backoff::new();

        loop {
            match self.inner.queue.dump(vec) {
                Ok(amount) => {
                    if amount > 0 {
                        return Ok(amount);
                    }
                }
                Err(_) => {
                    return Err(RecvMultError::MalformedInputVec);
                }
            };

            if backoff.is_completed() {
                self.inner.awaiting_sending.fetch_add(1, Ordering::Release);
                let ret;

                loop {
                    match self.inner.queue.dump(vec) {
                        Ok(elem) => {
                            if elem > 0 {
                                ret = Ok(elem);
                                break;
                            }
                        }
                        Err(_) => {
                            ret = Err(RecvMultError::MalformedInputVec);
                            break;
                        }
                    }

                    self.inner.waiting_sending.listen().await;
                }

                self.inner.awaiting_sending.fetch_sub(1, Ordering::Release);

                return ret;
            } else {
                backoff.snooze();
            }
        }
    }
}

impl<T, Z> Unpin for ReceiverMult<T, Z> where
    Z: Queue<T>  {}

impl<T, Z> Clone for ReceiverMult<T, Z> where
    Z: Queue<T>  {
    fn clone(&self) -> Self {
        return Self::new(self.inner.clone());
    }
}

///We have this extra abstractions so we can keep
///Count of how many sending clone there are without
///having to count them, as when this sending inner
///gets disposed of, it means that no other processes
///Are listening, so the channel is effectively closed
pub struct SendingInner<T, Z> where
    Z: Queue<T>  {
    inner: Arc<Inner<T, Z>>,
    phantom: PhantomData<fn() -> T>,
}

pub struct ReceivingInner<T, Z> where
    Z: Queue<T>  {
    inner: Arc<Inner<T, Z>>,
    phantom: PhantomData<fn() -> T>,
}

impl<T, Z> SendingInner<T, Z> where
    Z: Queue<T>  {
    fn new(inner: Arc<Inner<T, Z>>) -> Self {
        Self {
            inner,
            phantom: PhantomData::default(),
        }
    }
}

impl<T, Z> ReceivingInner<T, Z> where
    Z: Queue<T>  {
    fn new(inner: Arc<Inner<T, Z>>) -> Self {
        Self {
            inner,
            phantom: PhantomData::default(),
        }
    }
}

impl<T, Z> Deref for ReceivingInner<T, Z> where
    Z: Queue<T>  {
    type Target = Arc<Inner<T, Z>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, Z> Drop for ReceivingInner<T, Z> where
    Z: Queue<T>  {
    fn drop(&mut self) {
        self.inner.close();
    }
}

impl<T, Z> Deref for SendingInner<T, Z> where
    Z: Queue<T>  {
    type Target = Arc<Inner<T, Z>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, Z> Drop for SendingInner<T, Z> where
    Z: Queue<T>  {
    fn drop(&mut self) {
        self.inner.close();
    }
}

pub struct Inner<T, Z> where
    Z: Queue<T>  {
    pub(crate) queue: Z,
    //Is the channel disconnected
    pub(crate) is_dc: AtomicBool,
    //Sleeping event to allow threads that are waiting for a request
//To go to sleep efficiently
    awaiting_reception: CachePadded<AtomicU32>,
    pub(crate) awaiting_sending: CachePadded<AtomicU32>,
    waiting_reception: Event,
    pub(crate) waiting_sending: Event,
    phantom: PhantomData<fn() -> T>,
}

impl<T, Z> Inner<T, Z> where
    Z: Queue<T>  {
    fn new(queue: Z) -> Self {
        Self {
            queue,
            is_dc: AtomicBool::new(false),
            awaiting_reception: CachePadded::new(AtomicU32::new(0)),
            awaiting_sending: CachePadded::new(AtomicU32::new(0)),
            waiting_reception: Event::new(),
            waiting_sending: Event::new(),
            phantom: PhantomData::default(),
        }
    }

    fn close(&self) {
        self.is_dc.store(true, Ordering::Relaxed);

        self.waiting_sending.notify(usize::MAX);
        self.waiting_reception.notify(usize::MAX);
    }
}

pub enum RecvMultError {
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

pub fn make_mult_recv_from<T, Z>(recv: Receiver<T, Z>) -> ReceiverMult<T, Z> where Z: Queue<T> + Sync {
    ReceiverMult::new(recv.inner)
}

pub fn bounded_lf_queue<T>(capacity: usize) -> (Sender<T, LFBQueue<T>>, Receiver<T, LFBQueue<T>>)
    where {
    let inner = Inner::new(LFBQueue::new(capacity));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (Sender::new(Arc::new(sending_arc)),
     Receiver::new(Arc::new(receiving_arc)))
}

pub fn bounded_lf_room_queue<T>(capacity: usize) -> (Sender<T, LFBRArrayQueue<T>>, Receiver<T, LFBRArrayQueue<T>>)
    where {
    let inner = Inner::new(LFBRArrayQueue::new(capacity));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (Sender::new(Arc::new(sending_arc)),
     Receiver::new(Arc::new(receiving_arc)))
}

pub fn bounded_mutex_backoff_queue<T>(capacity: usize) -> (Sender<T, MQueue<T>>, Receiver<T, MQueue<T>>)
    where {
    let inner = Inner::new(MQueue::new(capacity, true));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (Sender::new(Arc::new(sending_arc)),
     Receiver::new(Arc::new(receiving_arc)))
}

pub fn bounded_mutex_no_backoff_queue<T>(capacity: usize) -> (Sender<T, MQueue<T>>, Receiver<T, MQueue<T>>)
    where {
    let inner = Inner::new(MQueue::new(capacity, false));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (Sender::new(Arc::new(sending_arc)),
     Receiver::new(Arc::new(receiving_arc)))
}
