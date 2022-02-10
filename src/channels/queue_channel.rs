use std::error::Error;
use std::fmt::{Debug, Formatter, write};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::Display;
use std::pin::Pin;
use std::stream::Stream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{RecvError, SendError, TryRecvError, TrySendError};
use std::task::{Context, Poll};

use crossbeam_utils::{Backoff, CachePadded};
use event_listener::{Event, EventListener};
use fastrand::usize;

use crate::queues::lf_array_queue::LFBQueue;
use crate::queues::mqueue::MQueue;
use crate::queues::queues::{Queue, QueueError};
use crate::queues::rooms_array_queue::LFBRArrayQueue;

pub struct ChannelTx<T, Z> where T: Send + Debug,
                                 Z: Queue<T> + Send + Sync {
    inner: Sender<T, Z>,
}

pub struct ChannelRx<T, Z> where T: Send + Debug,
                                 Z: Queue<T> + Send + Sync {
    inner: Receiver<T, Z>,
}

pub struct ChannelRxFut<'a, T, Z> where T: Send + Debug,
                                        Z: Queue<T> + Send + Sync {
    inner: &'a mut Receiver<T, Z>,
}

impl<T, Z> ChannelTx<T, Z> where T: Send + Debug,
                                 Z: Queue<T> + Send + Sync {
    #[inline]
    pub async fn send(&mut self, message: T) -> Result<(), SendError<T>> {
        self.inner.send_async(message).await
    }
}

impl<T, Z> ChannelRx<T, Z> where T: Send + Debug,
                                 Z: Queue<T> + Send + Sync {
    ///Async receiver with no backoff (Turns straight to event notifications)
    #[inline]
    pub fn recv<'a>(&'a mut self) -> ChannelRxFut<'a, T, Z> {
        let inner = &mut self.inner;

        ChannelRxFut { inner }
    }

    ///Blocking receiver with backoff and event notification on backoff complete
    #[inline]
    pub fn recv_backoff(&self) -> Result<T, RecvError> {
        self.inner.inner.recv()
    }

    ///Async receiver with backoff and async event notification on backoff complete
    #[inline]
    pub async fn recv_async_backoff(&self) -> Result<T, RecvError> {
        self.inner.inner.recv_async().await
    }

    ///A blocking receiver with backoff and event notification on backoff complete
    #[inline]
    pub fn recv_mult_with_target(&self, target: &mut Vec<T>) -> Result<usize, RecvError> {
        return match self.inner.inner.recv_mult(target) {
            Ok(std) => {
                Ok(std)
            }
            Err(_) => {
                Err(RecvError)
            }
        };
    }

    ///A blocking receiver with backoff and event notification on backoff complete
    #[inline]
    pub fn recv_mult(&self) -> Result<Vec<T>, RecvError> {

        let mut target = Vec::with_capacity(self.inner.inner.queue.capacity().unwrap_or(1024));

        return match self.inner.inner.recv_mult(&mut target) {
            Ok(_) => {
                Ok(target)
            }
            Err(_) => {
                Err(RecvError)
            }
        };
    }


    ///Async receiver with backoff and async event notification on backoff complete
    #[inline]
    pub async fn recv_mult_with_target_async(&self, target: &mut Vec<T>) -> Result<usize, RecvError> {
        return match self.inner.inner.recv_mult_async(target).await {
            Ok(std) => {
                Ok(std)
            }
            Err(_) => {
                Err(RecvError)
            }
        };
    }

    ///Async receiver with backoff and async event notification on backoff complete
    #[inline]
    pub async fn recv_mult_async(&self) -> Result<Vec<T>, RecvError> {
        let mut target = Vec::with_capacity(self.inner.inner.queue.capacity().unwrap_or(1024));

        return match self.inner.inner.recv_mult_async(&mut target).await {
            Ok(_) => {
                Ok(target)
            }
            Err(_) => {
                Err(RecvError)
            }
        };
    }

}

impl<'a, T, Z> Future for ChannelRxFut<'a, T, Z> where T: Send + Debug,
                                                       Z: Queue<T> + Send + Sync {
    type Output = Result<T, RecvError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|opt| opt.ok_or(RecvError))
    }
}

pub struct Sender<T, Z> where T: Send + Debug,
                              Z: Queue<T> + Send + Sync {
    inner: Arc<SendingInner<T, Z>>,
    phantom: PhantomData<T>,
}

pub struct Receiver<T, Z> where T: Send + Debug,
                                Z: Queue<T> + Send + Sync {
    inner: Arc<ReceivingInner<T, Z>>,
    phantom: PhantomData<T>,
    listener: Option<EventListener>,
}

impl<T, Z> Sender<T, Z> where T: Send + Debug,
                              Z: Queue<T> + Send + Sync {
    fn new(inner: Arc<SendingInner<T, Z>>) -> Self {
        Self {
            inner,
            phantom: PhantomData::default(),
        }
    }

    ///Only notifies the threads if there were any listeners registered
    fn notify_if_necessary(&self) {
        if self.inner.awaiting_reception.load(Ordering::Acquire) > 0 {
            self.inner.waiting_reception.notify_relaxed(usize::MAX);
        }
    }

    fn try_send(&self, obj: T) -> Result<(), TrySendError<T>> {
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

    async fn send_async(&self, elem: T) -> Result<(), SendError<T>> {
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

impl<T, Z> Clone for Sender<T, Z> where T: Send + Debug,
                                        Z: Queue<T> + Send + Sync {
    fn clone(&self) -> Self {
        return Self::new(self.inner.clone());
    }
}

impl<T, Z> Receiver<T, Z> where T: Send + Debug,
                                Z: Queue<T> + Send + Sync {
    fn new(inner: Arc<ReceivingInner<T, Z>>) -> Self {
        Self {
            inner,
            phantom: PhantomData::default(),
            listener: Option::None,
        }
    }
}

impl<T, Z> Unpin for Receiver<T, Z> where T: Send + Debug,
                                          Z: Queue<T> + Send + Sync {}

///Implement the stream for the receiver
impl<T, Z> Stream for Receiver<T, Z> where T: Send + Debug,
                                           Z: Queue<T> + Send + Sync {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ev_listener) = self.listener.as_mut() {
                futures_core::ready!(Pin::new(ev_listener).poll(cx));

                self.listener = Option::None;
            }

            loop {
                match self.inner.try_recv() {
                    Ok(msg) => {
                        self.listener = None;

                        return Poll::Ready(Some(msg));
                    }
                    Err(TryRecvError::Disconnected) => {
                        self.listener = None;

                        return Poll::Ready(None);
                    }
                    Err(TryRecvError::Empty) => {
                        match self.listener.as_mut() {
                            None => {
                                self.listener = Some(self.inner.waiting_sending.listen());
                            }
                            Some(_) => { break; }
                        }
                    }
                }
            }
        }
    }
}

impl<T, Z> Clone for Receiver<T, Z> where T: Send + Debug,
                                          Z: Queue<T> + Send + Sync {
    fn clone(&self) -> Self {
        return Self::new(self.inner.clone());
    }
}

///We have this extra abstractions so we can keep
///Count of how many sending clone there are without
///having to count them, as when this sending inner
///gets disposed of, it means that no other processes
///Are listening, so the channel is effectively closed
struct SendingInner<T, Z> where T: Send + Debug,
                                Z: Queue<T> + Send + Sync {
    inner: Arc<Inner<T, Z>>,
    phantom: PhantomData<T>,
}

struct ReceivingInner<T, Z> where T: Send + Debug,
                                  Z: Queue<T> + Send + Sync {
    inner: Arc<Inner<T, Z>>,
    phantom: PhantomData<T>,
}

impl<T, Z> SendingInner<T, Z> where T: Send + Debug,
                                    Z: Queue<T> + Send + Sync {
    fn new(inner: Arc<Inner<T, Z>>) -> Self {
        Self {
            inner,
            phantom: PhantomData::default(),
        }
    }
}

impl<T, Z> ReceivingInner<T, Z> where T: Send + Debug,
                                      Z: Queue<T> + Send + Sync {
    fn new(inner: Arc<Inner<T, Z>>) -> Self {
        Self {
            inner,
            phantom: PhantomData::default(),
        }
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

    pub fn recv(&self) -> Result<T, RecvError> {
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

    pub async fn recv_async(&self) -> Result<T, RecvError> {
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
            } else {
                backoff.snooze();
            }
        }
    }
}

impl<T, Z> Deref for ReceivingInner<T, Z> where T: Send + Debug,
                                                Z: Queue<T> + Send + Sync {
    type Target = Arc<Inner<T, Z>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, Z> Drop for ReceivingInner<T, Z> where T: Send + Debug,
                                               Z: Queue<T> + Send + Sync {
    fn drop(&mut self) {
        self.inner.is_dc.store(true, Ordering::Relaxed);

        //Notify all waiting processes that the queue is now closed
        self.inner.waiting_sending.notify(usize::MAX);
        self.inner.waiting_reception.notify(usize::MAX);
    }
}

impl<T, Z> Deref for SendingInner<T, Z> where T: Send + Debug,
                                              Z: Queue<T> + Send + Sync {
    type Target = Arc<Inner<T, Z>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, Z> Drop for SendingInner<T, Z> where T: Send + Debug,
                                             Z: Queue<T> + Send + Sync {
    fn drop(&mut self) {
        self.inner.is_dc.store(true, Ordering::Relaxed);

        //Notify all waiting processes that the queue is now closed
        self.inner.waiting_sending.notify(usize::MAX);
        self.inner.waiting_reception.notify(usize::MAX);
    }
}

struct Inner<T, Z> where T: Send + Debug,
                         Z: Queue<T> + Send + Sync {
    queue: Z,
    //Is the channel disconnected
    is_dc: AtomicBool,
    //Sleeping event to allow threads that are waiting for a request
    //To go to sleep efficiently
    awaiting_reception: CachePadded<AtomicU32>,
    awaiting_sending: CachePadded<AtomicU32>,
    waiting_reception: Event,
    waiting_sending: Event,
    phantom: PhantomData<T>,
}

impl<T, Z> Inner<T, Z> where T: Send + Debug,
                             Z: Queue<T> + Send + Sync {
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

pub fn bounded_lf_queue<T>(capacity: usize) -> (ChannelTx<T, LFBQueue<T>>, ChannelRx<T, LFBQueue<T>>)
    where T: Send + Debug {
    let inner = Inner::new(LFBQueue::new(capacity));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (ChannelTx { inner: Sender::new(Arc::new(sending_arc)) },
     ChannelRx { inner: Receiver::new(Arc::new(receiving_arc)) })
}

pub fn bounded_lf_room_queue<T>(capacity: usize) -> (ChannelTx<T, LFBRArrayQueue<T>>, ChannelRx<T, LFBRArrayQueue<T>>)
    where T: Send + Debug {
    let inner = Inner::new(LFBRArrayQueue::new(capacity));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (ChannelTx { inner: Sender::new(Arc::new(sending_arc)) },
     ChannelRx { inner: Receiver::new(Arc::new(receiving_arc)) })
}

pub fn bounded_mutex_backoff_queue<T>(capacity: usize) -> (ChannelTx<T, MQueue<T>>, ChannelRx<T, MQueue<T>>)
    where T: Send + Debug {
    let inner = Inner::new(MQueue::new(capacity, true));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (ChannelTx { inner: Sender::new(Arc::new(sending_arc)) },
     ChannelRx { inner: Receiver::new(Arc::new(receiving_arc)) })
}

pub fn bounded_mutex_no_backoff_queue<T>(capacity: usize) -> (ChannelTx<T, MQueue<T>>, ChannelRx<T, MQueue<T>>)
    where T: Send + Debug {
    let inner = Inner::new(MQueue::new(capacity, false));

    let inner_arc = Arc::new(inner);
    let sending_arc = SendingInner::new(inner_arc.clone());
    let receiving_arc = ReceivingInner::new(inner_arc);

    (ChannelTx { inner: Sender::new(Arc::new(sending_arc)) },
     ChannelRx { inner: Receiver::new(Arc::new(receiving_arc)) })
}
