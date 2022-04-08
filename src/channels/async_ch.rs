use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{RecvError, TryRecvError};
use std::task::{Context, Poll};

use event_listener::EventListener;
use futures_core::{FusedFuture, FusedStream, Stream};

use crate::channels::queue_channel::{Receiver, ReceiverMult, ReceiverPartialMult, RecvMultError};
use crate::queues::queues::{PartiallyDumpable, Queue};

#[derive(Clone)]
enum OwnedOrRef<'a, T> {
    Owned(T),
    Ref(&'a T),
}

impl<'a, T> Deref for OwnedOrRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            OwnedOrRef::Owned(arc) => &arc,
            OwnedOrRef::Ref(r) => r,
        }
    }
}

pub struct ReceiverFut<'a, T, Z> where Z: Queue<T> {
    receiver: OwnedOrRef<'a, Receiver<T, Z>>,
    listener: Option<EventListener>,
}

pub struct ReceiverMultFut<'a, T, Z> where Z: Queue<T> {
    receiver: OwnedOrRef<'a, ReceiverMult<T, Z>>,
    listener: Option<EventListener>,
    allocated: Option<Vec<T>>,
}

pub struct ReceiverPartialMultFut<'a, T, Z> where Z: PartiallyDumpable<T> + Queue<T>{
    receiver: OwnedOrRef<'a, ReceiverPartialMult<T, Z>>,
    listener: Option<EventListener>,
    allocated: Option<Vec<T>>,
    max: usize
}

//We need this because of the allocated variable, as it's a Vec<T> which
//Requires T to be send. But allocated will always be empty if it's stored
//in the allocated variable. When it's filled, it will instantly be returned
unsafe impl<'a, T, Z> Send for ReceiverMultFut<'a, T, Z> where Z: Queue<T> {}
unsafe impl<'a, T, Z> Sync for ReceiverMultFut<'a, T, Z> where Z: Queue<T> {}
unsafe impl<'a, T, Z> Send for ReceiverPartialMultFut<'a, T, Z> where Z: PartiallyDumpable<T> + Queue<T>{}
unsafe impl<'a, T, Z> Sync for ReceiverPartialMultFut<'a, T, Z> where Z: PartiallyDumpable<T> + Queue<T>{}

impl<'a, T, Z> Unpin for ReceiverFut<'a, T, Z> where Z: Queue<T> {}

impl<'a, T, Z> Future for ReceiverFut<'a, T, Z> where Z: Queue<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = Pin::new(self);

        loop {
            match this.receiver.try_recv() {
                Ok(msg) => {
                    return Poll::Ready(Ok(msg));
                }
                Err(TryRecvError::Disconnected) => {
                    return Poll::Ready(Err(RecvError));
                }
                Err(TryRecvError::Empty) => {}
            }

            match &mut this.listener {
                None => {
                    this.receiver.inner.awaiting_sending.fetch_add(1, Ordering::Relaxed);

                    this.listener = Some(this.receiver.inner.waiting_sending.listen())
                }
                Some(listener) => {
                    match Pin::new(listener).poll(cx) {
                        Poll::Ready(_) => {
                            this.listener = None;
                            this.receiver.inner.awaiting_sending.fetch_sub(1, Ordering::Relaxed);

                            continue;
                        }
                        Poll::Pending => { return Poll::Pending; }
                    }
                }
            }
        }
    }
}

impl<'a, T, Z> FusedFuture for ReceiverFut<'a, T, Z> where Z: Queue<T> {
    fn is_terminated(&self) -> bool {
        self.receiver.inner.is_closed_recv()
    }
}

impl<T, Z> Receiver<T, Z> where Z: Queue<T> {
    pub fn recv_fut(&self) -> ReceiverFut<'_, T, Z> {
        ReceiverFut {
            receiver: OwnedOrRef::Ref(self),
            listener: None,
        }
    }

    pub fn into_recv_fut(self) -> ReceiverFut<'static, T, Z> {
        ReceiverFut {
            receiver: OwnedOrRef::Owned(self),
            listener: None,
        }
    }
}

///Implement the stream for the receiver
impl<T, Z> Stream for Receiver<T, Z> where
    Z: Queue<T> + Sync {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ev_listener) = self.listener.as_mut() {
                futures_core::ready!(Pin::new(ev_listener).poll(cx));

                self.inner.awaiting_sending.fetch_sub(1, Ordering::Relaxed);
                self.listener = Option::None;
            }

            loop {
                match self.try_recv() {
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
                                self.inner.awaiting_sending.fetch_add(1, Ordering::Relaxed);
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

impl<T, Z> FusedStream for Receiver<T, Z> where Z: Queue<T> + Sync {
    fn is_terminated(&self) -> bool {
        self.inner.is_closed_recv()
    }
}

impl<T, Z> ReceiverMult<T, Z> where Z: Queue<T> {
    pub fn recv_fut(&self) -> ReceiverMultFut<'_, T, Z> {
        ReceiverMultFut {
            receiver: OwnedOrRef::Ref(self),
            listener: None,
            allocated: None,
        }
    }

    pub fn into_recv_fut(self) -> ReceiverMultFut<'static, T, Z> {
        ReceiverMultFut {
            receiver: OwnedOrRef::Owned(self),
            listener: None,
            allocated: None,
        }
    }
}

impl<'a, T, Z> Unpin for ReceiverMultFut<'a, T, Z> where Z: Queue<T> {}

///Multiple future receiver
impl<'a, T, Z> Future for ReceiverMultFut<'a, T, Z> where Z: Queue<T> {
    type Output = Result<Vec<T>, RecvMultError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = Pin::new(self);

        let mut allocated = match this.allocated.take() {
            None => { Vec::with_capacity(this.receiver.inner.queue().capacity().unwrap()) }
            Some(allocated) => { allocated }
        };

        loop {
            match this.receiver.try_recv_mult(&mut allocated) {
                Ok(msg) => {
                    if msg > 0 {
                        // println!("Found!");
                        return Poll::Ready(Ok(allocated));
                    }
                }
                Err(RecvMultError::Disconnected) => {
                    return Poll::Ready(Err(RecvMultError::Disconnected));
                }
                Err(RecvMultError::MalformedInputVec) => {
                    return Poll::Ready(Err(RecvMultError::MalformedInputVec));
                }
                _ => {
                    return Poll::Ready(Err(RecvMultError::UnimplementedOperation))
                }
            }

            match &mut this.listener {
                None => {
                    this.receiver.inner.awaiting_sending.fetch_add(1, Ordering::Relaxed);

                    this.listener = Some(this.receiver.inner.waiting_sending.listen())
                }
                Some(listener) => {
                    match Pin::new(listener).poll(cx) {
                        Poll::Ready(_) => {
                            this.listener = None;
                            this.receiver.inner.awaiting_sending.fetch_sub(1, Ordering::Relaxed);

                            continue;
                        }
                        Poll::Pending => {
                            this.allocated = Some(allocated);

                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl<'a, T, Z> FusedFuture for ReceiverMultFut<'a, T, Z> where Z: Queue<T> {
    fn is_terminated(&self) -> bool {
        self.receiver.inner.is_closed_recv()
    }
}

impl<'a, T, Z> Stream for ReceiverMultFut<'a, T, Z> where Z: Queue<T> + Sync {
    type Item = Vec<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ev_listener) = self.listener.as_mut() {
                futures_core::ready!(Pin::new(ev_listener).poll(cx));

                self.receiver.inner.awaiting_sending.fetch_sub(1, Ordering::Relaxed);
                self.listener = Option::None;
            }

            let mut allocated_vec: Vec<T>;

            if let Some(vec) = self.allocated.take() {
                allocated_vec = vec;
            } else {
                allocated_vec = Vec::with_capacity(self.receiver.inner.queue().capacity().unwrap_or(1024));
            }

            loop {
                match self.receiver.try_recv_mult(&mut allocated_vec) {
                    Ok(msg) => {
                        if msg > 0 {
                            self.listener = None;

                            return Poll::Ready(Some(allocated_vec));
                        } else {
                            match self.listener.as_mut() {
                                None => {
                                    self.receiver.inner.awaiting_sending.fetch_add(1, Ordering::Relaxed);
                                    self.listener = Some(self.receiver.inner.waiting_sending.listen());
                                }
                                Some(_) => { break; }
                            }
                        }
                    }
                    Err(RecvMultError::Disconnected) => {
                        self.listener = None;
                        self.allocated = Some(allocated_vec);

                        return Poll::Ready(None);
                    }
                    Err(RecvMultError::MalformedInputVec) => {}
                    _ => {
                        return Poll::Ready(None);
                    }
                }
            }

            self.allocated = Some(allocated_vec);
        }
    }
}

impl<'a, T, Z> FusedStream for ReceiverMultFut<'a, T, Z> where Z: Queue<T> + Sync {
    fn is_terminated(&self) -> bool {
        self.receiver.inner.is_closed_recv()
    }
}

///Receive partial dumps from the queue
impl<T, Z> ReceiverPartialMult<T, Z> where Z: PartiallyDumpable<T> + Queue<T> {
    pub fn recv_fut(&self, size: usize) -> ReceiverPartialMultFut<'_, T, Z> {
        ReceiverPartialMultFut {
            receiver: OwnedOrRef::Ref(self),
            listener: None,
            allocated: None,
            max: size
        }
    }

    pub fn into_recv_fut(self, size: usize) -> ReceiverPartialMultFut<'static, T, Z> {
        ReceiverPartialMultFut {
            receiver: OwnedOrRef::Owned(self),
            listener: None,
            allocated: None,
            max: size
        }
    }
}

impl<'a, T, Z> Unpin for ReceiverPartialMultFut<'a, T, Z> where Z: PartiallyDumpable<T> + Queue<T> {}

///Multiple future receiver
impl<'a, T, Z> Future for ReceiverPartialMultFut<'a, T, Z> where Z: PartiallyDumpable<T> + Queue<T>{
    type Output = Result<Vec<T>, RecvMultError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = Pin::new(self);

        let mut allocated = match this.allocated.take() {
            None => { Vec::with_capacity(this.max) }
            Some(allocated) => { allocated }
        };

        loop {
            match this.receiver.try_recv_mult(&mut allocated, this.max) {
                Ok(msg) => {
                    if msg > 0 {
                        // println!("Found!");
                        return Poll::Ready(Ok(allocated));
                    }
                }
                Err(RecvMultError::Disconnected) => {
                    return Poll::Ready(Err(RecvMultError::Disconnected));
                }
                Err(RecvMultError::MalformedInputVec) => {
                    return Poll::Ready(Err(RecvMultError::MalformedInputVec));
                }
                _ => {
                    return Poll::Ready(Err(RecvMultError::UnimplementedOperation))
                }
            }

            match &mut this.listener {
                None => {
                    this.receiver.inner.awaiting_sending.fetch_add(1, Ordering::Relaxed);

                    this.listener = Some(this.receiver.inner.waiting_sending.listen())
                }
                Some(listener) => {
                    match Pin::new(listener).poll(cx) {
                        Poll::Ready(_) => {
                            this.listener = None;
                            this.receiver.inner.awaiting_sending.fetch_sub(1, Ordering::Relaxed);

                            continue;
                        }
                        Poll::Pending => {
                            this.allocated = Some(allocated);

                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl<'a, T, Z> FusedFuture for ReceiverPartialMultFut<'a, T, Z> where Z: PartiallyDumpable<T> + Queue<T>{
    fn is_terminated(&self) -> bool {
        self.receiver.inner.is_closed_recv()
    }
}

