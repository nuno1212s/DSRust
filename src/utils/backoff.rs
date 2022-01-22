use std::borrow::BorrowMut;
use std::cell::Cell;
use std::{alloc, mem};
use std::error::Error;
use std::fmt::{Display, Formatter, write};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
use std::sync::{Mutex, TryLockResult};
use std::thread::sleep;
use std::time::Duration;
use std::u64::MAX;
use rand::RngCore;

thread_local! {
    static CURRENT_BACKOFF : Cell<u64> = Cell::new(1);
}

static MAX_DELAY: u64 = Duration::from_secs(1).as_millis() as u64;
static MULTIPLIER: u64 = 2;

pub struct Backoff {}

impl Backoff {
    pub fn backoff() {
        CURRENT_BACKOFF.with(move |f| {
            let mut current_backoff = f.get();

            current_backoff *= MULTIPLIER;

            if current_backoff >= MAX_DELAY {
                current_backoff = MAX_DELAY;
            }

            let mut rng = rand::thread_rng();

            let sleep_dur = rng.next_u64() % current_backoff;

            f.replace(current_backoff);

            sleep(Duration::from_millis(sleep_dur));
        });
    }

    pub fn reset() {
        CURRENT_BACKOFF.with(move |f| {
            let _previous = f.replace(1);
        });
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum State {
    FREE,
    OCCUPIED {
        currently_inside: u32,
        room_nr: i32,
    },
}

impl State {
    pub fn currently_inside(&self) -> i32 {
        match self {
            State::FREE => { -1 }
            State::OCCUPIED { currently_inside, .. } => {
                currently_inside.clone() as i32
            }
        }
    }

    pub fn room_nr(&self) -> i32 {
        match self {
            State::FREE => { -1 }
            State::OCCUPIED { room_nr, .. } => {
                *room_nr
            }
        }
    }

    pub fn enter(self) -> Self {
        match self {
            State::FREE => {
                self
            }
            State::OCCUPIED { mut currently_inside, .. } => {
                currently_inside += 1;

                self
            }
        }
    }

    pub fn leave(self) -> Self {
        match self {
            State::FREE => {
                self
            }
            State::OCCUPIED { mut currently_inside, .. } => {
                if currently_inside <= 1 {
                    return State::FREE;
                }

                currently_inside -= 1;
                self
            }
        }
    }
}

///Implements a blocking rooms approach, using exponential backoff to prevent excessive contention
/// when many threads are attempting to access the protected area
///
/// Initially, the state is free.
/// Whenever any thread joins a room X, no other thread can enter any other room Y, however any number of
/// threads can enter the room X.
pub struct Rooms {
    state: Mutex<State>,
    room_count: u32,
}

impl Rooms {
    pub fn new(room_count: u32) -> Self {
        Self {
            state: Mutex::new(State::FREE),
            room_count,
        }
    }

    pub fn room_count(&self) -> u32 {
        self.room_count
    }

    fn change_state<F>(&self, apply: F, room: i32) where F: FnOnce(State) -> State {
        loop {
            let lock_result = self.state.try_lock();

            match lock_result {
                Ok(mut lock_guard) => {

                    match lock_guard.deref() {
                        State::FREE => {}
                        State::OCCUPIED { room_nr, .. } => {
                            if room != *room_nr {
                                drop(lock_guard);

                                Backoff::backoff();

                                continue
                            }
                        }
                    }

                    //We only want to occupy the room when room == the current room_nr or the state is free

                    *lock_guard = apply.call_once((*lock_guard, ));

                    Backoff::reset();
                    break
                }
                Err(_) => {
                    Backoff::backoff();
                }
            }
        }
    }

    pub fn enter(&self, room: i32) -> Result<(), RoomAcquireError> {
        if room <= 0 || room >= self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        self.change_state(State::enter, room);

        Ok(())
    }

    pub fn leave(&self, room: i32) -> Result<(), RoomAcquireError>  {
        if room <= 0 || room >= self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        self.change_state(State::leave, room);

        Ok(())
    }
}

#[derive(Debug)]
pub enum RoomAcquireError {
    NoRoom
}

impl Error for RoomAcquireError {}

impl Display for RoomAcquireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoomAcquireError::NoRoom => {
                write!(f, "No room by that ID")
            }
        }
    }
}
