use std::borrow::Borrow;
use std::cell::Cell;
use std::cmp::min;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::{Condvar, Mutex, MutexGuard, TryLockResult};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

thread_local! {
    static CURRENT_BACKOFF : Cell<u64> = Cell::new(MIN_DELAY);
}

static MAX_DELAY: u64 = Duration::from_millis(1).as_nanos() as u64 - 1;
static MIN_DELAY: u64 = 1;
static MULTIPLIER: u64 = 2;

pub struct Backoff {}

impl Backoff {
    pub fn backoff() {
        CURRENT_BACKOFF.with(|f| {
            let mut current_backoff = f.get();

            let sleep_dur = fastrand::u64((0..current_backoff));

            //thread::sleep(Duration::from_nanos(sleep_dur));

            f.replace(min(current_backoff * MULTIPLIER, MAX_DELAY));
        });
    }

    pub fn reset() {
        CURRENT_BACKOFF.with(|f| {
            let _previous = f.replace(MIN_DELAY);
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

    pub fn enter(mut self, room_nr: i32) -> Self {
        match self {
            State::FREE => {
                State::OCCUPIED {
                    currently_inside: 1,
                    room_nr,
                }
            }
            State::OCCUPIED { ref mut currently_inside, room_nr } => {
                *currently_inside += 1;

                self
            }
        }
    }

    pub fn leave(mut self, _room_nr: i32) -> Self {
        match self {
            State::FREE => {
                self
            }
            State::OCCUPIED { ref mut currently_inside, room_nr } => {
                if *currently_inside <= 1 {
                    return State::FREE;
                }

                *currently_inside -= 1;

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
    backoff: bool,
    cond_var: Option<Condvar>,
}

impl Rooms {
    pub fn new_backoff(room_count: u32, backoff: bool) -> Self {
        Self {
            state: Mutex::new(State::FREE),
            room_count,
            backoff,
            cond_var: if backoff { None } else { Some(Condvar::new()) },
        }
    }

    pub fn new(room_count: u32) -> Self {
        Self {
            state: Mutex::new(State::FREE),
            room_count,
            backoff: true,
            cond_var: None,
        }
    }

    fn backoff(&self) -> bool {
        self.backoff
    }

    fn cond_var(&self) -> Option<&Condvar> {
        self.cond_var.as_ref()
    }

    pub fn room_count(&self) -> u32 {
        self.room_count
    }

    fn change_state_blk<F>(&self, apply: F, room: i32) where F: FnOnce(State, i32) -> State {
        if self.backoff() {
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

                                    continue;
                                }
                            }
                        }

                        //We only want to occupy the room when room == the current room_nr or the state is free
                        *lock_guard = apply.call_once((*lock_guard, room));

                        break;
                    }
                    Err(_) => {
                        Backoff::backoff();
                    }
                }
            }

            Backoff::reset();
        } else {
            let mut lock_guard = self.state.lock().unwrap();

            loop {
                match lock_guard.deref() {
                    State::FREE => {
                        break;
                    }
                    State::OCCUPIED { room_nr, .. } => {
                        if room != *room_nr {
                            lock_guard = self.cond_var().unwrap().wait(lock_guard).unwrap();

                            continue;
                        }

                        //If we are in the correct room, proceed
                        break;
                    }
                }
            }

            *lock_guard = apply.call_once((*lock_guard, room));

            self.cond_var().unwrap().notify_all();
        }
    }

    fn change_state<F>(&self, apply: F, room: i32) -> Result<(), RoomAcquireError> where F: FnOnce(State, i32) -> State {
        let lock_result = self.state.try_lock();

        return match lock_result {
            Ok(mut lock_guard) => {
                match lock_guard.deref() {
                    State::FREE => {}
                    State::OCCUPIED { room_nr, .. } => {
                        if room != *room_nr {
                            drop(lock_guard);

                            return Err(RoomAcquireError::FailedRoomOccupied);
                        }
                    }
                }

                //We only want to occupy the room when room == the current room_nr or the state is free
                *lock_guard = apply.call_once((*lock_guard, room));

                if !self.backoff() {
                    (&self.cond_var().unwrap()).notify_all();
                }

                Ok(())
            }
            Err(_) => {
                Err(RoomAcquireError::FailedToGetLock)
            }
        }
    }

    pub fn enter_blk(&self, room: i32) -> Result<(), RoomAcquireError> {
        if room <= 0 || room > self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        self.change_state_blk(State::enter, room);

        Ok(())
    }

    pub fn leave_blk(&self, room: i32) -> Result<(), RoomAcquireError> {
        if room <= 0 || room > self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        self.change_state_blk(State::leave, room);

        Ok(())
    }

    pub fn enter(&self, room: i32) -> Result<(), RoomAcquireError> {
        if room <= 0 || room > self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        return self.change_state(State::enter, room);
    }

    pub fn leave(&self, room: i32) -> Result<(), RoomAcquireError> {
        if room <= 0 || room > self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        return self.change_state(State::leave, room);
    }
}

#[derive(Debug)]
pub enum RoomAcquireError {
    NoRoom,
    FailedToGetLock,
    FailedRoomOccupied,
}

impl Error for RoomAcquireError {}

impl Display for RoomAcquireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoomAcquireError::NoRoom => {
                write!(f, "No room by that ID")
            }
            RoomAcquireError::FailedToGetLock => {
                write!(f, "Failed to get the lock")
            }
            RoomAcquireError::FailedRoomOccupied => {
                write!(f, "Failed as another room is already occupied")
            }
        }
    }
}

#[cfg(test)]
mod util_tests {
    use crate::utils::backoff::{RoomAcquireError, Rooms};

    const ROOM_1: i32 = 1;
    const ROOM_2: i32 = 2;
    const ROOMS: u32 = 2;

    #[test]
    fn test_rooms_sequential() {
        let rooms = Rooms::new(ROOMS);

        assert!(rooms.enter(ROOM_1).is_ok());

        assert!(rooms.enter(ROOM_2).is_err());

        assert!(rooms.enter(ROOM_1).is_ok());

        assert!(rooms.leave(ROOM_1).is_ok());
        assert!(rooms.leave(ROOM_1).is_ok());

        assert!(rooms.enter(ROOM_2).is_ok());

        assert!(rooms.enter(ROOM_1).is_err());
    }

    #[test]
    fn test_multi_threading() {
        let rooms = Rooms::new(ROOMS);
    }
}