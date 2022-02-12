use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_utils::{Backoff, CachePadded};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum State {
    FREE,
    OCCUPIED {
        currently_inside: u32,
        room_nr: u32,
    },
}

const CURRENTLY_INSIDE_MASK: u64 = !ROOM_NR_MASK;
const ROOM_NR_MASK: u64 = 0xFFFFFFFF00000000;

impl State {
    pub fn from_stored_state(stored_state: u64) -> Self {
        if stored_state == 0 {
            return State::FREE;
        }

        //Dislocate the currently_inside to the correct position
        let currently_inside = stored_state & CURRENTLY_INSIDE_MASK;
        let current_room_nr = (stored_state & ROOM_NR_MASK) >> 32;

        State::OCCUPIED {
            currently_inside: currently_inside as u32,
            room_nr: current_room_nr as u32,
        }
    }

    pub fn to_stored_state(&self) -> u64
    {
        let mut result: u64 = 0;

        return match self {
            State::FREE => {
                result
            }
            State::OCCUPIED { currently_inside, room_nr } => {
                let currently_inside_64 = currently_inside.clone() as u64;
                let current_room = room_nr.clone() as u64;

                result |= current_room << 32;

                //Move currently inside 32 bits to the left, making space for the room
                //In the following 32 bits
                result |= currently_inside_64;

                result
            }
        };
    }

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
                *room_nr as i32
            }
        }
    }


    pub fn enter(&self, new_room_nr: i32) -> Self {
        match self {
            State::FREE => {
                State::OCCUPIED {
                    currently_inside: 1,
                    room_nr: new_room_nr as u32,
                }
            }
            State::OCCUPIED { currently_inside, room_nr, .. } => {
                if new_room_nr as u32 == *room_nr {
                    State::OCCUPIED {
                        currently_inside: currently_inside + 1,
                        room_nr: *room_nr,
                    }
                } else {
                    *self
                }
            }
        }
    }

    pub fn leave(&self, _room_nr: i32) -> Self {
        return match self {
            State::FREE => {
                State::FREE
            }
            State::OCCUPIED { currently_inside, room_nr } => {
                if *currently_inside <= 1 {
                    return State::FREE;
                }

                State::OCCUPIED {
                    currently_inside: (*currently_inside) - 1,
                    room_nr: *room_nr,
                }
            }
        };
    }
}

///Implements a blocking rooms approach, using exponential backoff to prevent excessive contention
/// when many threads are attempting to access the protected area
///
/// Initially, the state is free.
/// Whenever any thread joins a room X, no other thread can enter any other room Y, however any number of
/// threads can enter the room X.
pub struct Rooms {
    state: CachePadded<AtomicU64>,
    room_count: u32,
}

impl Rooms {
    pub fn new(room_count: u32) -> Self {
        Self {
            state: CachePadded::new(AtomicU64::new(0)),
            room_count,
        }
    }

    fn current_state(&self) -> State {
        State::from_stored_state(
            self.state.load(Ordering::Relaxed))
    }

    pub fn room_count(&self) -> u32 {
        self.room_count
    }

    fn change_state_blk<F>(&self, apply: F, room: i32, success_ordering: Ordering) where F: Fn(&State, i32) -> State + Copy {
        let backoff = Backoff::new();

        let mut x = self.state.load(Ordering::Relaxed);

        loop {
            let state = State::from_stored_state(x);

            match state {
                State::FREE => {}
                State::OCCUPIED { room_nr, .. } => {
                    if room_nr != room as u32 {
                        backoff.snooze();

                        x = self.state.load(Relaxed);

                        continue;
                    }
                }
            }

            let new_state = apply(&state, room);

            match self.state.compare_exchange_weak(x, new_state.to_stored_state(),
                                                   success_ordering,
                                                   Ordering::Relaxed) {
                Ok(_state) => {
                    break;
                }
                Err(state) => {
                    x = state;
                }
            }
        }
    }

    fn change_state<F>(&self, apply: F, room: i32, success_ordering: Ordering) -> Result<(), RoomAcquireError> where F: Fn(&State, i32) -> State + Copy {
        let x = self.state.load(Ordering::Relaxed);

        let state = State::from_stored_state(x);

        match state {
            State::FREE => {}
            State::OCCUPIED { room_nr, .. } => {
                if room_nr != room as u32 {
                    return Err(RoomAcquireError::FailedRoomOccupied);
                }
            }
        }

        let new_state = apply(&state, room);

        return match self.state.compare_exchange_weak(x, new_state.to_stored_state(),
                                                      success_ordering,
                                                      Ordering::Relaxed) {
            Ok(_state) => {
                Ok(())
            }
            Err(_err) => {
                Err(RoomAcquireError::FailedToGetLock)
            }
        };
    }

    ///Attempt to enter the given room
    ///This will use the success ordering of Relaxed
    pub fn enter_blk(&self, room: i32) -> Result<(), RoomAcquireError> {
        self.enter_blk_ordered(room, Relaxed)
    }

    ///Attempt to enter the given room using the provided success memory ordering
    pub fn enter_blk_ordered(&self, room: i32, success_ordering: Ordering) -> Result<(), RoomAcquireError> {
        if room <= 0 || room > self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        self.change_state_blk(State::enter, room, success_ordering);

        Ok(())
    }

    ///Attempt to leave the given room
    ///This will use the success ordering of Relaxed
    pub fn leave_blk(&self, room: i32) -> Result<(), RoomAcquireError> {
        self.leave_blk_ordered(room, Relaxed)
    }

    ///Attempt to leave the given room using the provided success memory ordering
    pub fn leave_blk_ordered(&self, room: i32, success_ordering: Ordering) -> Result<(), RoomAcquireError> {
        if room <= 0 || room > self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        self.change_state_blk(State::leave, room, success_ordering);

        Ok(())
    }

    pub fn enter(&self, room: i32) -> Result<(), RoomAcquireError> {
        self.enter_ordered(room, Relaxed)
    }

    pub fn enter_ordered(&self, room: i32, success_ordering: Ordering)-> Result<(), RoomAcquireError> {
        if room <= 0 || room > self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        return self.change_state(State::enter, room, success_ordering);
    }

    pub fn leave(&self, room: i32) -> Result<(), RoomAcquireError> {
        self.leave_ordered(room, Relaxed)
    }

    pub fn leave_ordered(&self, room: i32, success_ordering: Ordering) -> Result<(), RoomAcquireError> {
        if room <= 0 || room > self.room_count() as i32 {
            return Err(RoomAcquireError::NoRoom);
        }

        return self.change_state(State::leave, room, success_ordering);
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
pub mod rooms_tests {
    use crate::utils::rooms::Rooms;

    const ROOM_1: i32 = 1;
    const ROOM_2: i32 = 2;
    const ROOMS: u32 = 2;

    #[test]
    fn test_rooms_sequential() {
        let rooms = Rooms::new(ROOMS);

        println!("State {:?}", rooms.current_state());

        assert!(rooms.enter(ROOM_1).is_ok());
        println!("State {:?}", rooms.current_state());

        assert!(rooms.enter(ROOM_2).is_err());
        println!("State {:?}", rooms.current_state());

        assert!(rooms.enter(ROOM_1).is_ok());
        println!("State {:?}", rooms.current_state());

        assert!(rooms.leave(ROOM_1).is_ok());
        println!("State {:?}", rooms.current_state());
        assert!(rooms.leave(ROOM_1).is_ok());
        println!("State {:?}", rooms.current_state());

        assert!(rooms.enter(ROOM_2).is_ok());
        println!("State {:?}", rooms.current_state());

        assert!(rooms.enter(ROOM_1).is_err());
        println!("State {:?}", rooms.current_state());
    }

    #[test]
    fn test_multi_threading() {
        let rooms = Rooms::new(ROOMS);
    }
}