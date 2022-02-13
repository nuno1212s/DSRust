#[cfg(test)]
pub mod channeltests {
    use std::rc::Rc;
    use crate::channels::queue_channel::{bounded_lf_queue, bounded_lf_room_queue, make_mult_recv_from, Receiver, Sender};

    struct Test(Rc<u32>);

    const CAPACITY: usize = 100;

    #[test]
    pub fn test_channel() {
        let (sender, receiver) = bounded_lf_room_queue(CAPACITY);

        sender.send(Test(Rc::new(42)));

        assert_eq!(*receiver.recv_blk().unwrap().0, 42);
    }

    #[test]
    pub fn test_channel_mult() {
        let (sender, receiver) = bounded_lf_room_queue(CAPACITY);

        let cap = sender.capacity().unwrap();

        for i in 0..cap {
            sender.send(Test(Rc::new(42)));
        }

        let mut vec = Vec::with_capacity(cap);

        assert_eq!(make_mult_recv_from(receiver).recv_mult(&mut vec).unwrap(), cap);
    }

    #[test]
    pub fn test_channel_2_threads() {
        let (sender, receiver) = bounded_lf_room_queue(CAPACITY);

        let handle = std::thread::spawn(move || {
            sender.send(Test(Rc::new(42)));
        });

        assert_eq!(*receiver.recv_blk().unwrap().0, 42);

        handle.join();
    }

    #[test]
    pub fn test_channel_2_threads_async() {
        let (sender, receiver) = bounded_lf_room_queue(CAPACITY);

        let join = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(250));

            assert!(sender.send(Test(Rc::new(42))).is_ok());
        });

        async_std::task::block_on(async {
            assert_eq!(*receiver.recv_fut().await.unwrap().0, 42);
        });

        join.join().unwrap();
    }
}