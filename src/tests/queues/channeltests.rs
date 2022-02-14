#[cfg(test)]
pub mod channeltests {
    use std::rc::Rc;
    use std::thread::sleep;
    use std::time::Duration;

    use futures::select;
    use crate::channels::queue_channel::{bounded_lf_queue, bounded_lf_room_queue, make_mult_recv_from, Receiver, Sender};
    use crate::queues::queues::Queue;
    use crate::tests::queues::queuebench::queue_bench::BenchedQueues;

    struct Test(Rc<u32>);

    const CAPACITY: usize = 100;

    fn test_channel_<Z>(sender: Sender<Test, Z>, receiver: Receiver<Test, Z>) where Z: Queue<Test> {
        sender.send(Test(Rc::new(42)));

        assert_eq!(*receiver.recv_blk().unwrap().0, 42);
    }

    fn test_channel_mult_<Z>(sender: Sender<Test, Z>, receiver: Receiver<Test, Z>) where Z: Queue<Test> {
        let cap = sender.capacity().unwrap();

        for i in 0..cap {
            sender.send(Test(Rc::new(42)));
        }

        let mut vec = Vec::with_capacity(cap);

        assert_eq!(make_mult_recv_from(receiver).recv_mult(&mut vec).unwrap(), cap);
    }

    fn test_channel_2_threads_<Z>(sender: Sender<Test, Z>, receiver: Receiver<Test, Z>) where Z: Queue<Test> + 'static {
        let handle = std::thread::spawn(move || {
            sender.send(Test(Rc::new(42)));
        });

        assert_eq!(*receiver.recv_blk().unwrap().0, 42);

        handle.join();
    }

    fn test_channel_2_threads_async_<Z>(sender: Sender<Test, Z>, receiver: Receiver<Test, Z>) where Z: Queue<Test> + 'static {
        let join = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(250));

            assert!(sender.send(Test(Rc::new(42))).is_ok());
        });

        async_std::task::block_on(async {
            assert_eq!(*receiver.recv_fut().await.unwrap().0, 42);
        });

        join.join().unwrap();
    }

    fn test_channel_producer<Z>(sender: Sender<Test, Z>, cap: usize) where Z: Queue<Test> + 'static {
        let i1 = fastrand::u64(3000..5000);

        println!("Sleeping for {}", i1);

        std::thread::sleep(std::time::Duration::from_millis(i1));

        for i in 0..cap / 2 {
            assert!(async_std::task::block_on(sender.send_async(Test(Rc::new(42)))).is_ok());
        }

        std::thread::sleep(std::time::Duration::from_millis(250));

        for i in 0..cap / 2 {
            assert!(async_std::task::block_on(sender.send_async(Test(Rc::new(42)))).is_ok());
        }
    }

    fn test_channel_2_threads_async_mult_<Z>(sender: Sender<Test, Z>,
                                             receiver: Receiver<Test, Z>) where Z: Queue<Test> + 'static {
        let cap = sender.capacity().unwrap_or(1024);

        let mult_recv = make_mult_recv_from(receiver);

        let join = std::thread::spawn(move || {
            test_channel_producer(sender, cap);
        });

        let collected = async_std::task::block_on(async move {
            let mut collected = 0;

            loop {
                let vector = mult_recv.recv_fut().await.unwrap();

                collected += vector.len();

                if collected >= cap {
                    break;
                }
            }

            collected
        });

        assert_eq!(collected, cap);

        join.join().unwrap();
    }

    fn test_2_channel_async_send<Z>(sender: Sender<Test, Z>, receiver: Receiver<Test, Z>) where Z: Queue<Test> + 'static {
        let sender1 = sender.clone();

        let handle = std::thread::spawn(move || {
            // sleep(Duration::from_millis(100));

            // sender1.send(Test(Rc::new(43)));

            let result = receiver.recv_blk().unwrap();

            assert_eq!(*result.0, 42);
        });

        async_std::task::block_on(sender.send_async(Test(Rc::new(42))));

        handle.join();
    }

    const DEFAULT_SELECTS: usize = 3;

    async fn test_channel_select_async<Z>(mut pairs: Vec<(Sender<Test, Z>, Receiver<Test, Z>)>) where Z: Queue<Test> + 'static {
        assert_eq!(pairs.len(), DEFAULT_SELECTS);

        let mut vec = Vec::with_capacity(DEFAULT_SELECTS);

        let mut receivers = Vec::with_capacity(DEFAULT_SELECTS);

        for i in 0..DEFAULT_SELECTS {
            let (sender, recv) = pairs.pop().unwrap();

            receivers.push(recv);

            vec.push(std::thread::spawn(move || {
                let cap = sender.capacity().unwrap_or(1024);

                test_channel_producer(sender, cap);
            }));
        }

        let recv1 = make_mult_recv_from(receivers.pop().unwrap());
        let recv2 = make_mult_recv_from(receivers.pop().unwrap());
        let recv3 = make_mult_recv_from(receivers.pop().unwrap());

        select! {
            result = recv1.recv_fut() => {
                println!("Future 1 received");
            },
            result = recv2.recv_fut() => {
                println!("Future 2 received");
            },
            result = recv3.recv_fut() => {
                println!("Future 3 received");
            }
        }

        for x in vec {
            x.join();
        }
    }

    #[test]
    pub fn test_channel() {
        let (sender, receiver) = bounded_lf_queue(CAPACITY);
        test_channel_(sender, receiver);
    }

    #[test]
    pub fn test_channel_mult() {
        let (sender, receiver) = bounded_lf_queue(CAPACITY);

        test_channel_mult_(sender, receiver);
    }

    #[test]
    pub fn test_channel_2_threads() {
        let (sender, receiver) = bounded_lf_queue(CAPACITY);

        test_channel_2_threads_(sender, receiver);
    }

    #[test]
    pub fn test_channel_2_threads_async_recv() {
        let (sender, receiver) = bounded_lf_queue(CAPACITY);

        test_channel_2_threads_async_(sender, receiver);
    }

    #[test]
    pub fn test_channel_2_threads_async_recv_mult() {
        let (sender, receiver) = bounded_lf_queue(CAPACITY);

        test_channel_2_threads_async_mult_(sender, receiver);
    }

    #[test]
    pub fn test_channel_2_threads_async_send() {

        let (sender, receiver) = bounded_lf_queue(CAPACITY);

        test_2_channel_async_send(sender, receiver);
    }

    #[test]
    pub fn test_channel_multiple_threads_async() {}

    #[test]
    pub fn test_select() {
        let mut pairs = Vec::new();

        for i in 0..DEFAULT_SELECTS {
            pairs.push(bounded_lf_queue(CAPACITY));
        }

        async_std::task::block_on(test_channel_select_async(pairs));
    }
}