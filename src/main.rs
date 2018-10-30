extern crate futures;
extern crate tokio;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate tokio_threadpool;
extern crate futures_cpupool;

// instead of tokio::prelude, which also re-exports streams and futures,
// we use the futures crate directly to get access to futures::sync::mspc
use futures::*;

use std::thread;
use std::time::Duration;
use std::fmt;
use std::collections::HashMap;
use std::io;
use std::io::Read;

use tokio_threadpool::Builder;
use futures_cpupool::CpuPool;

#[derive(Clone, Debug)]
struct Command {
    user_id: u32,
    payload: u32
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Command (payload: {})", self.payload)
    }
}

impl Command {
    fn new(user_id: u32, payload: u32) -> Self {
        Command {
            user_id,
            payload
        }
    }
}

struct Processor {
    user_id: u32,
    count: u32,
}

impl Processor {
    fn new(user_id: u32) -> Self {
        Processor {
            user_id,
            count: 0
        }
    }

    fn do_process(&mut self, command: &Command) {
        info!("I'm processing {} for user {}: {}", command, self.user_id, self.count);
        self.count += command.payload;
    }
}

struct ProcessorAcceptor {
    // user sends command here
    sender: sync::mpsc::Sender<Command>
}

impl ProcessorAcceptor {
    fn new(user_id: u32) -> Self {
        let (sender, receiver) = sync::mpsc::channel::<Command>(2);

        let result = ProcessorAcceptor {
            sender,
        };

        let mut processor = Processor::new(user_id);
        tokio::spawn(
            receiver
                .map(move |x| { processor.do_process(&x) })
                .fold((), |_, _| Ok(()))
        );

        result
    }

    fn send(&self, command: Command) {
        self.sender.clone().send_all(stream::once(Ok(command))).wait().ok();
    }
}

struct Multiplexor {
}

impl Multiplexor {
    fn new() -> Self {
        Multiplexor {
        }
    }

    fn run(&self, receiver: sync::mpsc::Receiver<Command>) {
        //let pool = CpuPool::new(4);
        let thread_pool = Builder::new()
            .pool_size(4)
            .keep_alive(Some(Duration::from_secs(30)))
            .build();

        let mut processors = HashMap::new();

        thread_pool.sender().spawn(
        //tokio::spawn(
        //pool.sender().spawn(
            receiver
                .map(move |x| {
                    info!("Got from receiver: {}", x);
                    let user_id = x.user_id;
                    let acceptor = processors.entry(user_id).or_insert_with(|| ProcessorAcceptor::new(user_id));
                    acceptor.send(x);
                })
                // turn the stream into a future that waits for the end of the stream.
                .fold((), |_, _| Ok(()))
        ).unwrap();
    }
}

fn main() {
    let env = env_logger::Env::default()
        .filter_or(env_logger::DEFAULT_FILTER_ENV, "info");
 
    env_logger::Builder::from_env(env).init();

    //
    // tokio::run starts a standard reactor core. all further future/stream
    // execution must be done inside this.
    //
    tokio::run(future::ok(()).map(|_| {
        //
        // multi producer, single consumer from the futures package bounded queue of 2.
        //
        let (sender, receiver) = sync::mpsc::channel::<Command>(2);

        // the sender side goes into a thread and emits 14, 15, 16.
        thread::spawn(move || {
            //
            // imagine we get a callback from c in some separate thread with some data
            // to send into the stream.
            //   1. send_all consumes the sender, hence .clone()
            //   2. stream::once(Ok(v)) to send a single value.
            //      (this specific example could have been done better with stream::iter_ok())
            //
            let mut n = 0;
            loop {
                let command = Command::new(n % 5,n);
                sender.clone().send_all(stream::once(Ok(command))).wait().ok();
                info!("Sent {}", n);
                n += 1;
                //thread::sleep(Duration::from_millis(300));
            }
        });

        Multiplexor::new().run(receiver);

        let (tx, rx) = sync::oneshot::channel();
        thread::spawn(move || {
            println!("Press ENTER to exit...");
            let _ = io::stdin().read(&mut [0]).unwrap();
            tx.send(())
        });
        let _ = rx.wait();
    }));
}
