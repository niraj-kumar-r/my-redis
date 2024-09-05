use bytes::Bytes;
use mini_redis::{Connection, Frame};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone)]
pub struct SharedMap {
    inner: Arc<Mutex<SharedMapInner>>,
}
struct SharedMapInner {
    data: HashMap<String, Bytes>,
}

impl SharedMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedMapInner {
                data: HashMap::new(),
            })),
        }
    }

    pub fn insert(&self, key: String, value: Bytes) {
        let mut lock = self.inner.lock().unwrap();
        lock.data.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<Bytes> {
        let lock = self.inner.lock().unwrap();
        lock.data.get(&key).cloned()
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Listening");

    let db = SharedMap::new();

    loop {
        let (socket, s_addr) = listener.accept().await.unwrap();
        let db = db.clone();

        println!("Accepted connection from {:?}", s_addr);
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: SharedMap) {
    use mini_redis::Command::{self, Get, Set};

    // Connection, provided by `mini-redis`, handles parsing frames from
    // the socket
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key().to_string()) {
                    Frame::Bulk(value)
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}
