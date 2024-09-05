use mini_redis::{Connection, Frame};
use my_redis::db::SharedMap;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

const MAX_CONNECTIONS: usize = 100;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Listening");

    let db = SharedMap::new(10);

    loop {
        let (mut socket, s_addr) = listener.accept().await.unwrap();
        let db = db.clone();

        if db.connection_count() >= MAX_CONNECTIONS {
            println!(
                "Maximum connection limit reached. Rejecting connection from {:?}",
                s_addr
            );
            socket.shutdown().await.unwrap();
            continue;
        }

        db.connection_made();
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
    db.connection_closed();
}
