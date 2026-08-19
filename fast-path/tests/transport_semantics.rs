use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const IO_TIMEOUT: Duration = Duration::from_secs(3);

struct Gateway {
    child: Child,
    address: SocketAddr,
}

impl Gateway {
    fn spawn(upstream: SocketAddr) -> Self {
        let address = reserve_address();
        let child = Command::new(env!("CARGO_BIN_EXE_tempera-browser-fastpath"))
            .args([
                "--listen",
                &address.to_string(),
                "--upstream",
                &upstream.to_string(),
                "--observe-ttl-ms",
                "0",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fast-path gateway");
        let gateway = Self { child, address };
        gateway.wait_until_ready();
        gateway
    }

    fn wait_until_ready(&self) {
        let deadline = Instant::now() + IO_TIMEOUT;
        loop {
            if TcpStream::connect_timeout(&self.address, Duration::from_millis(50)).is_ok() {
                return;
            }
            assert!(Instant::now() < deadline, "gateway did not become ready");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect_timeout(&self.address, IO_TIMEOUT)
            .expect("connect to fast-path gateway");
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .expect("set gateway read timeout");
        stream
            .set_write_timeout(Some(IO_TIMEOUT))
            .expect("set gateway write timeout");
        stream
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let address = listener.local_addr().expect("read reserved address");
    drop(listener);
    address
}

fn read_request(stream: TcpStream) -> String {
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .expect("set upstream timeout");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read upstream request");
    line
}

#[test]
fn mutation_is_not_replayed_after_ambiguous_upstream_eof() {
    let upstream = TcpListener::bind("127.0.0.1:0").expect("bind fake upstream");
    upstream
        .set_nonblocking(true)
        .expect("set fake upstream nonblocking");
    let upstream_address = upstream.local_addr().expect("fake upstream address");
    let (tx, rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut deliveries = Vec::new();
        while Instant::now() < deadline {
            match upstream.accept() {
                Ok((stream, _)) => {
                    deliveries.push(read_request(stream));
                    // Drop without a response. Once the gateway has written the
                    // mutation, it cannot know whether a real daemon executed it.
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("fake upstream accept failed: {error}"),
            }
            if !deliveries.is_empty() {
                // Leave enough time for an incorrect reconnect/replay to arrive.
                thread::sleep(Duration::from_millis(250));
                while let Ok((stream, _)) = upstream.accept() {
                    deliveries.push(read_request(stream));
                }
                break;
            }
        }
        tx.send(deliveries).expect("report deliveries");
    });

    let gateway = Gateway::spawn(upstream_address);
    let mut client = gateway.connect();
    let mutation = r#"{"command":{"name":"click"},"sessionId":"s1","targetId":"t1"}"#;
    writeln!(client, "{mutation}").expect("send mutation");
    client.flush().expect("flush mutation");

    let mut response = String::new();
    let _ = BufReader::new(client).read_line(&mut response);

    let deliveries = rx.recv_timeout(IO_TIMEOUT).expect("receive delivery count");
    server.join().expect("join fake upstream");
    assert_eq!(deliveries.len(), 1, "mutation must be delivered at most once");
    assert_eq!(deliveries[0].trim_end(), mutation);
}

#[test]
fn readonly_observation_reconnects_once_with_identical_request() {
    let upstream = TcpListener::bind("127.0.0.1:0").expect("bind fake upstream");
    let upstream_address = upstream.local_addr().expect("fake upstream address");
    let (tx, rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (first, _) = upstream.accept().expect("accept first observation");
        let first_request = read_request(first);
        // First connection closes before a response, which is safe to replay
        // because this request is classified as read-only.
        let (mut second, _) = upstream.accept().expect("accept retried observation");
        let second_request = read_request(second.try_clone().expect("clone second stream"));
        second
            .write_all(b"{\"success\":true,\"data\":{\"source\":\"retry\"}}\n")
            .expect("write retried response");
        second.flush().expect("flush retried response");
        tx.send((first_request, second_request))
            .expect("report observation requests");
    });

    let gateway = Gateway::spawn(upstream_address);
    let mut client = gateway.connect();
    let observation =
        r#"{"command":{"name":"snapshot"},"sessionId":"s1","targetId":"t1"}"#;
    writeln!(client, "{observation}").expect("send observation");
    client.flush().expect("flush observation");

    let mut response = String::new();
    BufReader::new(client)
        .read_line(&mut response)
        .expect("read gateway response");
    assert!(
        response.contains("\"source\":\"retry\""),
        "gateway should surface the successful replay response: {response}"
    );

    let (first, second) = rx.recv_timeout(IO_TIMEOUT).expect("receive observed requests");
    server.join().expect("join fake upstream");
    assert_eq!(first.trim_end(), observation);
    assert_eq!(second.trim_end(), observation);
}
