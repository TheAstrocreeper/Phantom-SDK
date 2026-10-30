use phantom_tunnel_native::{Options, PreparedTunnel, Tunnel};
use sha2::{Digest, Sha256};
use std::{io, time::Duration};

fn options(local: &str, remote: &str) -> Options {
    Options {
        local_peer: local.into(),
        remote_peer: remote.into(),
        session_id: "0123456789abcdef0123456789abcdef".into(),
        bind_address: "127.0.0.1".into(),
        stun: None,
        deadline: Duration::from_secs(5),
    }
}
async fn prepared() -> (PreparedTunnel, PreparedTunnel) {
    let (a, b) = tokio::join!(
        PreparedTunnel::gather(options("alice", "bob")),
        PreparedTunnel::gather(options("bob", "alice"))
    );
    (a.expect("gather alice"), b.expect("gather bob"))
}
async fn pair() -> (Tunnel, Tunnel) {
    let (a, b) = prepared().await;
    let ao = a.offer();
    let bo = b.offer();
    let (a, b) = tokio::join!(a.establish(bo, true), b.establish(ao, false));
    (a.expect("connect alice"), b.expect("connect bob"))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_ice_quic_stream_hash_reverse_receipt_and_datagram() -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (a, b) = pair().await;
        let count = 16 * 1024 * 1024;
        let block = vec![0xa7u8; 65536];
        let sender = async {
            let (mut send, mut recv) = a.connection.open_bi().await?;
            let mut hash = Sha256::new();
            for _ in 0..count / block.len() {
                send.write_all(&block).await?;
                hash.update(&block);
            }
            send.finish()?;
            let receipt = recv.read_to_end(256).await?;
            assert_eq!(receipt, hex::encode(hash.finalize()).as_bytes());
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let receiver = async {
            let (mut send, mut recv) = b.connection.accept_bi().await?;
            let mut received = 0;
            let mut hash = Sha256::new();
            let mut buf = vec![0u8; 32768];
            while let Some(size) = recv.read(&mut buf).await? {
                received += size;
                hash.update(&buf[..size]);
                tokio::task::yield_now().await;
            }
            assert_eq!(received, count);
            send.write_all(hex::encode(hash.finalize()).as_bytes())
                .await?;
            send.finish()?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let (sent, received) = tokio::join!(sender, receiver);
        sent.unwrap();
        received.unwrap();
        a.connection
            .send_datagram(b"native-datagram".as_slice().into())
            .unwrap();
        assert_eq!(
            &b.connection.read_datagram().await.unwrap()[..],
            b"native-datagram"
        );
        // Multiple consumers can open streams on the same carrier.
        let (mut tx, mut rx) = b.connection.open_bi().await.unwrap();
        tx.write_all(b"reverse").await.unwrap();
        tx.finish().unwrap();
        let (mut ack, mut incoming) = a.connection.accept_bi().await.unwrap();
        assert_eq!(incoming.read_to_end(32).await.unwrap(), b"reverse");
        ack.write_all(b"ok").await.unwrap();
        ack.finish().unwrap();
        assert_eq!(rx.read_to_end(32).await.unwrap(), b"ok");
        assert!(!a.selected_addresses().unwrap().0.is_empty());
        a.close();
        assert!(b.connection.closed().await.to_string().contains("closed"));
    })
    .await
    .map_err(io::Error::other)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_peer_and_session_confusion_before_connecting() {
    let (a, b) = prepared().await;
    let mut remote = b.offer();
    remote.from = "mallory".into();
    assert!(a
        .establish(remote, true)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("authenticated peer/session"));
    let (a, b) = prepared().await;
    let mut remote = b.offer();
    remote.session_id = "00".repeat(16);
    assert!(a.establish(remote, true).await.is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_untrusted_ephemeral_certificate() {
    let (a, b) = prepared().await;
    let c = PreparedTunnel::gather(options("mallory", "victim"))
        .await
        .unwrap();
    let ao = a.offer();
    let mut bo = b.offer();
    bo.certificate_der = c.offer().certificate_der;
    let (a, b) = tokio::join!(a.establish(bo, true), b.establish(ao, false));
    assert!(a.is_err());
    assert!(b.is_err());
    let reason = a.err().unwrap().to_string();
    assert!(
        !reason.contains("deadline"),
        "Certificate test must reach TLS, not just time out: {reason}"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_peer_has_a_bounded_deadline() {
    let mut opts = options("alice", "bob");
    opts.deadline = Duration::from_millis(150);
    let a = PreparedTunnel::gather(opts).await.unwrap();
    let b = PreparedTunnel::gather(options("bob", "alice"))
        .await
        .unwrap();
    // The remote never receives the local description, so cannot authenticate
    // connectivity checks or complete a QUIC session.
    let start = std::time::Instant::now();
    let result = a.establish(b.offer(), true).await;
    assert!(result.is_err());
    assert!(start.elapsed() < Duration::from_secs(2));
}
#[tokio::test]
async fn invalid_scope_and_unbounded_deadlines_are_rejected() {
    let mut opts = options("alice", "alice");
    assert!(PreparedTunnel::gather(opts).await.is_err());
    opts = options("alice", "bob");
    opts.deadline = Duration::from_secs(121);
    assert!(PreparedTunnel::gather(opts).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_source_rejects_a_wrong_peer_hash() {
    let (a, b) = pair().await;
    let source = phantom_tunnel_native::probe::run(a, 1024, true);
    let rogue = async {
        let (mut tx, mut rx) = b.connection.accept_bi().await.unwrap();
        let mut header = [0u8; 16];
        rx.read_exact(&mut header).await.unwrap();
        assert_eq!(&header, b"PHANTOM-PROBE/2\n");
        let mut body = [0u8; 1024];
        rx.read_exact(&mut body).await.unwrap();
        tx.write_all(&[b'0'; 64]).await.unwrap();
        tx.finish().unwrap();
    };
    let (result, _) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(source, rogue)
    })
    .await
    .unwrap();
    assert!(result
        .err()
        .unwrap()
        .to_string()
        .contains("receipt mismatch"));
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_receiver_requires_explicit_peer_verification_before_success() {
    let (a, b) = pair().await;
    let sink = phantom_tunnel_native::probe::run(b, 1024, false);
    let rogue = async {
        let (mut tx, mut rx) = a.connection.open_bi().await.unwrap();
        tx.write_all(b"PHANTOM-PROBE/2\n").await.unwrap();
        tx.write_all(&[0xa7; 1024]).await.unwrap();
        let mut receipt = [0u8; 64];
        rx.read_exact(&mut receipt).await.unwrap();
        tx.write_all(b"not_verified").await.unwrap();
        tx.finish().unwrap();
    };
    let (result, _) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(sink, rogue) })
            .await
            .unwrap();
    assert!(result
        .err()
        .unwrap()
        .to_string()
        .contains("acknowledgement missing"));
    a.shutdown().await;
}
