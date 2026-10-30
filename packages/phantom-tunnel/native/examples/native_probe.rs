//! Probe process: JSON offers on stdin/stdout stand in for the future WebRTC
//! control bridge. Payload bytes use native ICE/QUIC sockets, never these pipes.
use phantom_tunnel_native::{Offer, Options, PreparedTunnel};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    io::{self, BufRead, Read, Write},
    time::{Duration, Instant},
};
fn output(value: serde_json::Value) -> io::Result<()> {
    println!("{value}");
    io::stdout().flush()
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        return Err("usage: native_probe LOCAL REMOTE SESSION_HEX connect|accept BYTES [BIND_IP] [STUN_HOST:PORT]".into());
    }
    let initiator = match args[4].as_str() {
        "connect" => true,
        "accept" => false,
        _ => return Err("Invalid mode".into()),
    };
    let bytes: usize = args[5].parse()?;
    if bytes == 0 || bytes > 2 * 1024 * 1024 * 1024usize {
        return Err("Probe size must be 1 byte to 2 GiB".into());
    }
    let stun = if let Some(arg) = args.get(7) {
        let (host, port) = arg.rsplit_once(':').ok_or("Invalid STUN endpoint")?;
        Some((host.into(), port.parse()?))
    } else {
        None
    };
    let local = PreparedTunnel::gather(Options {
        local_peer: args[1].clone(),
        remote_peer: args[2].clone(),
        session_id: args[3].clone(),
        bind_address: args.get(6).cloned().unwrap_or_else(|| "0.0.0.0".into()),
        stun,
        deadline: Duration::from_secs(20),
    })
    .await?;
    output(json!({"type":"offer","offer":local.offer()}))?;
    // Read only a bounded control envelope. The trusted controller must ensure
    // this offer came from the named authenticated peer before forwarding it.
    let mut line = String::new();
    io::stdin().lock().take(16385).read_line(&mut line)?;
    if line.len() > 16384 {
        return Err("Control envelope too large".into());
    }
    let remote: Offer = serde_json::from_str(&line)?;
    let tunnel = local.establish(remote, initiator).await?;
    let (selected_local, selected_remote) = tunnel.selected_addresses()?;
    output(
        json!({"type":"connected","carrier":"ice-quic","selectedLocal":selected_local,"selectedRemote":selected_remote}),
    )?;
    let start = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        if initiator {
            let (mut send, mut recv) = tunnel.connection.open_bi().await?;
            let block = vec![0xa7u8; 65536];
            let mut hash = Sha256::new();
            let mut sent = 0;
            while sent < bytes {
                let n = (bytes - sent).min(block.len());
                send.write_all(&block[..n]).await?;
                hash.update(&block[..n]);
                sent += n;
            }
            send.finish()?;
            let receipt: serde_json::Value =
                serde_json::from_slice(&recv.read_to_end(1024).await?)?;
            let digest = hex::encode(hash.finalize());
            if receipt["bytes"].as_u64() != Some(bytes as u64)
                || receipt["sha256"].as_str() != Some(&digest)
            {
                return Err("Receipt mismatch".into());
            }
            Ok::<_, Box<dyn std::error::Error>>(digest)
        } else {
            let (mut send, mut recv) = tunnel.connection.accept_bi().await?;
            let mut buf = vec![0u8; 65536];
            let mut hash = Sha256::new();
            let mut received = 0;
            while let Some(size) = recv.read(&mut buf).await? {
                received += size;
                if received > bytes {
                    return Err("Probe exceeds agreed size".into());
                }
                hash.update(&buf[..size]);
            }
            if received != bytes {
                return Err("Truncated probe".into());
            }
            let digest = hex::encode(hash.finalize());
            send.write_all(&serde_json::to_vec(
                &json!({"bytes":received,"sha256":digest}),
            )?)
            .await?;
            send.finish()?;
            // Keep the endpoint alive until the receipt is acknowledged.
            send.stopped().await?;
            Ok(digest)
        }
    })
    .await??;
    let elapsed = start.elapsed().as_secs_f64();
    let (recv_drops, send_drops) = tunnel.local_packet_drops();
    output(
        json!({"type":"complete","bytes":bytes,"sha256":result,"elapsedSeconds":elapsed,"MBps":bytes as f64/elapsed/1e6,"localReceiveDrops":recv_drops,"localSendDrops":send_drops}),
    )?;
    tunnel.close();
    Ok(())
}
