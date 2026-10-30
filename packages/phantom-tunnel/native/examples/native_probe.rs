//! Probe process: JSON offers on stdin/stdout stand in for the future WebRTC
//! control bridge. Payload bytes use native ICE/QUIC sockets, never these pipes.
use phantom_tunnel_native::{probe, Offer, Options, PreparedTunnel};
use serde_json::json;
use std::{
    io::{self, BufRead, Read, Write},
    time::Duration,
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
    let receipt = probe::run(tunnel, bytes, initiator).await?;
    let mut value = serde_json::to_value(receipt)?;
    value["type"] = json!("complete");
    output(value)?;
    Ok(())
}
