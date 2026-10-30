//! Chromium native-messaging host. No HTTP listener, shell, filesystem API or
//! page-selected executable. Each browser document owns its own helper process.
use phantom_tunnel_native::{probe, Offer, Options, PreparedTunnel, Tunnel};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Read, Write},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};
const MAX_FRAME: usize = 16384;
const MAX_BYTES: usize = 32 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(
    tag = "action",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum Request {
    Prepare {
        id: String,
        job_id: String,
        local_peer: String,
        remote_peer: String,
        session_id: String,
        bytes: usize,
        initiator: bool,
    },
    Connect {
        id: String,
        job_id: String,
        offer: Offer,
    },
    Close {
        id: String,
        job_id: String,
    },
}
impl Request {
    fn ids(&self) -> (&str, &str) {
        match self {
            Self::Prepare { id, job_id, .. }
            | Self::Connect { id, job_id, .. }
            | Self::Close { id, job_id } => (id, job_id),
        }
    }
}
enum Event {
    Request(Value),
    End,
    Prepared(String, Result<Box<PreparedTunnel>, io::Error>),
    Connected(String, Result<Box<Tunnel>, io::Error>),
    Finished(String, Result<probe::ProbeReceipt, io::Error>),
    Expired(String),
}
struct Job {
    request_id: String,
    bytes: usize,
    initiator: bool,
    prepared: Option<Box<PreparedTunnel>>,
    task: JoinHandle<()>,
    deadline: JoinHandle<()>,
    phase: &'static str,
}
impl Drop for Job {
    fn drop(&mut self) {
        self.task.abort();
        self.deadline.abort();
    }
}
fn hex_id(s: &str, n: usize) -> bool {
    s.len() == n
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn response(id: &str, body: Value) -> Value {
    let mut value = body;
    value["id"] = json!(id);
    value
}
async fn send(out: &mpsc::Sender<Value>, value: Value) {
    // Stop this ephemeral host on output pressure instead of stalling the
    // input loop and losing EOF/cancellation. No persistent user data is owned.
    if out.try_send(value).is_err() {
        std::process::exit(3);
    }
}
fn remember(id: String, recent: &mut HashSet<String>, order: &mut VecDeque<String>) {
    recent.insert(id.clone());
    order.push_back(id);
    if order.len() > 256 {
        if let Some(old) = order.pop_front() {
            recent.remove(&old);
        }
    }
}
fn reader(tx: mpsc::Sender<Event>) {
    let input = io::stdin();
    let mut input = input.lock();
    loop {
        let mut head = [0u8; 4];
        if input.read_exact(&mut head).is_err() {
            break;
        }
        let size = u32::from_ne_bytes(head) as usize;
        if size == 0 || size > MAX_FRAME {
            break;
        }
        let mut bytes = vec![0u8; size];
        if input.read_exact(&mut bytes).is_err() {
            break;
        }
        let value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => break,
        };
        if tx.blocking_send(Event::Request(value)).is_err() {
            return;
        }
    }
    let _ = tx.blocking_send(Event::End);
}
fn writer(mut rx: mpsc::Receiver<Value>) {
    let output = io::stdout();
    let mut output = output.lock();
    while let Some(value) = rx.blocking_recv() {
        let bytes = match serde_json::to_vec(&value) {
            Ok(b) if b.len() <= 32768 => b,
            _ => break,
        };
        if output
            .write_all(&(bytes.len() as u32).to_ne_bytes())
            .and_then(|_| output.write_all(&bytes))
            .and_then(|_| output.flush())
            .is_err()
        {
            break;
        }
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let identity: Value =
        serde_json::from_str(include_str!("../../../browser-extension/identity.json"))
            .expect("Built-in helper identity");
    let expected = format!(
        "chrome-extension://{}/",
        identity["extensionId"].as_str().unwrap()
    );
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().map(|s| s.trim_end_matches('/')) != Some(expected.trim_end_matches('/'))
        || args.iter().skip(1).any(|s| {
            !s.strip_prefix("--parent-window=")
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|c| c.is_ascii_digit()))
        })
    {
        std::process::exit(2);
    }
    // Browser requests cannot alter endpoint policy. Overrides are trusted local
    // process environment, used by installers/test hosts, never protocol fields.
    let bind = std::env::var("PHANTOM_HELPER_BIND").unwrap_or_else(|_| "0.0.0.0".into());
    let stun = match std::env::var("PHANTOM_HELPER_STUN").as_deref() {
        Ok("none") => None,
        Ok(v) => v
            .rsplit_once(':')
            .and_then(|(h, p)| p.parse::<u16>().ok().map(|p| (h.to_owned(), p))),
        Err(_) => Some(("stun.l.google.com".into(), 19302)),
    };
    let (tx, mut rx) = mpsc::channel(16);
    let (out, outputs) = mpsc::channel(32);
    let input_tx = tx.clone();
    std::thread::spawn(move || reader(input_tx));
    std::thread::spawn(move || writer(outputs));
    let mut jobs: HashMap<String, Job> = HashMap::new();
    let mut recent = HashSet::new();
    let mut order = VecDeque::new();
    loop {
        let event = tokio::select! {event=rx.recv()=>event,_=out.closed()=>None};
        match event {
            None | Some(Event::End) => break,
            Some(Event::Request(value)) => {
                let request = match serde_json::from_value::<Request>(value.clone()) {
                    Ok(r) => r,
                    Err(_) => {
                        if let Some(id) = value
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|id| hex_id(id, 32))
                        {
                            send(
                                &out,
                                response(id, json!({"ok":false,"error":"invalid_request"})),
                            )
                            .await;
                        }
                        continue;
                    }
                };
                let (id, job_id) = request.ids();
                if !hex_id(id, 32) || !hex_id(job_id, 32) {
                    continue;
                }
                match request {
                    Request::Prepare {
                        id,
                        job_id,
                        local_peer,
                        remote_peer,
                        session_id,
                        bytes,
                        initiator,
                    } => {
                        if !hex_id(&local_peer, 16)
                            || !hex_id(&remote_peer, 16)
                            || local_peer == remote_peer
                            || !hex_id(&session_id, 32)
                            || bytes == 0
                            || bytes > MAX_BYTES
                        {
                            send(
                                &out,
                                response(&id, json!({"ok":false,"error":"invalid_scope"})),
                            )
                            .await;
                            continue;
                        }
                        if jobs.len() >= 4 || jobs.contains_key(&job_id) || recent.contains(&job_id)
                        {
                            send(&out, response(&id, json!({"ok":false,"error":"busy"}))).await;
                            continue;
                        }
                        let send_tx = tx.clone();
                        let key = job_id.clone();
                        let bind = bind.clone();
                        let stun = stun.clone();
                        let task = tokio::spawn(async move {
                            let result = PreparedTunnel::gather(Options {
                                local_peer,
                                remote_peer,
                                session_id,
                                bind_address: bind,
                                stun,
                                deadline: Duration::from_secs(20),
                            })
                            .await
                            .map(Box::new);
                            let _ = send_tx.send(Event::Prepared(key, result)).await;
                        });
                        let key = job_id.clone();
                        let deadline_tx = tx.clone();
                        let deadline = tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            let _ = deadline_tx.send(Event::Expired(key)).await;
                        });
                        jobs.insert(
                            job_id,
                            Job {
                                request_id: id,
                                bytes,
                                initiator,
                                prepared: None,
                                task,
                                deadline,
                                phase: "preparing",
                            },
                        );
                    }
                    Request::Connect { id, job_id, offer } => {
                        let Some(job) = jobs.get_mut(&job_id) else {
                            send(
                                &out,
                                response(&id, json!({"ok":false,"error":"unavailable"})),
                            )
                            .await;
                            continue;
                        };
                        let Some(prepared) = job.prepared.take() else {
                            send(&out, response(&id, json!({"ok":false,"error":"busy"}))).await;
                            continue;
                        };
                        job.phase = "connecting";
                        job.request_id = id;
                        let initiator = job.initiator;
                        let send_tx = tx.clone();
                        job.task = tokio::spawn(async move {
                            let result = prepared.establish(offer, initiator).await.map(Box::new);
                            let _ = send_tx.send(Event::Connected(job_id, result)).await;
                        });
                    }
                    Request::Close { id, job_id } => {
                        if let Some(job) = jobs.remove(&job_id) {
                            if job.phase != "running" && job.phase != "prepared" {
                                send(
                                    &out,
                                    response(
                                        &job.request_id,
                                        json!({"ok":false,"error":"cancelled"}),
                                    ),
                                )
                                .await;
                            }
                            drop(job);
                            remember(job_id.clone(), &mut recent, &mut order);
                        }
                        send(&out, response(&id, json!({"ok":true,"jobId":job_id}))).await;
                    }
                }
            }
            Some(Event::Prepared(key, result)) => {
                if let Some(job) = jobs.get_mut(&key) {
                    if job.phase != "preparing" {
                        continue;
                    }
                    match result {
                        Ok(prepared) => {
                            let offer = prepared.offer();
                            job.prepared = Some(prepared);
                            job.phase = "prepared";
                            send(
                                &out,
                                response(
                                    &job.request_id,
                                    json!({"ok":true,"jobId":key,"offer":offer}),
                                ),
                            )
                            .await;
                        }
                        Err(_) => {
                            let id = job.request_id.clone();
                            jobs.remove(&key);
                            remember(key, &mut recent, &mut order);
                            send(
                                &out,
                                response(&id, json!({"ok":false,"error":"native_failed"})),
                            )
                            .await;
                        }
                    }
                }
            }
            Some(Event::Connected(key, result)) => {
                if let Some(job) = jobs.get_mut(&key) {
                    if job.phase != "connecting" {
                        continue;
                    }
                    match result {
                        Ok(tunnel) => {
                            job.deadline.abort();
                            job.phase = "running";
                            let (local, remote) = tunnel.selected_addresses().unwrap_or_default();
                            send(&out,response(&job.request_id,json!({"ok":true,"jobId":key,"connected":true,"selectedLocal":local,"selectedRemote":remote}))).await;
                            let event_tx = tx.clone();
                            let bytes = job.bytes;
                            let initiator = job.initiator;
                            job.task = tokio::spawn(async move {
                                let result = probe::run(*tunnel, bytes, initiator).await;
                                let _ = event_tx.send(Event::Finished(key, result)).await;
                            });
                        }
                        Err(_) => {
                            let id = job.request_id.clone();
                            jobs.remove(&key);
                            remember(key, &mut recent, &mut order);
                            send(
                                &out,
                                response(&id, json!({"ok":false,"error":"native_failed"})),
                            )
                            .await;
                        }
                    }
                }
            }
            Some(Event::Finished(key, result)) => {
                if jobs.remove(&key).is_some() {
                    remember(key.clone(), &mut recent, &mut order);
                    send(
                        &out,
                        match result {
                            Ok(receipt) => json!({"event":"complete","jobId":key,"result":receipt}),
                            Err(_) => json!({"event":"failed","jobId":key,"error":"native_failed"}),
                        },
                    )
                    .await;
                }
            }
            Some(Event::Expired(key)) => {
                if let Some(job) = jobs.remove(&key) {
                    if job.phase != "prepared" {
                        send(
                            &out,
                            response(&job.request_id, json!({"ok":false,"error":"timeout"})),
                        )
                        .await;
                    }
                    send(
                        &out,
                        json!({"event":"failed","jobId":key,"error":"timeout"}),
                    )
                    .await;
                    remember(key, &mut recent, &mut order);
                }
            }
        }
    }
    jobs.clear(); // EOF/disconnect cancels every native owner.
}
