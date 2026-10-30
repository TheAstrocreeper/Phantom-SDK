//! Blocking C ABI. Call from platform worker threads, never a UI thread or
//! a Tokio worker. Handles must not be freed concurrently with any call.
use crate::{Offer, Options, PreparedTunnel, Tunnel};
use serde::Deserialize;
use std::{
    ffi::c_int,
    io, ptr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{runtime::Runtime, sync::watch};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Config {
    local_peer: String,
    remote_peer: String,
    session_id: String,
    bind_address: String,
    stun_host: Option<String>,
    stun_port: Option<u16>,
    deadline_ms: u64,
}
enum State {
    Prepared(Box<PreparedTunnel>),
    Connecting,
    Probing,
    Connected(Tunnel),
    Closed,
}
struct Owner {
    runtime: Runtime,
    state: Mutex<State>,
    cancel: watch::Sender<bool>,
}
pub struct Session {
    owner: Arc<Owner>,
    offer: Vec<u8>,
}
pub struct Stream {
    owner: Arc<Owner>,
    send: Mutex<quinn::SendStream>,
    recv: Mutex<quinn::RecvStream>,
}
fn error(message: &str) -> io::Error {
    io::Error::other(message)
}
fn lock<T>(m: &Mutex<T>) -> io::Result<std::sync::MutexGuard<'_, T>> {
    m.lock().map_err(|_| error("Poisoned handle"))
}
fn invoke(f: impl FnOnce() -> io::Result<()>) -> c_int {
    // No panic may unwind across a foreign boundary. Error codes deliberately
    // omit offer/ICE contents; callers can report the failed operation.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(())) => 0,
        Ok(Err(_)) => -1,
        Err(_) => -2,
    }
}
fn duration(ms: u64) -> io::Result<Duration> {
    if ms == 0 || ms > 120000 {
        Err(error("Invalid deadline"))
    } else {
        Ok(Duration::from_millis(ms))
    }
}
unsafe fn input<'a>(data: *const u8, len: usize) -> io::Result<&'a [u8]> {
    if data.is_null() || len == 0 || len > 16384 {
        return Err(error("Invalid input buffer"));
    }
    Ok(std::slice::from_raw_parts(data, len))
}
unsafe fn output(
    data: &[u8],
    buf: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> io::Result<()> {
    if written.is_null() {
        return Err(error("Missing length output"));
    }
    *written = data.len();
    if capacity < data.len() {
        return Err(error("Output buffer too small"));
    }
    if !data.is_empty() {
        if buf.is_null() {
            return Err(error("Missing output buffer"));
        }
        ptr::copy_nonoverlapping(data.as_ptr(), buf, data.len());
    }
    Ok(())
}
unsafe fn session<'a>(p: *const Session) -> io::Result<&'a Session> {
    p.as_ref().ok_or_else(|| error("Missing session"))
}
unsafe fn stream<'a>(p: *const Stream) -> io::Result<&'a Stream> {
    p.as_ref().ok_or_else(|| error("Missing stream"))
}
fn connection(owner: &Owner) -> io::Result<quinn::Connection> {
    match &*lock(&owner.state)? {
        State::Connected(t) => Ok(t.connection.clone()),
        _ => Err(error("Session is not connected")),
    }
}
fn close(owner: &Owner) {
    owner.cancel.send_replace(true);
    if let Ok(mut state) = owner.state.lock() {
        if let State::Connected(t) = &*state {
            t.close();
        }
        *state = State::Closed;
    }
}

#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_prepare(
    config: *const u8,
    len: usize,
    result: *mut *mut Session,
) -> c_int {
    invoke(|| {
        if result.is_null() {
            return Err(error("Missing session output"));
        }
        *result = ptr::null_mut();
        let config: Config =
            serde_json::from_slice(input(config, len)?).map_err(io::Error::other)?;
        let deadline = duration(config.deadline_ms)?;
        let stun = match (config.stun_host, config.stun_port) {
            (None, None) => None,
            (Some(h), Some(p)) if !h.is_empty() && p > 0 => Some((h, p)),
            _ => return Err(error("Invalid STUN config")),
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let prepared = runtime.block_on(PreparedTunnel::gather(Options {
            local_peer: config.local_peer,
            remote_peer: config.remote_peer,
            session_id: config.session_id,
            bind_address: config.bind_address,
            stun,
            deadline,
        }))?;
        let offer = serde_json::to_vec(&prepared.offer()).map_err(io::Error::other)?;
        *result = Box::into_raw(Box::new(Session {
            owner: Arc::new(Owner {
                runtime,
                state: Mutex::new(State::Prepared(Box::new(prepared))),
                cancel: watch::channel(false).0,
            }),
            offer,
        }));
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_offer(
    handle: *const Session,
    buf: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> c_int {
    invoke(|| output(&session(handle)?.offer, buf, capacity, written))
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_connect(
    handle: *const Session,
    remote: *const u8,
    len: usize,
    initiator: c_int,
) -> c_int {
    invoke(|| {
        if initiator != 0 && initiator != 1 {
            return Err(error("Invalid role"));
        }
        let owner = &session(handle)?.owner;
        let remote: Offer =
            serde_json::from_slice(input(remote, len)?).map_err(io::Error::other)?;
        let prepared = {
            let mut state = lock(&owner.state)?;
            match std::mem::replace(&mut *state, State::Connecting) {
                State::Prepared(p) => p,
                other => {
                    *state = other;
                    return Err(error("Session already consumed"));
                }
            }
        };
        let mut cancel = owner.cancel.subscribe();
        let result=owner.runtime.block_on(async {
            if *cancel.borrow(){return Err(error("Cancelled"));}
            tokio::select!{result=prepared.establish(remote,initiator==1)=>result,_=cancel.changed()=>Err(error("Cancelled"))}
        });
        let mut state = lock(&owner.state)?;
        if matches!(*state, State::Closed) {
            drop(result);
            return Err(error("Cancelled"));
        }
        match result {
            Ok(tunnel) => {
                *state = State::Connected(tunnel);
                Ok(())
            }
            Err(e) => {
                *state = State::Closed;
                Err(e)
            }
        }
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_open_stream(
    handle: *const Session,
    accept: c_int,
    timeout_ms: u64,
    result: *mut *mut Stream,
) -> c_int {
    invoke(|| {
        if result.is_null() || !(0..=1).contains(&accept) {
            return Err(error("Invalid stream output/role"));
        }
        *result = ptr::null_mut();
        let deadline = duration(timeout_ms)?;
        let owner = &session(handle)?.owner;
        let connection = connection(owner)?;
        let (send, recv) = owner.runtime.block_on(async {
            tokio::time::timeout(deadline, async {
                if accept == 1 {
                    connection.accept_bi().await
                } else {
                    connection.open_bi().await
                }
            })
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)
        })?;
        *result = Box::into_raw(Box::new(Stream {
            owner: owner.clone(),
            send: Mutex::new(send),
            recv: Mutex::new(recv),
        }));
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_stream_write(
    handle: *const Stream,
    data: *const u8,
    len: usize,
    timeout_ms: u64,
) -> c_int {
    invoke(|| {
        if data.is_null() || len == 0 || len > 1024 * 1024 {
            return Err(error("Invalid stream write"));
        }
        let bytes = std::slice::from_raw_parts(data, len);
        let deadline = duration(timeout_ms)?;
        let stream = stream(handle)?;
        let mut send = lock(&stream.send)?;
        let result = stream.owner.runtime.block_on(async {
            tokio::time::timeout(deadline, send.write_all(bytes))
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)
        });
        // A timeout may follow a partial write. Reset rather than let a caller
        // retry the full buffer and silently duplicate bytes.
        if result.is_err() {
            let _ = send.reset(1u32.into());
        }
        result
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_stream_read(
    handle: *const Stream,
    buf: *mut u8,
    capacity: usize,
    written: *mut usize,
    timeout_ms: u64,
) -> c_int {
    invoke(|| {
        if buf.is_null() || written.is_null() || capacity == 0 || capacity > 1024 * 1024 {
            return Err(error("Invalid stream read"));
        }
        *written = 0;
        let deadline = duration(timeout_ms)?;
        let stream = stream(handle)?;
        let mut recv = lock(&stream.recv)?;
        let output = std::slice::from_raw_parts_mut(buf, capacity);
        *written = stream
            .owner
            .runtime
            .block_on(async {
                tokio::time::timeout(deadline, recv.read(output))
                    .await
                    .map_err(io::Error::other)?
                    .map_err(io::Error::other)
            })?
            .unwrap_or(0);
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_stream_finish(handle: *const Stream) -> c_int {
    invoke(|| {
        lock(&stream(handle)?.send)?
            .finish()
            .map_err(io::Error::other)
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_send_datagram(
    handle: *const Session,
    data: *const u8,
    len: usize,
) -> c_int {
    invoke(|| {
        if data.is_null() || len == 0 || len > 1100 {
            return Err(error("Invalid datagram"));
        }
        connection(&session(handle)?.owner)?
            .send_datagram(std::slice::from_raw_parts(data, len).to_vec().into())
            .map_err(io::Error::other)
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_recv_datagram(
    handle: *const Session,
    buf: *mut u8,
    capacity: usize,
    written: *mut usize,
    timeout_ms: u64,
) -> c_int {
    invoke(|| {
        if written.is_null() || buf.is_null() || capacity < 1100 {
            return Err(error("Datagram buffer must hold 1100 bytes"));
        }
        *written = 0;
        let deadline = duration(timeout_ms)?;
        let owner = &session(handle)?.owner;
        let connection = connection(owner)?;
        let data = owner.runtime.block_on(async {
            tokio::time::timeout(deadline, connection.read_datagram())
                .await
                .map_err(io::Error::other)?
                .map_err(io::Error::other)
        })?;
        output(&data, buf, capacity, written)
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_status(
    handle: *const Session,
    buf: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> c_int {
    invoke(|| {
        let state = lock(&session(handle)?.owner.state)?;
        let value = match &*state {
            State::Connected(t) => {
                let (local, remote) = t.selected_addresses()?;
                let (recv, send) = t.local_packet_drops();
                serde_json::json!({"state":"connected","carrier":"ice-quic","selectedLocal":local,"selectedRemote":remote,"localReceiveDrops":recv,"localSendDrops":send})
            }
            State::Prepared(_) => serde_json::json!({"state":"prepared"}),
            State::Connecting => serde_json::json!({"state":"connecting"}),
            State::Probing => serde_json::json!({"state":"probing"}),
            State::Closed => serde_json::json!({"state":"closed"}),
        };
        output(
            &serde_json::to_vec(&value).map_err(io::Error::other)?,
            buf,
            capacity,
            written,
        )
    })
}
#[no_mangle]
pub extern "C" fn phantom_tunnel_probe_version() -> u32 {
    crate::probe::VERSION
}

#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_run_probe(
    handle: *const Session,
    bytes: usize,
    initiator: c_int,
    buf: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> c_int {
    invoke(|| {
        if buf.is_null()
            || written.is_null()
            || capacity < 2048
            || bytes == 0
            || bytes > 32 * 1024 * 1024
            || !(0..=1).contains(&initiator)
        {
            return Err(error("Invalid probe output/scope"));
        }
        *written = 0;
        let owner = &session(handle)?.owner;
        let tunnel = {
            let mut state = lock(&owner.state)?;
            match std::mem::replace(&mut *state, State::Probing) {
                State::Connected(t) => t,
                other => {
                    *state = other;
                    return Err(error("Probe requires a connected session"));
                }
            }
        };
        let mut cancel = owner.cancel.subscribe();
        let result=owner.runtime.block_on(async {
            if *cancel.borrow(){return Err(error("Cancelled"));}
            tokio::select!{result=crate::probe::run(tunnel,bytes,initiator==1)=>result,_=cancel.changed()=>Err(error("Cancelled"))}
        });
        *lock(&owner.state)? = State::Closed;
        output(
            &serde_json::to_vec(&result?).map_err(io::Error::other)?,
            buf,
            capacity,
            written,
        )
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_close(handle: *const Session) -> c_int {
    invoke(|| {
        close(&session(handle)?.owner);
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_session_free(handle: *mut Session) {
    let _ = invoke(|| {
        if !handle.is_null() {
            let session = Box::from_raw(handle);
            close(&session.owner);
        }
        Ok(())
    });
}
#[no_mangle]
pub unsafe extern "C" fn phantom_tunnel_stream_free(handle: *mut Stream) {
    let _ = invoke(|| {
        if !handle.is_null() {
            let stream = Box::from_raw(handle);
            if let Ok(mut recv) = stream.recv.lock() {
                let _ = recv.stop(0u32.into());
            }
            if let Ok(mut send) = stream.send.lock() {
                let _ = send.reset(0u32.into());
            };
        }
        Ok(())
    });
}
