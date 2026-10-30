use futures_util::task::AtomicWaker;
use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use std::{
    collections::VecDeque,
    ffi::{c_char, c_int, c_void, CStr, CString},
    fmt,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tokio::sync::watch;

const MAX_PACKET: usize = 2048;
const MAX_QUEUED: usize = 1024;

extern "C" {
    fn phantom_ice_create(
        bind: *const c_char,
        stun: *const c_char,
        port: u16,
        state: extern "C" fn(*mut c_void, c_int, *mut c_void),
        done: extern "C" fn(*mut c_void, *mut c_void),
        recv: extern "C" fn(*mut c_void, *const c_char, usize, *mut c_void),
        context: *mut c_void,
    ) -> *mut c_void;
    fn juice_destroy(agent: *mut c_void);
    fn phantom_ice_quiet();
    fn juice_gather_candidates(agent: *mut c_void) -> c_int;
    fn juice_get_local_description(agent: *mut c_void, buf: *mut c_char, size: usize) -> c_int;
    fn juice_set_remote_description(agent: *mut c_void, sdp: *const c_char) -> c_int;
    fn juice_send(agent: *mut c_void, data: *const c_char, size: usize) -> c_int;
    fn juice_get_selected_addresses(
        agent: *mut c_void,
        local: *mut c_char,
        lsize: usize,
        remote: *mut c_char,
        rsize: usize,
    ) -> c_int;
}

struct Callbacks {
    state: watch::Sender<i32>,
    gathered: watch::Sender<bool>,
    packets: Mutex<VecDeque<Vec<u8>>>,
    waker: AtomicWaker,
    dropped_recv: AtomicU64,
    dropped_send: AtomicU64,
}
extern "C" fn state_cb(_: *mut c_void, state: c_int, context: *mut c_void) {
    // context is owned by Agent until juice_destroy joins the callback thread.
    let c = unsafe { &*(context as *const Callbacks) };
    c.state.send_replace(state);
    c.waker.wake();
}
extern "C" fn done_cb(_: *mut c_void, context: *mut c_void) {
    unsafe { &*(context as *const Callbacks) }
        .gathered
        .send_replace(true);
}
extern "C" fn recv_cb(_: *mut c_void, data: *const c_char, size: usize, context: *mut c_void) {
    let c = unsafe { &*(context as *const Callbacks) };
    if size > MAX_PACKET {
        c.dropped_recv.fetch_add(1, Ordering::Relaxed);
        return;
    }
    // Never block a native callback on an async receiver or grow unbounded.
    if let Ok(mut queue) = c.packets.lock() {
        if queue.len() >= MAX_QUEUED {
            c.dropped_recv.fetch_add(1, Ordering::Relaxed);
            return;
        }
        queue.push_back(unsafe { std::slice::from_raw_parts(data.cast::<u8>(), size) }.to_vec());
    }
    c.waker.wake();
}

pub(crate) struct Agent {
    raw: *mut c_void,
    callbacks: Box<Callbacks>,
}
// libjuice's public API serializes operations; destroy runs only when the last
// Arc is dropped and joins its worker before callbacks are deallocated.
unsafe impl Send for Agent {}
unsafe impl Sync for Agent {}
impl Drop for Agent {
    fn drop(&mut self) {
        unsafe { juice_destroy(self.raw) }
    }
}
impl fmt::Debug for Agent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IceAgent")
    }
}
fn checked(result: c_int) -> io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!("ICE operation failed ({result})")))
    }
}

impl Agent {
    pub fn new(bind: &str, stun: Option<(&str, u16)>) -> io::Result<Arc<Self>> {
        static LOG_INIT: std::sync::Once = std::sync::Once::new();
        LOG_INIT.call_once(|| unsafe { phantom_ice_quiet() });
        let bind = CString::new(bind)?;
        let stun_host = stun.map(|s| CString::new(s.0)).transpose()?;
        let callbacks = Box::new(Callbacks {
            state: watch::channel(0).0,
            gathered: watch::channel(false).0,
            packets: Mutex::new(VecDeque::new()),
            waker: AtomicWaker::new(),
            dropped_recv: AtomicU64::new(0),
            dropped_send: AtomicU64::new(0),
        });
        let raw = unsafe {
            phantom_ice_create(
                bind.as_ptr(),
                stun_host.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
                stun.map_or(0, |s| s.1),
                state_cb,
                done_cb,
                recv_cb,
                (&*callbacks as *const Callbacks).cast_mut().cast(),
            )
        };
        if raw.is_null() {
            return Err(io::Error::other("ICE agent creation failed"));
        }
        Ok(Arc::new(Self { raw, callbacks }))
    }
    pub async fn gather(&self) -> io::Result<String> {
        let mut done = self.callbacks.gathered.subscribe();
        checked(unsafe { juice_gather_candidates(self.raw) })?;
        while !*done.borrow_and_update() {
            done.changed().await.map_err(io::Error::other)?;
        }
        let mut buf = [0 as c_char; 4096];
        checked(unsafe { juice_get_local_description(self.raw, buf.as_mut_ptr(), buf.len()) })?;
        let description = unsafe { CStr::from_ptr(buf.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        if !description.contains("a=candidate:") {
            return Err(io::Error::other("No local ICE candidates were gathered"));
        }
        Ok(description)
    }
    pub async fn connect(&self, sdp: &str) -> io::Result<()> {
        if sdp.len() > 4095 {
            return Err(io::Error::other("ICE description too large"));
        }
        let sdp = CString::new(sdp)?;
        let mut state = self.callbacks.state.subscribe();
        checked(unsafe { juice_set_remote_description(self.raw, sdp.as_ptr()) })?;
        loop {
            match *state.borrow_and_update() {
                3 | 4 => return Ok(()),
                5 => return Err(io::Error::other("ICE traversal failed")),
                _ => {}
            }
            state.changed().await.map_err(io::Error::other)?;
        }
    }
    pub fn selected_addresses(&self) -> io::Result<(String, String)> {
        let mut local = [0 as c_char; 64];
        let mut remote = [0 as c_char; 64];
        checked(unsafe {
            juice_get_selected_addresses(self.raw, local.as_mut_ptr(), 64, remote.as_mut_ptr(), 64)
        })?;
        Ok((
            unsafe { CStr::from_ptr(local.as_ptr()) }
                .to_string_lossy()
                .into_owned(),
            unsafe { CStr::from_ptr(remote.as_ptr()) }
                .to_string_lossy()
                .into_owned(),
        ))
    }
    pub fn dropped(&self) -> (u64, u64) {
        (
            self.callbacks.dropped_recv.load(Ordering::Relaxed),
            self.callbacks.dropped_send.load(Ordering::Relaxed),
        )
    }
}

// One ICE agent owns one remote peer. Quinn uses stable logical addresses here;
// only libjuice sends to actual selected network addresses. They are never OS
// localhost sockets and never imply that an internet route is loopback.
const LOCAL: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 40000);
pub(crate) const REMOTE: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 40001);
impl AsyncUdpSocket for Agent {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(AlwaysWritable)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(LOCAL)
    }
    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        if transmit.destination != REMOTE
            || transmit.contents.len() > MAX_PACKET
            || transmit.segment_size.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Unsupported ICE datagram",
            ));
        }
        let result = unsafe {
            juice_send(
                self.raw,
                transmit.contents.as_ptr().cast(),
                transmit.contents.len(),
            )
        };
        if result == -5 {
            // libjuice exposes no writable notification. Treat UDP EAGAIN as
            // local packet loss (counted); QUIC handles retransmission. Never
            // return WouldBlock with a poller that would spin on the same fd.
            self.callbacks.dropped_send.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        checked(result)
    }
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.callbacks.waker.register(cx.waker());
        let mut packets = self
            .callbacks
            .packets
            .lock()
            .map_err(|_| io::Error::other("ICE queue poisoned"))?;
        if let Some(packet) = packets.pop_front() {
            if bufs.is_empty() || meta.is_empty() || bufs[0].len() < packet.len() {
                return Poll::Ready(Err(io::Error::other("QUIC receive buffer too small")));
            }
            bufs[0][..packet.len()].copy_from_slice(&packet);
            meta[0] = RecvMeta {
                addr: REMOTE,
                len: packet.len(),
                stride: packet.len(),
                ecn: None,
                dst_ip: Some(LOCAL.ip()),
            };
            return Poll::Ready(Ok(1));
        }
        if *self.callbacks.state.borrow() == 5 {
            return Poll::Ready(Err(io::Error::other("ICE route failed")));
        }
        Poll::Pending
    }
    fn may_fragment(&self) -> bool {
        true
    }
}
#[derive(Debug)]
struct AlwaysWritable;
impl UdpPoller for AlwaysWritable {
    fn poll_writable(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
