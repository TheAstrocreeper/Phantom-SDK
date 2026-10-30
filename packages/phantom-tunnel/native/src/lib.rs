//! Experimental native bulk carrier: ICE/libjuice + QUIC/Quinn.
//! Offers must be exchanged over an authenticated confidential Phantom channel.
mod ffi;
mod ice;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use std::{io, sync::Arc, time::Duration};

pub const PROTOCOL: &str = "phantom-bulk-quic-v1";
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Offer {
    pub protocol: String,
    pub session_id: String,
    pub from: String,
    pub to: String,
    pub certificate_der: String,
    pub ice_sdp: String,
}
// Deliberately no Debug on Offer: ICE credentials belong off diagnostic logs.
pub struct Options {
    pub local_peer: String,
    pub remote_peer: String,
    pub session_id: String,
    pub bind_address: String,
    pub stun: Option<(String, u16)>,
    pub deadline: Duration,
}
pub struct PreparedTunnel {
    options: Options,
    ice: Arc<ice::Agent>,
    offer: Offer,
    key: Vec<u8>,
    cert: CertificateDer<'static>,
}
pub struct Tunnel {
    pub connection: quinn::Connection,
    endpoint: quinn::Endpoint,
    ice: Arc<ice::Agent>,
}
impl Tunnel {
    pub fn selected_addresses(&self) -> io::Result<(String, String)> {
        self.ice.selected_addresses()
    }
    pub fn local_packet_drops(&self) -> (u64, u64) {
        self.ice.dropped()
    }
    pub fn close(&self) {
        self.connection.close(0u32.into(), b"closed");
        self.endpoint.close(0u32.into(), b"closed");
    }
}
impl Drop for Tunnel {
    fn drop(&mut self) {
        self.close();
    }
}
fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}
fn peer(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b":_-".contains(&c))
}
impl PreparedTunnel {
    pub async fn gather(options: Options) -> io::Result<Self> {
        if !peer(&options.local_peer)
            || !peer(&options.remote_peer)
            || options.local_peer == options.remote_peer
            || options.session_id.len() != 32
            || !options.session_id.bytes().all(|c| c.is_ascii_hexdigit())
            || options.deadline.is_zero()
            || options.deadline > Duration::from_secs(120)
        {
            return Err(err("Invalid tunnel scope or deadline"));
        }
        let ice = ice::Agent::new(
            &options.bind_address,
            options.stun.as_ref().map(|s| (s.0.as_str(), s.1)),
        )?;
        let sdp = tokio::time::timeout(options.deadline, ice.gather())
            .await
            .map_err(err)??;
        let cert =
            rcgen::generate_simple_self_signed(vec!["phantom.invalid".into()]).map_err(err)?;
        let offer = Offer {
            protocol: PROTOCOL.into(),
            session_id: options.session_id.clone(),
            from: options.local_peer.clone(),
            to: options.remote_peer.clone(),
            certificate_der: hex::encode(cert.cert.der()),
            ice_sdp: sdp,
        };
        Ok(Self {
            options,
            ice,
            offer,
            key: cert.signing_key.serialize_der(),
            cert: cert.cert.der().clone(),
        })
    }
    pub fn offer(&self) -> Offer {
        self.offer.clone()
    }
    // Consumes the prepared session. A failure cannot silently retry or reuse it.
    pub async fn establish(self, remote: Offer, initiator: bool) -> io::Result<Tunnel> {
        if remote.protocol != PROTOCOL
            || remote.session_id != self.options.session_id
            || remote.from != self.options.remote_peer
            || remote.to != self.options.local_peer
            || remote.certificate_der.len() > 8192
            || remote.ice_sdp.len() > 4095
        {
            return Err(err(
                "Remote offer does not match the authenticated peer/session",
            ));
        }
        let deadline = self.options.deadline;
        tokio::time::timeout(deadline, self.establish_inner(remote, initiator))
            .await
            .map_err(err)?
    }
    async fn establish_inner(self, remote: Offer, initiator: bool) -> io::Result<Tunnel> {
        let remote_cert = CertificateDer::from(hex::decode(remote.certificate_der).map_err(err)?);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(remote_cert).map_err(err)?;
        let roots = Arc::new(roots);
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            roots.clone(),
            provider.clone(),
        )
        .build()
        .map_err(err)?;
        let mut server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(err)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![self.cert.clone()],
                PrivatePkcs8KeyDer::from(self.key.clone()).into(),
            )
            .map_err(err)?;
        server_tls.alpn_protocols = vec![PROTOCOL.as_bytes().to_vec()];
        let mut client_tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(err)?
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![self.cert], PrivatePkcs8KeyDer::from(self.key).into())
            .map_err(err)?;
        client_tls.alpn_protocols = vec![PROTOCOL.as_bytes().to_vec()];
        let mut transport = quinn::TransportConfig::default();
        transport
            .initial_mtu(1200)
            .min_mtu(1200)
            .mtu_discovery_config(None);
        transport
            .max_concurrent_bidi_streams(8u32.into())
            .max_concurrent_uni_streams(0u32.into());
        transport.max_idle_timeout(Some(Duration::from_secs(30).try_into().map_err(err)?));
        transport
            .receive_window((8u32 * 1024 * 1024).into())
            .stream_receive_window((4u32 * 1024 * 1024).into());
        transport
            .send_window(8 * 1024 * 1024)
            .datagram_receive_buffer_size(Some(256 * 1024))
            .datagram_send_buffer_size(256 * 1024);
        let transport = Arc::new(transport);
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(server_tls).map_err(err)?,
        ));
        server.transport_config(transport.clone());
        server
            .max_incoming(1)
            .incoming_buffer_size(65536)
            .incoming_buffer_size_total(65536);
        let mut client = quinn::ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(client_tls).map_err(err)?,
        ));
        client.transport_config(transport);
        // Allocate Quinn before ICE connects, so early QUIC Initial packets are
        // buffered by the bounded native receive queue rather than lost.
        let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            if initiator { None } else { Some(server) },
            self.ice.clone(),
            Arc::new(quinn::TokioRuntime),
        )
        .map_err(err)?;
        endpoint.set_default_client_config(client);
        self.ice.connect(&remote.ice_sdp).await?;
        let connection = if initiator {
            endpoint
                .connect(ice::REMOTE, "phantom.invalid")
                .map_err(err)?
                .await
                .map_err(err)?
        } else {
            endpoint
                .accept()
                .await
                .ok_or_else(|| err("Endpoint closed"))?
                .await
                .map_err(err)?
        };
        endpoint.set_server_config(None); // A session admits exactly one connection.
                                          // TLS authenticates the peer's ephemeral certificate; this stream binds
                                          // it to the exact Phantom session and both expected peer names.
        let hello = serde_json::to_vec(&(
            PROTOCOL,
            &self.options.session_id,
            &self.options.local_peer,
            &self.options.remote_peer,
        ))
        .map_err(err)?;
        if initiator {
            let (mut send, mut recv) = connection.open_bi().await.map_err(err)?;
            send.write_all(&hello).await.map_err(err)?;
            send.finish().map_err(err)?;
            if recv.read_to_end(2048).await.map_err(err)? != b"accepted" {
                return Err(err("Session rejected"));
            }
        } else {
            let (mut send, mut recv) = connection.accept_bi().await.map_err(err)?;
            let bytes = recv.read_to_end(2048).await.map_err(err)?;
            let expected = serde_json::to_vec(&(
                PROTOCOL,
                &self.options.session_id,
                &self.options.remote_peer,
                &self.options.local_peer,
            ))
            .map_err(err)?;
            if bytes != expected {
                return Err(err("Session binding rejected"));
            }
            send.write_all(b"accepted").await.map_err(err)?;
            send.finish().map_err(err)?;
        }
        Ok(Tunnel {
            connection,
            endpoint,
            ice: self.ice,
        })
    }
}
