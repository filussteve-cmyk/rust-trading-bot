//! Proxy-aware WebSocket connector.
//!
//! The bot frequently runs on hosts where direct outbound TLS is intercepted and
//! all egress must go through an HTTP proxy (`HTTPS_PROXY`). tokio-tungstenite
//! does not honor proxy env vars, so its `connect_async` can never reach
//! `wss://` endpoints from such hosts. This module opens the tunnel itself:
//! TCP to the proxy, `CONNECT target:443` (with `Proxy-Authorization` taken from
//! the proxy URL userinfo when present), TLS upgrade with SNI for the target
//! host, then the tungstenite client handshake over the resulting stream.

use anyhow::{Context, Result};
use base64::Engine as _;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub trait WsIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> WsIo for T {}

pub type BoxWs = WebSocketStream<Box<dyn WsIo>>;

pub type WsSplit = (
    futures_util::stream::SplitSink<BoxWs, Message>,
    futures_util::stream::SplitStream<BoxWs>,
);

/// Connect a `ws://`/`wss://` URL and return the split sink/stream.
/// Tunnels through `HTTPS_PROXY`/`https_proxy` when set, otherwise connects directly.
pub async fn connect_ws_split(ws_url: &str) -> Result<WsSplit> {
    let (host, port, tls) = parse_ws_url(ws_url)?;
    let stream: Box<dyn WsIo> = match proxy_from_env() {
        Some((proxy_host, proxy_port, userinfo)) => {
            tracing::info!("WS via proxy {proxy_host}:{proxy_port} -> {host}:{port}");
            let tcp =
                connect_via_proxy(&proxy_host, proxy_port, userinfo.as_deref(), &host, port).await?;
            let tls_stream = upgrade_tls(&host, tcp).await?;
            Box::new(tls_stream)
        }
        None if tls => {
            let tcp = TcpStream::connect((host.as_str(), port)).await?;
            Box::new(upgrade_tls(&host, tcp).await?)
        }
        None => {
            let tcp = TcpStream::connect((host.as_str(), port)).await?;
            Box::new(tcp)
        }
    };
    let (ws, _resp) = tokio_tungstenite::client_async(ws_url, stream).await?;
    Ok(ws.split())
}

fn parse_ws_url(ws_url: &str) -> Result<(String, u16, bool)> {
    let (scheme, rest) = ws_url.split_once("://").context("bad ws url")?;
    let tls = scheme == "wss";
    anyhow::ensure!(tls || scheme == "ws", "unsupported ws scheme");
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().context("bad ws port")?),
        None => (authority.to_string(), if tls { 443 } else { 80 }),
    };
    anyhow::ensure!(!host.is_empty(), "empty ws host");
    Ok((host, port, tls))
}

/// Returns (proxy_host, proxy_port, userinfo) when a proxy is configured.
fn proxy_from_env() -> Option<(String, u16, Option<String>)> {
    let raw = std::env::var("HTTPS_PROXY")
        .or_else(|_| std::env::var("https_proxy"))
        .ok()?;
    let rest = raw.split_once("://").map(|(_, r)| r).unwrap_or(&raw);
    if rest.is_empty() {
        return None;
    }
    let (userinfo, hostport) = match rest.rsplit_once('@') {
        Some((u, h)) => (Some(u.to_string()), h),
        None => (None, rest),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(3128)),
        None => (hostport.to_string(), 3128),
    };
    if host.is_empty() {
        return None;
    }
    Some((host, port, userinfo))
}

async fn connect_via_proxy(
    proxy_host: &str,
    proxy_port: u16,
    userinfo: Option<&str>,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream> {
    let mut tcp = tokio::time::timeout(
        Duration::from_secs(15),
        TcpStream::connect((proxy_host, proxy_port)),
    )
    .await
    .context("proxy TCP connect timed out")??;

    let mut req = format!(
        "CONNECT {target_host}:{target_port} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\n"
    );
    if let Some(ui) = userinfo {
        let enc = base64::engine::general_purpose::STANDARD.encode(ui);
        req.push_str(&format!("Proxy-Authorization: Basic {enc}\r\n"));
    }
    req.push_str("\r\n");
    tcp.write_all(req.as_bytes()).await?;

    let mut buf: Vec<u8> = Vec::with_capacity(512);
    let mut tmp = [0u8; 512];
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let n = tcp.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            if buf.len() > 8192 {
                anyhow::bail!("proxy response too large");
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("proxy CONNECT timed out")??;

    let status = String::from_utf8_lossy(&buf);
    let status_line = status.lines().next().unwrap_or("");
    if !status_line.contains(" 200 ") {
        anyhow::bail!("proxy CONNECT rejected: {status_line}");
    }
    Ok(tcp)
}

async fn upgrade_tls(host: &str, tcp: TcpStream) -> Result<tokio_native_tls::TlsStream<TcpStream>> {
    let native = native_tls::TlsConnector::new()?;
    let connector = tokio_native_tls::TlsConnector::from(native);
    let s = tokio::time::timeout(Duration::from_secs(20), connector.connect(host, tcp))
        .await
        .context("TLS handshake timed out")??;
    Ok(s)
}
