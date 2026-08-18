//! Upstream WebSocket handshake and frame conversion utilities.
//!
//! These helpers intentionally do not parse messages.  A protocol adapter is
//! responsible for deciding when and what to send, while this module owns the
//! HTTP-to-WebSocket transport conversion and provider transport profile.

use std::collections::BTreeMap;
use std::time::Duration;

use axum::extract::ws::{CloseFrame as AxumCloseFrame, Message as AxumWsMessage, WebSocket};
use axum::http::header::{
    ACCEPT, ACCEPT_ENCODING, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HOST,
    PROXY_AUTHORIZATION, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use axum::http::{HeaderMap, HeaderName};
use futures_util::{SinkExt, TryFutureExt};
use serde_json::json;
use url::Url;
use wreq::ws::message::{CloseFrame as WreqCloseFrame, Message as WreqWsMessage};

use crate::ai_serving::AiExecutionDecision;
use crate::execution_runtime::transport::{
    build_browser_wreq_client, build_request_headers, ExecutionTransportControls,
};
use crate::handlers::proxy::websocket::session::{
    WebSocketSessionLimits, RELAY_WRITE_TIMEOUT, TEARDOWN_WRITE_TIMEOUT,
};

#[derive(Clone, Copy)]
pub(crate) struct UpstreamWebSocketErrorCodes {
    pub(crate) upstream_url_missing: &'static str,
    pub(crate) upstream_url_invalid: &'static str,
    pub(crate) headers_invalid: &'static str,
    pub(crate) client_build_failed: &'static str,
    pub(crate) proxy_invalid: &'static str,
    pub(crate) tunnel_proxy_unsupported: &'static str,
    pub(crate) handshake_failed: &'static str,
    pub(crate) upgrade_rejected: &'static str,
    pub(crate) upgrade_failed: &'static str,
}

pub(crate) struct UpstreamWebSocketConnection {
    pub(crate) socket: wreq::ws::WebSocket,
    pub(crate) response_headers: BTreeMap<String, String>,
}

pub(crate) async fn connect_upstream_websocket(
    decision: &AiExecutionDecision,
    limits: WebSocketSessionLimits,
    errors: UpstreamWebSocketErrorCodes,
) -> Result<UpstreamWebSocketConnection, &'static str> {
    let upstream_url = decision
        .upstream_url
        .as_deref()
        .ok_or(errors.upstream_url_missing)?;
    let upstream_url = websocket_upstream_url(upstream_url, errors.upstream_url_invalid)?;
    let headers =
        websocket_handshake_headers(&decision.provider_request_headers, errors.headers_invalid)?;
    let client = build_websocket_client(decision, errors)?;
    let response = client
        .websocket(upstream_url.as_str())
        .headers(headers)
        .max_frame_size(limits.max_frame_size)
        .max_message_size(limits.max_message_size)
        .send()
        .await
        .map_err(|_| errors.handshake_failed)?;
    if response.status().as_u16() != 101 {
        return Err(errors.upgrade_rejected);
    }
    let response_headers = websocket_response_headers(response.headers());
    let socket = response
        .into_websocket()
        .await
        .map_err(|_| errors.upgrade_failed)?;
    Ok(UpstreamWebSocketConnection {
        socket,
        response_headers,
    })
}

fn websocket_response_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

pub(crate) fn websocket_upstream_url(
    raw: &str,
    invalid_code: &'static str,
) -> Result<Url, &'static str> {
    let mut url = Url::parse(raw).map_err(|_| invalid_code)?;
    if url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() {
        return Err(invalid_code);
    }
    let websocket_scheme = match url.scheme() {
        "https" => "wss",
        "http" => "ws",
        "wss" | "ws" => return Ok(url),
        _ => return Err(invalid_code),
    };
    url.set_scheme(websocket_scheme).map_err(|_| invalid_code)?;
    Ok(url)
}

pub(crate) fn websocket_handshake_headers(
    provider_headers: &BTreeMap<String, String>,
    invalid_code: &'static str,
) -> Result<HeaderMap, &'static str> {
    // `build_request_headers` already strips `Connection` itself. Read the
    // dynamic hop-by-hop names from the source map first, otherwise a header
    // named by `Connection: keep-alive, x-provider-hop` would survive.
    let connection_scoped_names = provider_headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case(CONNECTION.as_str()))
        .flat_map(|(_, value)| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();
    let mut headers =
        build_request_headers(provider_headers, None, false).map_err(|_| invalid_code)?;
    for name in connection_scoped_names {
        headers.remove(name);
    }
    for header in [
        ACCEPT,
        ACCEPT_ENCODING,
        CONNECTION,
        CONTENT_ENCODING,
        CONTENT_LENGTH,
        CONTENT_TYPE,
        HOST,
        PROXY_AUTHORIZATION,
        TE,
        TRAILER,
        TRANSFER_ENCODING,
        UPGRADE,
    ] {
        headers.remove(header);
    }
    for header in ["keep-alive", "proxy-connection"] {
        headers.remove(header);
    }
    // The WebSocket client owns every Sec-WebSocket-* field, including
    // extensions introduced after this gateway was built.  Passing a
    // downstream handshake field through here can corrupt negotiation or
    // disclose the client's nonce/subprotocol to a different upstream.
    let websocket_managed_names = headers
        .keys()
        .filter(|name| name.as_str().starts_with("sec-websocket-"))
        .cloned()
        .collect::<Vec<_>>();
    for name in websocket_managed_names {
        headers.remove(name);
    }
    Ok(headers)
}

fn build_websocket_client(
    decision: &AiExecutionDecision,
    errors: UpstreamWebSocketErrorCodes,
) -> Result<wreq::Client, &'static str> {
    let timeouts = websocket_timeouts(decision);
    if let Some(profile) = decision.transport_profile.as_ref() {
        return build_browser_wreq_client(
            timeouts.as_ref(),
            decision.proxy.as_ref(),
            profile,
            ExecutionTransportControls::default(),
            false,
        )
        .map_err(|_| errors.client_build_failed);
    }

    let mut builder = wreq::Client::builder();
    if let Some(connect_ms) = timeouts.as_ref().and_then(|timeouts| timeouts.connect_ms) {
        builder = builder.connect_timeout(Duration::from_millis(connect_ms));
    }
    if let Some(proxy) = decision
        .proxy
        .as_ref()
        .filter(|proxy| proxy.enabled != Some(false))
    {
        if let Some(proxy_url) = proxy
            .url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
        {
            let proxy = wreq::Proxy::all(proxy_url).map_err(|_| errors.proxy_invalid)?;
            builder = builder.proxy(proxy);
        } else if proxy.node_id.is_some() || proxy.mode.as_deref() == Some("tunnel") {
            return Err(errors.tunnel_proxy_unsupported);
        }
    }
    builder.build().map_err(|_| errors.client_build_failed)
}

pub(crate) fn websocket_timeouts(
    decision: &AiExecutionDecision,
) -> Option<aether_contracts::ExecutionTimeouts> {
    let mut timeouts = decision.timeouts.clone()?;
    timeouts.read_ms = None;
    timeouts.first_byte_ms = None;
    timeouts.total_ms = None;
    Some(timeouts)
}

/// Why a frame did not reach its peer.  A timeout is reported separately from
/// a socket error because the two describe different peers: one has gone away,
/// the other is still connected but has stopped reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WebSocketWriteError {
    Failed,
    TimedOut,
}

impl WebSocketWriteError {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "write_failed",
            Self::TimedOut => "write_timeout",
        }
    }
}

/// Relays one frame to the client under [`RELAY_WRITE_TIMEOUT`].
pub(crate) async fn send_client_message(
    client_socket: &mut WebSocket,
    message: AxumWsMessage,
) -> Result<(), WebSocketWriteError> {
    bounded_send(
        RELAY_WRITE_TIMEOUT,
        client_socket.send(message).map_err(|_| ()),
    )
    .await
}

/// Sends one frame to the upstream under [`RELAY_WRITE_TIMEOUT`].
pub(crate) async fn send_upstream_message(
    upstream: &mut wreq::ws::WebSocket,
    message: WreqWsMessage,
) -> Result<(), WebSocketWriteError> {
    bounded_send(RELAY_WRITE_TIMEOUT, upstream.send(message).map_err(|_| ())).await
}

/// Queues one frame in the upstream sink without flushing it. Completion means
/// `start_send` succeeded, so callers must conservatively treat the frame as
/// possibly delivered even when a later flush fails or is cancelled.
pub(crate) async fn feed_upstream_message(
    upstream: &mut wreq::ws::WebSocket,
    message: WreqWsMessage,
) -> Result<(), WebSocketWriteError> {
    bounded_send(RELAY_WRITE_TIMEOUT, upstream.feed(message).map_err(|_| ())).await
}

/// Flushes frames previously queued with [`feed_upstream_message`].
pub(crate) async fn flush_upstream_messages(
    upstream: &mut wreq::ws::WebSocket,
) -> Result<(), WebSocketWriteError> {
    bounded_send(RELAY_WRITE_TIMEOUT, upstream.flush().map_err(|_| ())).await
}

/// Best-effort teardown write.  The caller is already ending the session, so
/// the outcome only matters for keeping the wait bounded.
async fn send_teardown_message<F>(write: F)
where
    F: std::future::Future<Output = Result<(), ()>>,
{
    let _ = bounded_send(TEARDOWN_WRITE_TIMEOUT, write).await;
}

async fn bounded_send<F>(budget: Duration, write: F) -> Result<(), WebSocketWriteError>
where
    F: std::future::Future<Output = Result<(), ()>>,
{
    match tokio::time::timeout(budget, write).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(())) => Err(WebSocketWriteError::Failed),
        Err(_) => Err(WebSocketWriteError::TimedOut),
    }
}

/// Sends a WebSocket Close frame upstream without waiting on an unresponsive
/// provider.  The socket is dropped by the caller either way.
pub(crate) async fn close_upstream_socket(
    upstream: &mut wreq::ws::WebSocket,
    frame: Option<WreqCloseFrame>,
) {
    send_teardown_message(upstream.send(WreqWsMessage::Close(frame)).map_err(|_| ())).await;
}

pub(crate) fn upstream_message_to_client(message: WreqWsMessage) -> AxumWsMessage {
    match message {
        WreqWsMessage::Text(text) => AxumWsMessage::Text(text.to_string().into()),
        WreqWsMessage::Binary(data) => AxumWsMessage::Binary(data),
        WreqWsMessage::Ping(data) => AxumWsMessage::Ping(data),
        WreqWsMessage::Pong(data) => AxumWsMessage::Pong(data),
        WreqWsMessage::Close(frame) => AxumWsMessage::Close(frame.map(|frame| AxumCloseFrame {
            code: frame.code.into(),
            reason: frame.reason.to_string().into(),
        })),
    }
}

/// Builds a Responses WebSocket error event in the shape understood by the
/// official client implementations.  The status is part of the event body,
/// not the WebSocket handshake, because the connection is already upgraded.
pub(crate) fn responses_websocket_error_event(
    status: u16,
    error_type: &str,
    code: &str,
    message: &str,
) -> serde_json::Value {
    json!({
        "type": "error",
        "status": status,
        "error": {
            "type": error_type,
            "code": code,
            "message": message,
        },
    })
}

pub(crate) async fn send_responses_websocket_error(
    client_socket: &mut WebSocket,
    status: u16,
    error_type: &str,
    code: &str,
    message: &str,
) {
    let event = responses_websocket_error_event(status, error_type, code, message);
    send_teardown_message(
        client_socket
            .send(AxumWsMessage::Text(event.to_string().into()))
            .map_err(|_| ()),
    )
    .await;
}

pub(crate) async fn send_gateway_error(client_socket: &mut WebSocket, code: &str, message: &str) {
    send_gateway_error_with_status(client_socket, 400, code, message).await;
}

pub(crate) async fn send_gateway_error_with_status(
    client_socket: &mut WebSocket,
    status: u16,
    code: &str,
    message: &str,
) {
    send_responses_websocket_error(client_socket, status, "gateway_error", code, message).await;
}

pub(crate) async fn close_client_socket(client_socket: &mut WebSocket, code: u16, reason: &str) {
    send_teardown_message(
        client_socket
            .send(AxumWsMessage::Close(Some(AxumCloseFrame {
                code,
                reason: reason.to_string().into(),
            })))
            .map_err(|_| ()),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::{
        bounded_send, responses_websocket_error_event, websocket_handshake_headers,
        websocket_upstream_url, WebSocketWriteError, RELAY_WRITE_TIMEOUT, TEARDOWN_WRITE_TIMEOUT,
    };
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[tokio::test]
    async fn a_peer_that_never_drains_its_window_times_out_instead_of_pinning_the_relay() {
        let stalled = std::future::pending::<Result<(), ()>>();

        let outcome = bounded_send(Duration::from_millis(1), stalled).await;

        assert_eq!(outcome, Err(WebSocketWriteError::TimedOut));
    }

    #[tokio::test]
    async fn a_socket_error_is_reported_separately_from_a_stalled_peer() {
        let outcome = bounded_send(RELAY_WRITE_TIMEOUT, std::future::ready(Err(()))).await;

        assert_eq!(outcome, Err(WebSocketWriteError::Failed));
        assert_eq!(WebSocketWriteError::Failed.as_str(), "write_failed");
        assert_eq!(WebSocketWriteError::TimedOut.as_str(), "write_timeout");
    }

    #[tokio::test]
    async fn a_write_that_completes_within_its_budget_succeeds() {
        let outcome = bounded_send(RELAY_WRITE_TIMEOUT, std::future::ready(Ok::<(), ()>(()))).await;

        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn teardown_writes_are_given_a_shorter_budget_than_relayed_frames() {
        assert!(TEARDOWN_WRITE_TIMEOUT < RELAY_WRITE_TIMEOUT);
    }

    #[test]
    fn builds_a_client_compatible_responses_error_event() {
        let event = responses_websocket_error_event(
            400,
            "invalid_request_error",
            "previous_response_not_found",
            "Previous response was not found.",
        );

        assert_eq!(event["type"], "error");
        assert_eq!(event["status"], 400);
        assert_eq!(event["error"]["type"], "invalid_request_error");
        assert_eq!(event["error"]["code"], "previous_response_not_found");
        assert_eq!(
            event["error"]["message"],
            "Previous response was not found."
        );
    }

    #[test]
    fn maps_http_url_to_websocket_url_without_losing_path_or_query() {
        let url = websocket_upstream_url(
            "https://example.test/backend-api/codex/responses?x=1",
            "invalid",
        )
        .expect("URL should be converted");
        assert_eq!(
            url.as_str(),
            "wss://example.test/backend-api/codex/responses?x=1"
        );
    }

    #[test]
    fn rejects_upstream_url_with_credentials() {
        assert!(websocket_upstream_url("https://token@example.test/responses", "invalid").is_err());
    }

    #[test]
    fn upstream_handshake_keeps_provider_auth_but_drops_transport_managed_headers() {
        let provider_headers = BTreeMap::from([
            (
                "authorization".to_string(),
                "Bearer provider-token".to_string(),
            ),
            ("x-api-key".to_string(), "provider-api-key".to_string()),
            (
                "cookie".to_string(),
                "provider_session=provider-cookie".to_string(),
            ),
            (
                "connection".to_string(),
                "keep-alive, x-provider-hop".to_string(),
            ),
            ("x-provider-hop".to_string(), "must-not-pass".to_string()),
            ("upgrade".to_string(), "websocket".to_string()),
            (
                "sec-websocket-key".to_string(),
                "downstream-nonce".to_string(),
            ),
            (
                "sec-websocket-future-field".to_string(),
                "future-value".to_string(),
            ),
            (
                "proxy-authorization".to_string(),
                "Basic must-not-pass".to_string(),
            ),
            ("x-provider-header".to_string(), "safe".to_string()),
        ]);

        let headers = websocket_handshake_headers(&provider_headers, "invalid")
            .expect("provider headers should be valid");

        assert_eq!(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer provider-token")
        );
        assert_eq!(
            headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok()),
            Some("provider-api-key")
        );
        assert_eq!(
            headers.get("cookie").and_then(|value| value.to_str().ok()),
            Some("provider_session=provider-cookie")
        );
        assert_eq!(
            headers
                .get("x-provider-header")
                .and_then(|value| value.to_str().ok()),
            Some("safe")
        );
        for name in [
            "connection",
            "x-provider-hop",
            "upgrade",
            "sec-websocket-key",
            "sec-websocket-future-field",
            "proxy-authorization",
        ] {
            assert!(headers.get(name).is_none(), "{name} must not survive");
        }
    }
}
