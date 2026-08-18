//! Connection-level Responses WebSocket FSM.

use axum::extract::ws::{Message as AxumWsMessage, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use wreq::ws::message::Message as WreqWsMessage;

use super::adapter::ResponsesWebSocketRelayDirective;
use super::client::{adapter_drain_ready, forward_client_message, RelayDisposition};
use super::frame::{encode_opaque_websocket_event, ParsedResponsesWebSocketFrame};
use super::lifecycle::{
    await_pending_adapter_observation, finalize_active_turn, queue_turn_finalization,
    settle_turn_finalization, spawn_bounded_adapter_observation, PreviousAttemptSettled,
};
use super::plan_admission::terminate_responses_websocket_for_plan_permit_loss;
use super::quota::{
    detach_exhausted_upstream, is_usage_limit_error_event, mark_active_response_retry_unsafe,
    observe_active_response_rebind_safety, retry_active_turn_after_quota_exhaustion,
};
use super::relay_policy::{
    classify_quota_relay, fatal_relay_policy, FatalRelaySignal, QuotaRelayAction, QuotaRelayFacts,
};
use super::settlement::settle_signal_for_client_delivery_failure;
use super::state::BoundResponsesConnection;
use super::turn::{
    ResponsesProviderAttempt, ResponsesWebSocketTurnObservation, ResponsesWebSocketTurnOutcome,
};
use super::upstream::{close_bound_upstream, receive_optional_upstream};
use crate::handlers::proxy::websocket::ingress::WebSocketRequestContext;
use crate::handlers::proxy::websocket::session::{
    wait_for_optional_deadline, CLOSE_INTERNAL_ERROR, CLOSE_TRY_AGAIN, WEBSOCKET_LOG_TRANSPORT,
};
use crate::handlers::proxy::websocket::transport::{
    close_client_socket, send_client_message, send_gateway_error_with_status,
    send_responses_websocket_error, upstream_message_to_client,
};
use crate::AppState;

const LOG_TARGET: &str = "aether_gateway::handlers::proxy::responses_ws";

/// 写客户端 socket 失败时记录的投递失败原因。刻意不说「客户端在终态前断开」：
/// 供应商的终态可能已经到达，只是最后一跳没送出去。
const CLIENT_DELIVERY_FAILED_REASON: &str =
    "gateway could not relay the provider event to the client";

macro_rules! debug {
    ($($arg:tt)*) => {
        tracing::debug!(target: LOG_TARGET, $($arg)*)
    };
}

macro_rules! warn {
    ($($arg:tt)*) => {
        tracing::warn!(target: LOG_TARGET, $($arg)*)
    };
}

pub(super) async fn relay_bound_connection(
    client_socket: &mut WebSocket,
    bound: &mut BoundResponsesConnection,
    state: &AppState,
    context: &WebSocketRequestContext,
) {
    loop {
        let active_turn_deadline = bound.turn_state.attempt().map(|turn| turn.deadline());
        tokio::select! {
            _ = wait_for_optional_deadline(active_turn_deadline.map(|deadline| deadline.deadline)) => {
                let Some(turn_deadline) = active_turn_deadline else {
                    continue;
                };
                warn!(
                    event_name = "responses_websocket_turn_timeout",
                    log_type = "ops",
                    transport = WEBSOCKET_LOG_TRANSPORT,
                    websocket = true,
                    trace_id = %context.trace_id,
                    timeout_phase = ?turn_deadline.phase,
                    timeout_ms = turn_deadline.timeout.as_millis() as u64,
                    "Responses WebSocket response did not reach its configured deadline"
                );
                finalize_active_turn(bound, state, turn_deadline.phase.outcome()).await;
                send_gateway_error_with_status(
                    client_socket,
                    504,
                    turn_deadline.phase.error_code(),
                    turn_deadline.phase.client_message(),
                ).await;
                close_bound_upstream(bound).await;
                close_client_socket(
                    client_socket,
                    CLOSE_TRY_AGAIN,
                    turn_deadline.phase.error_code(),
                ).await;
                break;
            }
            client_message = client_socket.next() => {
                let Some(client_message) = client_message else {
                    finalize_active_turn(
                        bound,
                        state,
                        ResponsesWebSocketTurnOutcome::client_disconnected(),
                    ).await;
                    close_bound_upstream(bound).await;
                    break;
                };
                let Ok(client_message) = client_message else {
                    warn!(
                        event_name = "responses_websocket_client_receive_failed",
                        log_type = "ops",
                        transport = WEBSOCKET_LOG_TRANSPORT,
                        websocket = true,
                        trace_id = %context.trace_id,
                        "client WebSocket receive failed"
                    );
                    finalize_active_turn(
                        bound,
                        state,
                        ResponsesWebSocketTurnOutcome::client_disconnected(),
                    ).await;
                    close_bound_upstream(bound).await;
                    break;
                };
                match Box::pin(forward_client_message(
                    client_message,
                    bound,
                    client_socket,
                    state,
                    context,
                ))
                .await
                {
                    RelayDisposition::Continue => {}
                    RelayDisposition::Close => {
                        finalize_active_turn(
                            bound,
                            state,
                            ResponsesWebSocketTurnOutcome::client_disconnected(),
                        ).await;
                        break;
                    }
                    RelayDisposition::PlanUsagePermitLost => {
                        warn!(
                            event_name = "responses_websocket_plan_usage_concurrency_lost_before_send",
                            log_type = "ops",
                            transport = WEBSOCKET_LOG_TRANSPORT,
                            websocket = true,
                            trace_id = %context.trace_id,
                            "gateway stopped a Responses WebSocket turn before its upstream send after the subscription plan concurrency lease became unhealthy"
                        );
                        close_bound_upstream(bound).await;
                        terminate_responses_websocket_for_plan_permit_loss(client_socket).await;
                        break;
                    }
                    RelayDisposition::UpstreamError(code) => {
                        warn!(
                            event_name = "responses_websocket_upstream_send_failed",
                            log_type = "ops",
                            transport = WEBSOCKET_LOG_TRANSPORT,
                            websocket = true,
                            trace_id = %context.trace_id,
                            error_code = code,
                            "Upstream WebSocket send failed"
                        );
                        finalize_active_turn(
                            bound,
                            state,
                            ResponsesWebSocketTurnOutcome::upstream_send_failed(),
                        ).await;
                        send_gateway_error_with_status(
                            client_socket,
                            502,
                            code,
                            "Gateway could not forward the WebSocket event upstream",
                        ).await;
                        close_bound_upstream(bound).await;
                        close_client_socket(client_socket, CLOSE_INTERNAL_ERROR, code).await;
                        break;
                    }
                }
            }
            upstream_message = receive_optional_upstream(&mut bound.upstream) => {
                let Some(upstream_message) = upstream_message else {
                    finalize_active_turn(
                        bound,
                        state,
                        ResponsesWebSocketTurnOutcome::upstream_closed(),
                    ).await;
                    bound.upstream = None;
                    close_client_socket(client_socket, 1000, "upstream_closed").await;
                    break;
                };
                let Ok(upstream_message) = upstream_message else {
                    warn!(
                        event_name = "responses_websocket_upstream_receive_failed",
                        log_type = "ops",
                        transport = WEBSOCKET_LOG_TRANSPORT,
                        websocket = true,
                        trace_id = %context.trace_id,
                        "Upstream WebSocket receive failed"
                    );
                    finalize_active_turn(
                        bound,
                        state,
                        ResponsesWebSocketTurnOutcome::upstream_receive_failed(),
                    ).await;
                    send_gateway_error_with_status(
                        client_socket,
                        502,
                        "responses_websocket_receive_failed",
                        "Provider connection closed unexpectedly",
                    ).await;
                    bound.upstream = None;
                    close_client_socket(client_socket, CLOSE_INTERNAL_ERROR, "upstream_receive_failed").await;
                    break;
                };
                let parsed_upstream_frame = match &upstream_message {
                    WreqWsMessage::Text(text) => {
                        ParsedResponsesWebSocketFrame::parse(text.as_str()).ok()
                    }
                    _ => None,
                };
                let parsed_upstream_event = parsed_upstream_frame
                    .as_ref()
                    .map(ParsedResponsesWebSocketFrame::event);
                if let WreqWsMessage::Text(text) = &upstream_message {
                    debug!(
                        event_name = "responses_websocket_upstream_event",
                        log_type = "event",
                        transport = WEBSOCKET_LOG_TRANSPORT,
                        websocket = true,
                        trace_id = %context.trace_id,
                        event_type = %parsed_upstream_frame
                            .as_ref()
                            .map(ParsedResponsesWebSocketFrame::event_type_for_log)
                            .unwrap_or_else(|| "invalid_json".to_string()),
                        frame_bytes = text.len(),
                        chunked = parsed_upstream_frame
                            .as_ref()
                            .is_some_and(ParsedResponsesWebSocketFrame::is_chunked),
                        active_turn = bound.turn_state.response_in_flight(),
                        "gateway received Responses WebSocket event"
                    );
                }
                if matches!(&upstream_message, WreqWsMessage::Binary(_)) {
                    mark_active_response_retry_unsafe(bound, "upstream_binary_frame");
                } else if matches!(&upstream_message, WreqWsMessage::Text(_))
                    && parsed_upstream_event.is_none()
                {
                    mark_active_response_retry_unsafe(bound, "invalid_upstream_event");
                }
                if let Some(event) = parsed_upstream_event {
                    observe_active_response_rebind_safety(bound, event);
                    if bound.pending_adapter_drain.is_none()
                        && bound.adapter.observes_upstream_events()
                    {
                        let adapter = bound.adapter;
                        if let Some(observation) = adapter.observe_upstream_event(event) {
                            let directive = observation.drain;
                            await_pending_adapter_observation(bound).await;
                            let state_for_observation = state.clone();
                            let trace_id = context.trace_id.clone();
                            let report_context = bound.decision_template.report_context.clone();
                            bound.pending_adapter_observation = Some(spawn_bounded_adapter_observation(async move {
                                adapter
                                    .persist_upstream_observation(
                                        &state_for_observation,
                                        &trace_id,
                                        report_context.as_ref(),
                                        observation,
                                    )
                                    .await;
                            }));
                            if let Some(directive) = directive {
                                bound.pending_adapter_drain = Some(directive);
                                // A definitive quota signal must be visible to
                                // the next planner before a transparent retry.
                                await_pending_adapter_observation(bound).await;
                            }
                        }
                    }
                }
                let observation = match &upstream_message {
                    WreqWsMessage::Text(text) => {
                        let adapter = bound.adapter;
                        match parsed_upstream_frame.as_ref() {
                            Some(frame) => bound
                                .turn_state
                                .attempt_mut()
                                .and_then(|turn| turn.observe_upstream_frame(frame, adapter)),
                            None => {
                                if let Some(turn) = bound.turn_state.attempt_mut() {
                                    turn.observe_invalid_upstream_text(text.as_str())
                                }
                                else {
                                    None
                                }
                            }
                        }
                    }
                    _ => None,
                };
                if matches!(
                    observation,
                    Some(ResponsesWebSocketTurnObservation::Started)
                        | Some(ResponsesWebSocketTurnObservation::Terminal(_))
                ) {
                    if let Some(turn) = bound.turn_state.attempt_mut() {
                        turn.mark_stream_started(state).await;
                    }
                }
                let terminal_outcome = match observation {
                    Some(ResponsesWebSocketTurnObservation::Terminal(outcome)) => Some(outcome),
                    _ => None,
                };
                if matches!(&upstream_message, WreqWsMessage::Text(_))
                    && parsed_upstream_frame.is_none()
                {
                    let policy = fatal_relay_policy(FatalRelaySignal::InvalidUpstreamText);
                    finalize_active_turn(
                        bound,
                        state,
                        terminal_outcome.unwrap_or_else(
                            ResponsesWebSocketTurnOutcome::upstream_receive_failed,
                        ),
                    )
                    .await;
                    send_responses_websocket_error(
                        client_socket,
                        policy.status_code,
                        "server_error",
                        policy.error_code,
                        policy.client_message,
                    )
                    .await;
                    close_bound_upstream(bound).await;
                    close_client_socket(
                        client_socket,
                        policy.close_code,
                        policy.close_reason,
                    )
                    .await;
                    break;
                }
                let is_close = matches!(upstream_message, WreqWsMessage::Close(_));
                let drain_for_adapter = adapter_drain_ready(
                    bound.pending_adapter_drain,
                    bound.turn_state.response_in_flight(),
                    observation,
                    is_close,
                );
                let quota_facts = QuotaRelayFacts {
                    drain_ready: drain_for_adapter,
                    retry_current_turn: bound
                        .pending_adapter_drain
                        .is_some_and(|directive| directive.retry_current_turn)
                        && bound
                            .turn_state
                            .logical()
                            .is_some_and(|turn| turn.quota_retry_block_reason().is_none()),
                    transparent_retry_failed: false,
                    usage_limit_error: parsed_upstream_event.is_some_and(is_usage_limit_error_event),
                    upstream_closed: is_close,
                };
                let mut quota_relay_action = classify_quota_relay(quota_facts);
                if matches!(quota_relay_action, QuotaRelayAction::AttemptTransparentRetry) {
                    // detach_attempt 保留 logical turn：重试是同一轮请求的下一个 attempt。
                    let retry_turn = bound.turn_state.detach_attempt();
                    // 先结算旧 attempt 并等它落地，再规划下一个 attempt。两个理由：
                    //
                    // 1. 规划要读 health / adaptive / pool 状态，而这些正是旧
                    //    attempt 结算时才投射的。普通的新 turn 早就在 client.rs 里
                    //    用 await_pending_turn_finalization 挡住了「基于陈旧状态
                    //    规划」，透明重试这条路径原先漏了这一步。
                    // 2. 旧 attempt 还占着自己的 pool key lease。不先释放，重试就
                    //    可能因为「这把 key 仍被占用」而挑不到本该可用的替代 key，
                    //    或者干脆判成无可用供应商。
                    let settled = match retry_turn {
                        Some(mut turn) => {
                            turn.release_admission().await;
                            settle_turn_finalization(
                                bound,
                                state,
                                turn,
                                terminal_outcome.unwrap_or_else(
                                    ResponsesWebSocketTurnOutcome::upstream_closed,
                                ),
                            )
                            .await
                        }
                        None => Some(PreviousAttemptSettled::nothing_to_settle()),
                    };
                    // Planning and binding a replacement carries the complete
                    // scheduler/provider state machine. Keep that large future
                    // off the relay task's stack; the default Tokio/test worker
                    // stack is otherwise easy to exhaust on this rare branch.
                    if let Some(settled) = settled {
                        match Box::pin(retry_active_turn_after_quota_exhaustion(
                            bound, state, context, settled,
                        ))
                        .await
                        {
                            Ok(true) => continue,
                            Ok(false) => {}
                            Err(()) => {
                                finalize_active_turn(
                                    bound,
                                    state,
                                    ResponsesWebSocketTurnOutcome::connection_admission_lost(),
                                )
                                .await;
                                close_bound_upstream(bound).await;
                                terminate_responses_websocket_for_plan_permit_loss(client_socket)
                                    .await;
                                break;
                            }
                        }
                    }
                    // 重试失败。旧 attempt 已经结算，logical turn 仍停在
                    // Replanning，所以后面分支里的 end() / finalize_active_turn
                    // 只会清掉 logical turn 而不会交出 attempt——不存在重复结算。
                    quota_relay_action = classify_quota_relay(QuotaRelayFacts {
                        retry_current_turn: false,
                        transparent_retry_failed: true,
                        ..quota_facts
                    });
                }
                let detach_after_forward =
                    matches!(quota_relay_action, QuotaRelayAction::ForwardQuotaAndDetach);
                if detach_after_forward && is_close {
                    let directive = bound
                        .pending_adapter_drain
                        .expect("adapter drain state should be present");
                    finalize_active_turn(
                        bound,
                        state,
                        terminal_outcome
                            .unwrap_or_else(ResponsesWebSocketTurnOutcome::provider_quota_exhausted),
                    )
                    .await;
                    send_gateway_error_with_status(
                        client_socket,
                        429,
                        directive.error_code,
                        "Provider connection closed after reporting exhausted quota; send a new response.create to select another Provider connection",
                    )
                    .await;
                    detach_exhausted_upstream(bound, directive, &context.trace_id).await;
                    continue;
                }
                // Standard Responses frames cross the gateway byte-for-byte unless PII
                // restoration has something to replace. Codex may wrap public events with
                // provider-private side-channel chunks; only that explicit envelope is
                // peeled, and each retained event is serialized as a complete opaque Value.
                // Observation and capture continue to consume the redacted event, while the
                // final client hop receives restored text.
                let relay_directive = parsed_upstream_frame
                    .as_ref()
                    .map(|frame| {
                        bound
                            .adapter
                            .relay_directive_for_upstream_event(frame.event())
                    });
                let mut relay_send_error = None;
                let mut relay_serialization_failed = false;
                match relay_directive {
                    Some(ResponsesWebSocketRelayDirective::ForwardOriginal) => {
                        let restored = parsed_upstream_frame
                            .as_ref()
                            .and_then(|frame| {
                                bound
                                    .redaction_restorer
                                    .restore_provider_frame_text(frame.event())
                            });
                        let client_frame = match restored {
                            Some(text) => AxumWsMessage::Text(text.into()),
                            None => upstream_message_to_client(upstream_message.clone()),
                        };
                        match send_client_message(client_socket, client_frame).await {
                            Ok(()) => {
                                if let (Some(turn), Some(frame)) = (
                                    bound.turn_state.attempt_mut(),
                                    parsed_upstream_frame.as_ref(),
                                ) {
                                    turn.capture_client_frame(frame.event());
                                }
                            }
                            Err(error) => relay_send_error = Some(error),
                        }
                    }
                    Some(ResponsesWebSocketRelayDirective::ForwardEvents(events)) => {
                        for event in events {
                            let text = match bound
                                .redaction_restorer
                                .restore_provider_frame_text(event)
                            {
                                Some(restored) => restored,
                                None => match encode_opaque_websocket_event(event) {
                                    Ok(encoded) => encoded,
                                    Err(_) => {
                                        relay_serialization_failed = true;
                                        break;
                                    }
                                },
                            };
                            match send_client_message(
                                client_socket,
                                AxumWsMessage::Text(text.into()),
                            )
                            .await
                            {
                                Ok(()) => {
                                    if let Some(turn) = bound.turn_state.attempt_mut() {
                                        turn.capture_client_frame(event);
                                    }
                                }
                                Err(error) => {
                                    relay_send_error = Some(error);
                                    break;
                                }
                            }
                        }
                    }
                    Some(ResponsesWebSocketRelayDirective::SuppressProviderPrivate) => {}
                    None => {
                        if let Err(error) = send_client_message(
                            client_socket,
                            upstream_message_to_client(upstream_message.clone()),
                        )
                        .await
                        {
                            relay_send_error = Some(error);
                        }
                    }
                }
                if relay_serialization_failed {
                    warn!(
                        event_name = "responses_websocket_provider_event_serialization_failed",
                        log_type = "ops",
                        transport = WEBSOCKET_LOG_TRANSPORT,
                        websocket = true,
                        trace_id = %context.trace_id,
                        provider_terminal_reached = terminal_outcome.is_some(),
                        "gateway could not serialize an opaque provider event"
                    );
                    bound
                        .turn_state
                        .record_client_delivery_aborted(CLIENT_DELIVERY_FAILED_REASON);
                    finalize_active_turn(
                        bound,
                        state,
                        settle_signal_for_client_delivery_failure(terminal_outcome),
                    )
                    .await;
                    send_gateway_error_with_status(
                        client_socket,
                        502,
                        "responses_websocket_event_serialization_failed",
                        "Gateway could not relay the provider event",
                    )
                    .await;
                    close_bound_upstream(bound).await;
                    close_client_socket(
                        client_socket,
                        CLOSE_INTERNAL_ERROR,
                        "provider_event_serialization_failed",
                    )
                    .await;
                    break;
                }
                if let Some(error) = relay_send_error {
                    warn!(
                        event_name = "responses_websocket_client_send_failed",
                        log_type = "ops",
                        transport = WEBSOCKET_LOG_TRANSPORT,
                        websocket = true,
                        trace_id = %context.trace_id,
                        error_code = error.as_str(),
                        provider_terminal_reached = terminal_outcome.is_some(),
                        "gateway could not relay a provider event to the client"
                    );
                    // 投递失败是独立事实，不能覆盖已经到达的 provider 终态：
                    // 供应商已经完成推理并消耗 token，账单按它的终态计。
                    bound
                        .turn_state
                        .record_client_delivery_aborted(CLIENT_DELIVERY_FAILED_REASON);
                    finalize_active_turn(
                        bound,
                        state,
                        settle_signal_for_client_delivery_failure(terminal_outcome),
                    ).await;
                    close_bound_upstream(bound).await;
                    break;
                }
                if let Some(outcome) = terminal_outcome {
                    finalize_active_turn(bound, state, outcome).await;
                } else if is_close {
                    finalize_active_turn(
                        bound,
                        state,
                        ResponsesWebSocketTurnOutcome::upstream_closed(),
                    )
                    .await;
                }
                if detach_after_forward {
                    let directive = bound
                        .pending_adapter_drain
                        .expect("adapter drain state should be present");
                    if bound.turn_state.response_in_flight() {
                        finalize_active_turn(
                            bound,
                            state,
                            ResponsesWebSocketTurnOutcome::provider_quota_exhausted(),
                        )
                        .await;
                    }
                    detach_exhausted_upstream(bound, directive, &context.trace_id).await;
                    continue;
                }
                if drain_for_adapter {
                    let directive = bound
                        .pending_adapter_drain
                        .expect("adapter drain state should be present");
                    detach_exhausted_upstream(bound, directive, &context.trace_id).await;
                    continue;
                }
                if is_close {
                    bound.upstream = None;
                    break;
                }
            }
        }
    }
}
