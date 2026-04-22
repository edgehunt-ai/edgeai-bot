use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudePermissionRequest {
    pub request_id: String,
    pub run_id: String,
    pub transport_session_id: String,
    pub tool_name: Option<String>,
    pub tool_input: Value,
    pub permission_suggestions: Option<Value>,
    pub created_at_unix_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudePermissionResponse {
    pub request_id: String,
    pub behavior: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeHookStatusEvent {
    pub event_id: String,
    pub run_id: String,
    pub transport_session_id: String,
    pub summary: String,
    pub created_at_unix_ms: u128,
}

pub fn claude_permission_runtime_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("claude-permissions")
}

pub fn always_allow_commands_path(state_dir: &Path) -> PathBuf {
    state_dir.join("always_allow_commands.json")
}

pub async fn load_always_allow_commands(state_dir: &Path) -> Vec<String> {
    match tokio::fs::read(always_allow_commands_path(state_dir)).await {
        Ok(body) => serde_json::from_slice::<Vec<String>>(&body).unwrap_or_default(),
        Err(_) => vec![],
    }
}

pub async fn append_always_allow_command(state_dir: &Path, command: &str) -> Result<()> {
    let mut list = load_always_allow_commands(state_dir).await;
    let command = command.trim().to_string();
    if !list.contains(&command) {
        list.push(command);
        tokio::fs::write(
            always_allow_commands_path(state_dir),
            serde_json::to_vec_pretty(&list)?,
        )
        .await?;
    }
    Ok(())
}

pub fn claude_permission_requests_dir(state_dir: &Path) -> PathBuf {
    claude_permission_runtime_dir(state_dir).join("requests")
}

pub fn claude_permission_responses_dir(state_dir: &Path) -> PathBuf {
    claude_permission_runtime_dir(state_dir).join("responses")
}

pub fn claude_permission_request_path(state_dir: &Path, request_id: &str) -> PathBuf {
    claude_permission_requests_dir(state_dir).join(format!("{request_id}.json"))
}

pub fn claude_permission_response_path(state_dir: &Path, request_id: &str) -> PathBuf {
    claude_permission_responses_dir(state_dir).join(format!("{request_id}.json"))
}

pub fn claude_status_events_dir(state_dir: &Path) -> PathBuf {
    claude_permission_runtime_dir(state_dir).join("events")
}

pub fn claude_status_event_path(state_dir: &Path, event_id: &str) -> PathBuf {
    claude_status_events_dir(state_dir).join(format!("{event_id}.json"))
}

pub async fn ensure_permission_runtime_dirs(state_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(claude_permission_requests_dir(state_dir)).await?;
    tokio::fs::create_dir_all(claude_permission_responses_dir(state_dir)).await?;
    tokio::fs::create_dir_all(claude_status_events_dir(state_dir)).await?;
    Ok(())
}

pub async fn run_permission_hook(
    state_dir: PathBuf,
    transport_session_id: String,
    run_id: String,
) -> Result<()> {
    ensure_permission_runtime_dirs(&state_dir).await?;
    tracing::debug!(
        transport_session_id = %transport_session_id,
        run_id = %run_id,
        state_dir = %state_dir.display(),
        "starting Claude permission hook bridge"
    );

    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut body = Vec::new();
    stdin.read_to_end(&mut body).await?;
    let payload = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice::<Value>(&body).context("failed to parse Claude hook input JSON")?
    };
    let hook_event_name = payload
        .get("hook_event_name")
        .and_then(Value::as_str)
        .or_else(|| payload.get("hookEventName").and_then(Value::as_str))
        .unwrap_or_default();
    let tool_name = payload
        .get("tool_name")
        .and_then(Value::as_str)
        .or_else(|| payload.get("toolName").and_then(Value::as_str))
        .map(ToString::to_string);

    if let Some(summary) = summarize_hook_status_event(hook_event_name, tool_name.as_deref(), &payload)
    {
        let event = ClaudeHookStatusEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            transport_session_id: transport_session_id.clone(),
            summary,
            created_at_unix_ms: crate::audit::now_unix_ms(),
        };
        tokio::fs::write(
            claude_status_event_path(&state_dir, &event.event_id),
            serde_json::to_vec_pretty(&event)?,
        )
        .await?;
    }

    if hook_event_name == "PostToolUse" {
        return Ok(());
    }

    let bash_command = payload
        .get("tool_input")
        .and_then(|v| v.get("command"))
        .and_then(Value::as_str)
        .unwrap_or_default();

    let dynamic_allow = load_always_allow_commands(&state_dir).await;
    if tool_name.as_deref() != Some("Bash") || is_auto_allowed_bash_command(bash_command, &dynamic_allow) {
        stdout
            .write_all(
                serde_json::to_string(&serde_json::json!({
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "allow",
                        "permissionDecisionReason": "Allowed by edgeai tool status hook"
                    }
                }))?
                .as_bytes(),
            )
            .await?;
        stdout.flush().await?;
        return Ok(());
    }

    let request_id = uuid::Uuid::new_v4().to_string();
    let permission_request = ClaudePermissionRequest {
        request_id: request_id.clone(),
        run_id: run_id.clone(),
        transport_session_id: transport_session_id.clone(),
        tool_name,
        tool_input: payload
            .get("tool_input")
            .cloned()
            .or_else(|| payload.get("toolInput").cloned())
            .unwrap_or_else(|| payload.clone()),
        permission_suggestions: payload.get("permission_suggestions").cloned(),
        created_at_unix_ms: crate::audit::now_unix_ms(),
    };
    tracing::debug!(
        transport_session_id = %transport_session_id,
        run_id = %run_id,
        request_id = %request_id,
        tool_name = permission_request.tool_name.as_deref().unwrap_or(""),
        request_path = %claude_permission_request_path(&state_dir, &request_id).display(),
        "wrote Claude permission hook request for edgeai approval"
    );

    tokio::fs::write(
        claude_permission_request_path(&state_dir, &request_id),
        serde_json::to_vec_pretty(&permission_request)?,
    )
    .await?;

    let response = wait_for_permission_response(&state_dir, &request_id).await?;
    tracing::debug!(
        transport_session_id = %transport_session_id,
        run_id = %run_id,
        request_id = %request_id,
        behavior = %response.behavior,
        "loaded Claude permission hook decision"
    );
    let _ = tokio::fs::remove_file(claude_permission_request_path(&state_dir, &request_id)).await;
    let _ = tokio::fs::remove_file(claude_permission_response_path(&state_dir, &request_id)).await;

    stdout
        .write_all(
            serde_json::to_string(&permission_hook_output(&permission_request, &response))?
                .as_bytes(),
        )
        .await?;
    stdout.flush().await?;
    Ok(())
}

pub fn is_auto_allowed_bash_command(command: &str, dynamic_allow: &[String]) -> bool {
    let command = command.trim();
    const ASK_PREFIXES: &[&str] = &[
        "chainpilot swap execute",
        "chainpilot swap approve",
        "chainpilot swap revoke",
        "chainpilot token add",
    ];
    if ASK_PREFIXES.iter().any(|p| prefix_matches(command, p)) {
        return false;
    }
    const ALLOW_PREFIXES: &[&str] = &[
        "cast wallet list",
        "chainpilot",
        "date",
        "cd */.claude/skills/polymarket-data-layer && node -e",
        "ls */.claude/skills/polymarket-data-layer",
        "cat */.claude/skills/polymarket-data-layer",
    ];
    if ALLOW_PREFIXES.iter().any(|p| prefix_matches(command, p)) {
        return true;
    }
    dynamic_allow.iter().any(|p| prefix_matches(command, p.as_str()))
}

fn prefix_matches(command: &str, pattern: &str) -> bool {
    if !pattern.contains('*') {
        return command == pattern || command.starts_with(&format!("{pattern} "));
    }
    // Wildcard match: split on '*', each segment must appear in order.
    // After all segments are consumed, the remainder must be empty or start with a space.
    let mut segments = pattern.split('*');
    let first = segments.next().unwrap_or("");
    if !command.starts_with(first) {
        return false;
    }
    let mut pos = first.len();
    for segment in segments {
        if segment.is_empty() {
            continue;
        }
        match command[pos..].find(segment) {
            Some(offset) => pos += offset + segment.len(),
            None => return false,
        }
    }
    let remaining = &command[pos..];
    remaining.is_empty() || remaining.starts_with(' ')
}

fn permission_hook_output(
    _request: &ClaudePermissionRequest,
    response: &ClaudePermissionResponse,
) -> Value {
    match response.behavior.as_str() {
        "allow" => serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",
                "permissionDecisionReason": session_permission_reason(response),
            }
        }),
        _ => serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": response
                    .message
                    .clone()
                    .unwrap_or_else(|| "Denied by edgeai approval".to_string()),
            }
        }),
    }
}

fn session_permission_reason(response: &ClaudePermissionResponse) -> &'static str {
    if response.message.as_deref() == Some("accept_for_session") {
        "Approved for this Telegram session"
    } else {
        "Approved by Telegram"
    }
}

fn summarize_hook_status_event(
    hook_event_name: &str,
    tool_name: Option<&str>,
    payload: &Value,
) -> Option<String> {
    let tool_name = tool_name?;
    match (hook_event_name, tool_name) {
        ("PreToolUse", "WebSearch") => extract_command_from_value(
            payload
                .get("tool_input")
                .or_else(|| payload.get("toolInput"))
                .unwrap_or(payload),
        )
        .map(|query| format!("[search] running: {}", truncate_status_text(&query, 120))),
        ("PostToolUse", "WebSearch") => extract_command_from_value(
            payload
                .get("tool_input")
                .or_else(|| payload.get("toolInput"))
                .unwrap_or(payload),
        )
        .map(|query| format!("[search] done: {}", truncate_status_text(&query, 120))),
        ("PreToolUse", "WebFetch") => extract_command_from_value(
            payload
                .get("tool_input")
                .or_else(|| payload.get("toolInput"))
                .unwrap_or(payload),
        )
        .map(|target| format!("[fetch] running: {}", truncate_status_text(&target, 120))),
        ("PostToolUse", "WebFetch") => extract_command_from_value(
            payload
                .get("tool_input")
                .or_else(|| payload.get("toolInput"))
                .unwrap_or(payload),
        )
        .map(|target| format!("[fetch] done: {}", truncate_status_text(&target, 120))),
        ("PostToolUse", "Bash") => extract_command_from_value(
            payload
                .get("tool_input")
                .or_else(|| payload.get("toolInput"))
                .unwrap_or(payload),
        )
        .map(|command| format!("[command] done: {}", truncate_status_text(&command, 120))),
        _ => None,
    }
}

fn extract_command_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let text = text.trim();
            if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            }
        }
        Value::Array(items) => {
            let parts = items.iter().filter_map(Value::as_str).collect::<Vec<_>>();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(" "))
            }
        }
        Value::Object(map) => {
            if let Some(value) = map.get("query").and_then(extract_command_from_value) {
                return Some(value);
            }
            if let Some(value) = map.get("url").and_then(extract_command_from_value) {
                return Some(value);
            }
            if let Some(value) = map.get("command").and_then(extract_command_from_value) {
                return Some(value);
            }
            if let Some(value) = map.get("commands").and_then(extract_command_from_value) {
                return Some(value);
            }
            None
        }
        _ => None,
    }
}

fn truncate_status_text(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

async fn wait_for_permission_response(
    state_dir: &Path,
    request_id: &str,
) -> Result<ClaudePermissionResponse> {
    let response_path = claude_permission_response_path(state_dir, request_id);
    let mut logged_wait = false;
    loop {
        match tokio::fs::read(&response_path).await {
            Ok(body) => {
                return Ok(serde_json::from_slice(&body).with_context(|| {
                    format!(
                        "failed to parse permission response {}",
                        response_path.display()
                    )
                })?);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !logged_wait {
                    tracing::debug!(
                        request_id,
                        response_path = %response_path.display(),
                        "waiting for Claude permission response file"
                    );
                    logged_wait = true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::io::duplex;

    #[tokio::test]
    async fn mcp_message_roundtrip_uses_content_length_framing() {
        let expected = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list"
        });
        let expected_for_write = expected.clone();

        let (mut tx, rx) = duplex(4096);
        let write_task =
            tokio::spawn(async move { write_mcp_message(&mut tx, &expected_for_write).await });

        let mut reader = BufReader::new(rx);
        let decoded = read_mcp_message(&mut reader)
            .await
            .expect("expected decode success")
            .expect("expected one message");

        write_task.await.expect("writer task joined").expect("writer ok");
        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn wait_for_permission_response_reads_written_response() {
        let dir = tempdir().expect("tempdir");
        ensure_permission_runtime_dirs(dir.path())
            .await
            .expect("runtime dirs");

        let request_id = "req-123";
        let response_path = claude_permission_response_path(dir.path(), request_id);
        let response = ClaudePermissionResponse {
            request_id: request_id.to_string(),
            behavior: "allow".to_string(),
            message: None,
        };

        let writer = tokio::spawn({
            let response_path = response_path.clone();
            let body = serde_json::to_vec_pretty(&response).expect("serialize response");
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                tokio::fs::write(response_path, body).await.expect("write response");
            }
        });

        let loaded = wait_for_permission_response(dir.path(), request_id)
            .await
            .expect("wait for response");
        writer.await.expect("writer task joined");

        assert_eq!(loaded.request_id, request_id);
        assert_eq!(loaded.behavior, "allow");
        assert!(loaded.message.is_none());
    }

    #[test]
    fn permission_payload_formats_allow_and_deny_responses() {
        let allow = ClaudePermissionResponse {
            request_id: "a".to_string(),
            behavior: "allow".to_string(),
            message: None,
        };
        let deny = ClaudePermissionResponse {
            request_id: "d".to_string(),
            behavior: "deny".to_string(),
            message: Some("Denied by test".to_string()),
        };

        assert_eq!(permission_response_payload(&allow), r#"{"behavior":"allow"}"#);
        assert_eq!(
            permission_response_payload(&deny),
            r#"{"behavior":"deny","message":"Denied by test"}"#
        );
    }

    #[test]
    fn permission_hook_output_allows_with_pretooluse_shape() {
        let request = ClaudePermissionRequest {
            request_id: "req-1".to_string(),
            run_id: "run-1".to_string(),
            transport_session_id: "session-1".to_string(),
            tool_name: Some("Bash".to_string()),
            tool_input: serde_json::json!({ "command": "cargo test" }),
            permission_suggestions: Some(serde_json::json!([
                {
                    "addRules": [{ "toolName": "Bash", "ruleContent": "cargo test" }],
                    "behavior": "allow",
                    "destination": "session"
                }
            ])),
            created_at_unix_ms: 0,
        };
        let response = ClaudePermissionResponse {
            request_id: "req-1".to_string(),
            behavior: "allow".to_string(),
            message: Some("accept_for_session".to_string()),
        };

        let output = permission_hook_output(&request, &response);
        assert_eq!(
            output["hookSpecificOutput"]["hookEventName"],
            "PreToolUse"
        );
        assert_eq!(output["hookSpecificOutput"]["permissionDecision"], "allow");
    }

    #[test]
    fn permission_hook_output_denies_with_reason() {
        let request = ClaudePermissionRequest {
            request_id: "req-1".to_string(),
            run_id: "run-1".to_string(),
            transport_session_id: "session-1".to_string(),
            tool_name: Some("Bash".to_string()),
            tool_input: serde_json::json!({ "command": "rm -rf /" }),
            permission_suggestions: None,
            created_at_unix_ms: 0,
        };
        let response = ClaudePermissionResponse {
            request_id: "req-1".to_string(),
            behavior: "deny".to_string(),
            message: Some("Denied by Telegram approval".to_string()),
        };

        let output = permission_hook_output(&request, &response);
        assert_eq!(output["hookSpecificOutput"]["permissionDecision"], "deny");
    }

    #[test]
    fn summarize_hook_status_event_formats_search_status() {
        let payload = serde_json::json!({
            "tool_name": "WebSearch",
            "tool_input": { "query": "latest fed news" }
        });
        let summary =
            summarize_hook_status_event("PreToolUse", Some("WebSearch"), &payload).expect("summary");
        assert_eq!(summary, "[search] running: latest fed news");
    }

    #[tokio::test]
    async fn permission_paths_are_created_under_state_dir() {
        let dir = tempdir().expect("tempdir");
        ensure_permission_runtime_dirs(dir.path())
            .await
            .expect("runtime dirs");

        assert!(claude_permission_requests_dir(dir.path()).exists());
        assert!(claude_permission_responses_dir(dir.path()).exists());
        assert_eq!(
            claude_permission_request_path(dir.path(), "abc")
                .file_name()
                .and_then(|value| value.to_str()),
            Some("abc.json")
        );
        assert_eq!(
            claude_permission_response_path(dir.path(), "xyz")
                .file_name()
                .and_then(|value| value.to_str()),
            Some("xyz.json")
        );
    }
}
