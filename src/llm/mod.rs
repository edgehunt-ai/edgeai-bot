use crate::claude_mcp::{
    ClaudeHookStatusEvent, ClaudePermissionRequest, ClaudePermissionResponse,
    claude_permission_request_path, claude_permission_requests_dir, claude_permission_response_path,
    claude_status_event_path, claude_status_events_dir, ensure_permission_runtime_dirs,
    is_auto_allowed_bash_command, load_always_allow_commands,
};
use crate::config::{LlmConfig, RuntimeConfig};
use anyhow::{Context, Result, bail};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use uuid::Uuid;

const MAX_HISTORY_MESSAGES: usize = 24;
const CODEX_STREAM_IDLE_NOTICE_SECS: u64 = 15;

#[derive(Debug, Clone, Serialize)]
pub struct ProviderOption {
    pub id: String,
    pub label: String,
    pub binary: Option<String>,
    pub installed: bool,
    pub install_command: Option<String>,
    pub supports_native_sessions: bool,
    pub setup_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LlmReply {
    pub provider: String,
    pub provider_session_id: Option<String>,
    pub text: String,
}

#[derive(Debug, Clone)]
pub enum LlmStreamEvent {
    Content(String),
    ApprovalRequested(LlmApprovalRequest),
    ApprovalResolved { request_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmApprovalKind {
    CommandExecution,
    ExecCommand,
    Permissions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmApprovalChoice {
    Accept,
    AcceptForSession,
    AlwaysAllow,
    Decline,
    Cancel,
}

#[derive(Debug, Clone)]
pub struct LlmApprovalRequest {
    pub request_id: String,
    pub kind: LlmApprovalKind,
    pub command: Option<String>,
    pub reason: Option<String>,
    pub allow_accept_for_session: bool,
    pub allow_cancel: bool,
}

#[derive(Debug, Clone)]
pub struct LlmApprovalDecision {
    pub request_id: String,
    pub choice: LlmApprovalChoice,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSession {
    pub transport_session_id: String,
    pub provider: String,
    pub provider_session_id: Option<String>,
    pub messages: Vec<StoredMessage>,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    pub role: String,
    pub content: String,
}

pub struct LlmClient {
    runtime: RuntimeConfig,
    config: LlmConfig,
    http: reqwest::Client,
    store: LlmSessionStore,
}

pub struct LlmSessionStore {
    path: std::path::PathBuf,
    sessions: tokio::sync::Mutex<HashMap<String, StoredSession>>,
}

struct ProviderPreset {
    id: &'static str,
    label: &'static str,
    binary: &'static str,
    install_command: &'static str,
    setup_hint: &'static str,
    supports_native_sessions: bool,
}

struct CommandOutput {
    stdout: String,
    stderr: String,
    status: i32,
}

impl LlmClient {
    pub async fn from_config(runtime: RuntimeConfig, config: LlmConfig) -> Result<Self> {
        let store = LlmSessionStore::load(runtime.llm_sessions_file.clone()).await?;
        Ok(Self {
            runtime,
            config,
            http: reqwest::Client::new(),
            store,
        })
    }

    pub async fn ask(&self, transport_session_id: &str, prompt: &str) -> Result<LlmReply> {
        tracing::debug!(session = transport_session_id, provider = %self.config.provider, "user prompt: {prompt}");
        let mut session = self
            .store
            .get(transport_session_id)
            .await
            .unwrap_or_else(|| StoredSession {
                transport_session_id: transport_session_id.to_string(),
                provider: self.config.provider.clone(),
                provider_session_id: None,
                messages: Vec::new(),
                updated_at_unix_ms: crate::audit::now_unix_ms() as i64,
            });
        let provider_matches = session.provider == self.config.provider;
        if !provider_matches {
            session.provider_session_id = None;
        }

        let reply = tokio::time::timeout(Duration::from_secs(self.runtime.timeout_secs), async {
            match self.config.provider.as_str() {
                "custom-api" => self.ask_custom_api(&session, prompt).await,
                "claude" => self.ask_claude(&session, prompt, provider_matches).await,
                "codex" => self.ask_codex(&session, prompt, provider_matches).await,
                "opencode" => self.ask_opencode(&session, prompt, provider_matches).await,
                "openclaw" => self.ask_openclaw(&session, prompt, provider_matches).await,
                "hermes" => self.ask_hermes(&session, prompt, provider_matches).await,
                other => bail!("unsupported llm provider: {other}"),
            }
        })
        .await
        .with_context(|| {
            format!(
                "{} timed out after {}s",
                self.config.provider, self.runtime.timeout_secs
            )
        })??;

        tracing::debug!(session = transport_session_id, "llm reply: {}", reply.text);
        session.provider = self.config.provider.clone();
        session.provider_session_id = reply.provider_session_id.clone();
        push_message(&mut session.messages, "user", prompt);
        push_message(&mut session.messages, "assistant", &reply.text);
        session.updated_at_unix_ms = crate::audit::now_unix_ms() as i64;
        self.store.upsert(session).await?;

        Ok(reply)
    }

    pub async fn ask_streaming<F, Fut>(
        &self,
        transport_session_id: &str,
        prompt: &str,
        extra_system: Option<&str>,
        approval_rx: mpsc::UnboundedReceiver<LlmApprovalDecision>,
        mut on_update: F,
    ) -> Result<LlmReply>
    where
        F: FnMut(LlmStreamEvent) -> Fut,
        Fut: Future<Output = ()>,
    {
        tracing::debug!(session = transport_session_id, provider = %self.config.provider, "user prompt: {prompt}");
        let mut session = self
            .store
            .get(transport_session_id)
            .await
            .unwrap_or_else(|| StoredSession {
                transport_session_id: transport_session_id.to_string(),
                provider: self.config.provider.clone(),
                provider_session_id: None,
                messages: Vec::new(),
                updated_at_unix_ms: crate::audit::now_unix_ms() as i64,
            });
        let provider_matches = session.provider == self.config.provider;
        if !provider_matches {
            session.provider_session_id = None;
        }

        let reply = tokio::time::timeout(Duration::from_secs(self.runtime.timeout_secs), async {
            match self.config.provider.as_str() {
                "claude" => {
                    self.ask_claude_streaming(
                        &session,
                        prompt,
                        extra_system,
                        provider_matches,
                        approval_rx,
                        &mut on_update,
                    )
                    .await
                }
                "codex" => {
                    self.ask_codex_streaming(
                        &session,
                        prompt,
                        extra_system,
                        provider_matches,
                        approval_rx,
                        &mut on_update,
                    )
                    .await
                }
                "custom-api" => self.ask_custom_api(&session, prompt).await,
                "opencode" => self.ask_opencode(&session, prompt, provider_matches).await,
                "openclaw" => self.ask_openclaw(&session, prompt, provider_matches).await,
                "hermes" => self.ask_hermes(&session, prompt, provider_matches).await,
                other => bail!("unsupported llm provider: {other}"),
            }
        })
        .await
        .with_context(|| {
            format!(
                "{} timed out after {}s",
                self.config.provider, self.runtime.timeout_secs
            )
        })??;

        tracing::debug!(session = transport_session_id, "llm reply: {}", reply.text);
        session.provider = self.config.provider.clone();
        session.provider_session_id = reply.provider_session_id.clone();
        push_message(&mut session.messages, "user", prompt);
        push_message(&mut session.messages, "assistant", &reply.text);
        session.updated_at_unix_ms = crate::audit::now_unix_ms() as i64;
        self.store.upsert(session).await?;

        Ok(reply)
    }

    pub async fn reset_session(&self, transport_session_id: &str) -> Result<bool> {
        self.store.reset(transport_session_id).await
    }

    pub async fn message_count(&self, transport_session_id: &str) -> usize {
        self.store
            .get(transport_session_id)
            .await
            .map(|session| session.messages.len())
            .unwrap_or(0)
    }

    pub async fn suggest_thread_title(&self, transport_session_id: &str) -> Result<Option<String>> {
        let Some(session) = self.store.get(transport_session_id).await else {
            return Ok(None);
        };

        Ok(generate_title_from_messages(&session.messages))
    }

    pub fn supports_streaming(&self) -> bool {
        matches!(self.config.provider.as_str(), "codex" | "claude")
    }

    pub fn provider_label(&self) -> String {
        provider_presets()
            .into_iter()
            .find(|preset| preset.id == self.config.provider)
            .map(|preset| preset.label.to_string())
            .unwrap_or_else(|| "Custom Model API".to_string())
    }

    async fn ask_custom_api(&self, session: &StoredSession, prompt: &str) -> Result<LlmReply> {
        let api_url = self
            .config
            .api_url
            .as_deref()
            .context("custom-api provider requires `api_url` in config")?;
        let model = self
            .config
            .model
            .as_deref()
            .context("custom-api provider requires `model` in config")?;

        let mut messages = history_messages(session);
        messages.push(serde_json::json!({
            "role": "user",
            "content": prompt,
        }));

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(api_key) = self.config.api_key.as_deref() {
            let value = format!("Bearer {api_key}");
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&value).context("invalid api key header")?,
            );
        }

        let response = self
            .http
            .post(api_url)
            .headers(headers)
            .json(&serde_json::json!({
                "model": model,
                "messages": messages,
                "stream": false
            }))
            .send()
            .await?
            .error_for_status()?;
        let body: Value = response.json().await?;
        let text = body
            .pointer("/choices/0/message/content")
            .and_then(value_to_text)
            .or_else(|| body.pointer("/output_text").and_then(value_to_text))
            .context("custom api response did not contain assistant text")?;

        Ok(LlmReply {
            provider: "custom-api".to_string(),
            provider_session_id: Some(session.transport_session_id.clone()),
            text,
        })
    }

    async fn ask_claude(
        &self,
        session: &StoredSession,
        prompt: &str,
        resume_existing: bool,
    ) -> Result<LlmReply> {
        let session_id = session.provider_session_id.clone();
        let mut args = vec![
            "-p".to_string(),
            "--output-format".to_string(),
            "text".to_string(),
        ];
        if let Some(session_id) = session_id.as_deref() {
            args.push("--resume".to_string());
            args.push(session_id.to_string());
        } else {
            let new_session_id = Uuid::new_v4().to_string();
            args.push("--session-id".to_string());
            args.push(new_session_id.clone());
        }
        if let Some(model) = self.config.model.as_deref() {
            args.push("--model".to_string());
            args.push(model.to_string());
        }
        args.push(prompt_for_native_session(session, prompt, resume_existing));

        let provider_session_id = session_id.or_else(|| {
            args.windows(2)
                .find(|window| window[0] == "--session-id")
                .map(|window| window[1].clone())
        });

        let output = run_command("claude", &args, &self.runtime.cwd).await?;
        ensure_success("claude", &output)?;

        Ok(LlmReply {
            provider: "claude".to_string(),
            provider_session_id,
            text: final_text_from_plain_output(&output.stdout)?,
        })
    }

    async fn ask_codex(
        &self,
        session: &StoredSession,
        prompt: &str,
        resume_existing: bool,
    ) -> Result<LlmReply> {
        let mut args = vec![
            "exec".to_string(),
            "--json".to_string(),
            "--skip-git-repo-check".to_string(),
        ];
        if let Some(model) = self.config.model.as_deref() {
            args.push("--model".to_string());
            args.push(model.to_string());
        }

        let rendered_prompt = prompt_for_native_session(session, prompt, resume_existing);
        if let Some(session_id) = session.provider_session_id.as_deref() {
            args.push("resume".to_string());
            args.push(session_id.to_string());
            args.push(rendered_prompt);
        } else {
            args.push(rendered_prompt);
        }

        let output = run_command("codex", &args, &self.runtime.cwd).await?;
        ensure_success("codex", &output)?;
        append_codex_event_log(
            &self.runtime,
            &session.transport_session_id,
            session.provider_session_id.as_deref(),
            &output.stdout,
        )?;
        let parsed = parse_codex_output(&output.stdout);

        Ok(LlmReply {
            provider: "codex".to_string(),
            provider_session_id: parsed
                .session_id
                .or_else(|| session.provider_session_id.clone()),
            text: parsed
                .text
                .or_else(|| fallback_text(&output.stdout))
                .context("codex response did not include assistant text")?,
        })
    }

    async fn ask_claude_streaming<F, Fut>(
        &self,
        session: &StoredSession,
        prompt: &str,
        extra_system: Option<&str>,
        resume_existing: bool,
        mut approval_rx: mpsc::UnboundedReceiver<LlmApprovalDecision>,
        on_update: &mut F,
    ) -> Result<LlmReply>
    where
        F: FnMut(LlmStreamEvent) -> Fut,
        Fut: Future<Output = ()>,
    {
        ensure_permission_runtime_dirs(&self.runtime.state_dir).await?;
        let session_id = session.provider_session_id.clone();
        let claude_run_id = Uuid::new_v4().to_string();
        let mut args = vec![
            "-p".to_string(),
            "--verbose".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--include-partial-messages".to_string(),
        ];
        let current_exe =
            std::env::current_exe().context("failed to resolve current executable")?;
        let hook_command = format!(
            "{} claude-permission-hook --state-dir {} --transport-session-id {} --run-id {}",
            shell_quote(current_exe.to_string_lossy().as_ref()),
            shell_quote(self.runtime.state_dir.display().to_string().as_str()),
            shell_quote(session.transport_session_id.as_str()),
            shell_quote(claude_run_id.as_str()),
        );
        let settings = serde_json::json!({
            "permissions": {
                "allow": ["Bash(*)", "WebSearch(*)", "WebFetch(*)"],
                "defaultMode": "acceptEdits"
            },
            "hooks": {
                "PreToolUse": [
                    {
                        "matcher": "Bash",
                        "hooks": [{"type": "command", "command": hook_command}]
                    }
                ],
                "PostToolUse": [{
                    "hooks": [{
                        "type": "command",
                        "command": hook_command,
                    }]
                }]
            }
        });
        let hook_command_log = settings["hooks"]["PreToolUse"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        tracing::debug!(
            session = %session.transport_session_id,
            provider = "claude",
            run_id = %claude_run_id,
            current_exe = %current_exe.display(),
            hook_command = %hook_command_log,
            "starting claude streaming request with hook-based permission bridge"
        );
        args.push("--settings".to_string());
        args.push(settings.to_string());
        if let Some(session_id) = session_id.as_deref() {
            args.push("--resume".to_string());
            args.push(session_id.to_string());
        } else {
            let new_session_id = Uuid::new_v4().to_string();
            args.push("--session-id".to_string());
            args.push(new_session_id.clone());
        }
        if let Some(model) = self.config.model.as_deref() {
            args.push("--model".to_string());
            args.push(model.to_string());
        }
        if let Some(system) = extra_system {
            args.push("--append-system-prompt".to_string());
            args.push(system.to_string());
        }
        args.push(prompt_for_native_session(session, prompt, resume_existing));

        let provider_session_id = session_id.or_else(|| {
            args.windows(2)
                .find(|window| window[0] == "--session-id")
                .map(|window| window[1].clone())
        });

        let mut child = Command::new("claude")
            .args(&args)
            .current_dir(&self.runtime.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| "failed to spawn provider command `claude`")?;

        let stdout = child
            .stdout
            .take()
            .context("claude process did not expose stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("claude process did not expose stderr")?;

        let stderr_task = tokio::spawn(read_to_string(stderr));
        let mut stdout_lines = BufReader::new(stdout).lines();
        let mut raw_stdout = String::new();
        let mut state = ClaudeStreamingState::default();
        let mut pending_approval: Option<ClaudePendingApproval> = None;
        let mut seen_permission_requests: HashSet<String> = HashSet::new();
        let mut seen_status_events: HashSet<String> = HashSet::new();
        let mut permission_poll = tokio::time::interval(Duration::from_millis(200));

        loop {
            tokio::select! {
                maybe_line = stdout_lines.next_line() => {
                    let Some(line) = maybe_line? else {
                        break;
                    };
                    if !raw_stdout.is_empty() {
                        raw_stdout.push('\n');
                    }
                    raw_stdout.push_str(&line);
                    if let Some(text) = state.ingest_line(&line) {
                        on_update(LlmStreamEvent::Content(text)).await;
                    }
                }
                _ = permission_poll.tick() => {
                    if let Some(event) = next_claude_status_event(
                        &self.runtime.state_dir,
                        &claude_run_id,
                        &mut seen_status_events,
                    ).await? {
                        if let Some(text) = state.ingest_external_status(event.summary) {
                            on_update(LlmStreamEvent::Content(text)).await;
                        }
                    }
                    if pending_approval.is_none()
                        && let Some(request) = next_claude_permission_request(
                            &self.runtime.state_dir,
                            &claude_run_id,
                            &mut seen_permission_requests,
                        ).await?
                    {
                        tracing::debug!(
                            session = %session.transport_session_id,
                            provider = "claude",
                            run_id = %claude_run_id,
                            request_id = %request.request_id,
                            tool_name = request.tool_name.as_deref().unwrap_or(""),
                            command = request.as_llm_approval().command.as_deref().unwrap_or(""),
                            "received Claude permission request from hook bridge"
                        );
                        let approval = request.as_llm_approval();
                        pending_approval = Some(ClaudePendingApproval {
                            request_id: request.request_id.clone(),
                        });
                        on_update(LlmStreamEvent::ApprovalRequested(approval)).await;
                    }
                }
                Some(decision) = approval_rx.recv(), if pending_approval.is_some() => {
                    let Some(pending) = pending_approval.as_ref() else {
                        continue;
                    };
                    if decision.request_id != pending.request_id {
                        continue;
                    }
                    tracing::debug!(
                        session = %session.transport_session_id,
                        provider = "claude",
                        run_id = %claude_run_id,
                        request_id = %decision.request_id,
                        choice = ?decision.choice,
                        "writing Claude permission decision"
                    );
                    write_claude_permission_decision(&self.runtime.state_dir, &decision).await?;
                    pending_approval = None;
                    on_update(LlmStreamEvent::ApprovalResolved {
                        request_id: decision.request_id,
                    }).await;
                }
            }
        }

        let status = child.wait().await?;
        let stderr = stderr_task
            .await
            .context("failed to join claude stderr reader")??;
        let output = CommandOutput {
            stdout: raw_stdout,
            stderr,
            status: status.code().unwrap_or(1),
        };
        ensure_success("claude", &output)?;

        Ok(LlmReply {
            provider: "claude".to_string(),
            provider_session_id: state.session_id.clone().or(provider_session_id),
            text: state
                .finish_text()
                .or_else(|| parse_json_stream(&output.stdout).text)
                .context("claude response did not include assistant text")?,
        })
    }

    async fn ask_codex_streaming<F, Fut>(
        &self,
        session: &StoredSession,
        prompt: &str,
        extra_system: Option<&str>,
        resume_existing: bool,
        mut approval_rx: mpsc::UnboundedReceiver<LlmApprovalDecision>,
        on_update: &mut F,
    ) -> Result<LlmReply>
    where
        F: FnMut(LlmStreamEvent) -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut child = Command::new("codex")
            .args(["app-server", "--listen", "stdio://"])
            .current_dir(&self.runtime.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| "failed to spawn provider command `codex`")?;

        let mut stdin = child
            .stdin
            .take()
            .context("codex process did not expose stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("codex process did not expose stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("codex process did not expose stderr")?;

        let stderr_task = tokio::spawn(read_to_string(stderr));
        let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        let _ = stdout_tx.send(Ok(line));
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = stdout_tx.send(Err(error.to_string()));
                        break;
                    }
                }
            }
        });

        let mut raw_stdout = String::new();
        let mut parsed = CodexAppServerStreamingState::default();
        let idle_deadline = Duration::from_secs(CODEX_STREAM_IDLE_NOTICE_SECS);
        let idle_sleep = tokio::time::sleep(idle_deadline);
        tokio::pin!(idle_sleep);
        let mut next_request_id = 1_u64;

        send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "initialize",
            serde_json::json!({
                "clientInfo": {
                    "name": "edgeai",
                    "title": "edgeai",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true,
                }
            }),
        )
        .await?;
        wait_for_json_rpc_response(
            &self.runtime,
            &session.transport_session_id,
            session.provider_session_id.as_deref(),
            &mut stdout_rx,
            &mut raw_stdout,
            next_request_id - 1,
        )
        .await?;

        let thread_request_id = if let Some(thread_id) = session.provider_session_id.as_deref() {
            send_json_rpc_request(
                &mut stdin,
                &mut next_request_id,
                "thread/resume",
                serde_json::json!({
                    "threadId": thread_id,
                    "developerInstructions": extra_system,
                    "persistExtendedHistory": false,
                }),
            )
            .await?
        } else {
            send_json_rpc_request(
                &mut stdin,
                &mut next_request_id,
                "thread/start",
                serde_json::json!({
                    "model": self.config.model,
                    "cwd": self.runtime.cwd.display().to_string(),
                    "approvalPolicy": "on-request",
                    "approvalsReviewer": "user",
                    "sandbox": "workspace-write",
                    "developerInstructions": extra_system,
                    "experimentalRawEvents": false,
                    "persistExtendedHistory": false,
                }),
            )
            .await?
        };
        let thread_response = wait_for_json_rpc_response(
            &self.runtime,
            &session.transport_session_id,
            session.provider_session_id.as_deref(),
            &mut stdout_rx,
            &mut raw_stdout,
            thread_request_id,
        )
        .await?;
        if parsed.session_id.is_none() {
            parsed.session_id = thread_response
                .pointer("/thread/id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| {
                    thread_response
                        .pointer("/thread/threadId")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                })
                .or_else(|| session.provider_session_id.clone());
        }

        let rendered_prompt = prompt_for_native_session(session, prompt, resume_existing);
        let turn_request_id = send_json_rpc_request(
            &mut stdin,
            &mut next_request_id,
            "turn/start",
            serde_json::json!({
                "threadId": parsed
                    .session_id
                    .as_deref()
                    .or(session.provider_session_id.as_deref())
                    .context("codex app-server did not provide a thread id")?,
                "input": [{
                    "type": "text",
                    "text": rendered_prompt,
                    "text_elements": [],
                }],
            }),
        )
        .await?;

        let mut pending_approval: Option<CodexPendingApproval> = None;
        let mut turn_finished = false;

        loop {
            tokio::select! {
                maybe_line = stdout_rx.recv() => {
                    let Some(line) = maybe_line else {
                        break;
                    };
                    let line = line.map_err(anyhow::Error::msg)?;
                    idle_sleep.as_mut().reset(tokio::time::Instant::now() + idle_deadline);
                    if !raw_stdout.is_empty() {
                        raw_stdout.push('\n');
                    }
                    raw_stdout.push_str(&line);
                    append_codex_event_log_line(
                        &self.runtime,
                        &session.transport_session_id,
                        parsed.session_id.as_deref().or(session.provider_session_id.as_deref()),
                        &line,
                    )?;

                    let value = serde_json::from_str::<Value>(&line).unwrap_or_else(|_| serde_json::json!({ "raw_line": line }));
                    if let Some(response_id) = value.get("id").and_then(jsonrpc_id_to_string) {
                        if response_id == turn_request_id.to_string() {
                            if let Some(error) = value.get("error") {
                                let message = error
                                    .get("message")
                                    .and_then(Value::as_str)
                                    .unwrap_or("codex turn/start failed");
                                bail!("{message}");
                            }
                            continue;
                        }
                    }

                    match parsed.ingest_value(&value) {
                        CodexAppServerEvent::None => {}
                        CodexAppServerEvent::Content(text) => {
                            on_update(LlmStreamEvent::Content(text)).await;
                        }
                        CodexAppServerEvent::ApprovalRequested(request) => {
                            let auto_approve = if let Some(cmd) = request.command.as_deref() {
                                let dynamic_allow = load_always_allow_commands(&self.runtime.state_dir).await;
                                is_auto_allowed_bash_command(cmd, &dynamic_allow)
                            } else {
                                false
                            };
                            if auto_approve {
                                send_json_rpc_response(
                                    &mut stdin,
                                    &request.request_id,
                                    approval_result_json(&LlmApprovalChoice::Accept, &request.kind),
                                )
                                .await?;
                            } else {
                                pending_approval = Some(CodexPendingApproval {
                                    request_id: request.request_id.clone(),
                                    kind: request.kind.clone(),
                                });
                                on_update(LlmStreamEvent::ApprovalRequested(request)).await;
                            }
                        }
                        CodexAppServerEvent::ApprovalResolved { request_id } => {
                            pending_approval = None;
                            on_update(LlmStreamEvent::ApprovalResolved { request_id }).await;
                        }
                        CodexAppServerEvent::TurnCompleted => {
                            turn_finished = true;
                            break;
                        }
                        CodexAppServerEvent::TurnFailed(message) => {
                            bail!("{message}");
                        }
                    }
                }
                Some(decision) = approval_rx.recv(), if pending_approval.is_some() => {
                    let Some(pending) = pending_approval.as_ref() else {
                        continue;
                    };
                    if decision.request_id != pending.request_id {
                        continue;
                    }

                    send_json_rpc_response(
                        &mut stdin,
                        &decision.request_id,
                        approval_result_json(&decision.choice, &pending.kind),
                    )
                    .await?;
                }
                _ = &mut idle_sleep => {
                    if let Some(text) = parsed.mark_idle_waiting() {
                        on_update(LlmStreamEvent::Content(text)).await;
                    }
                    idle_sleep.as_mut().reset(tokio::time::Instant::now() + idle_deadline);
                }
            }
        }

        let text = parsed
            .finish_text()
            .or_else(|| fallback_text(&raw_stdout))
            .context("codex response did not include assistant text")?;
        if turn_finished {
            stderr_task.abort();
            return Ok(LlmReply {
                provider: "codex".to_string(),
                provider_session_id: parsed
                    .session_id
                    .clone()
                    .or_else(|| session.provider_session_id.clone()),
                text,
            });
        }

        let status = child.wait().await?;
        let stderr = stderr_task
            .await
            .context("failed to join codex stderr reader")??;
        let output = CommandOutput {
            stdout: raw_stdout,
            stderr,
            status: status.code().unwrap_or(1),
        };
        ensure_success("codex", &output)?;

        Ok(LlmReply {
            provider: "codex".to_string(),
            provider_session_id: parsed
                .session_id
                .clone()
                .or_else(|| session.provider_session_id.clone()),
            text,
        })
    }

    async fn ask_opencode(
        &self,
        session: &StoredSession,
        prompt: &str,
        resume_existing: bool,
    ) -> Result<LlmReply> {
        let mut args = vec![
            "run".to_string(),
            "--format".to_string(),
            "json".to_string(),
            "--dir".to_string(),
            self.runtime.cwd.display().to_string(),
        ];
        if let Some(session_id) = session.provider_session_id.as_deref() {
            args.push("--session".to_string());
            args.push(session_id.to_string());
        }
        if let Some(model) = self.config.model.as_deref() {
            args.push("--model".to_string());
            args.push(model.to_string());
        }
        args.push(prompt_for_native_session(session, prompt, resume_existing));

        let output = run_command("opencode", &args, &self.runtime.cwd).await?;
        ensure_success("opencode", &output)?;
        let parsed = parse_json_stream(&output.stdout);

        Ok(LlmReply {
            provider: "opencode".to_string(),
            provider_session_id: parsed
                .session_id
                .or_else(|| session.provider_session_id.clone()),
            text: parsed
                .text
                .or_else(|| fallback_text(&output.stdout))
                .context("opencode response did not include assistant text")?,
        })
    }

    async fn ask_openclaw(
        &self,
        session: &StoredSession,
        prompt: &str,
        resume_existing: bool,
    ) -> Result<LlmReply> {
        let mut args = vec![
            "agent".to_string(),
            "--json".to_string(),
            "--message".to_string(),
            prompt_for_native_session(session, prompt, resume_existing),
        ];
        if let Some(session_id) = session.provider_session_id.as_deref() {
            args.push("--session-id".to_string());
            args.push(session_id.to_string());
        }
        if let Some(model) = self.config.model.as_deref() {
            args.push("--model".to_string());
            args.push(model.to_string());
        }

        let output = run_command("openclaw", &args, &self.runtime.cwd).await?;
        ensure_success("openclaw", &output)?;
        let parsed = parse_json_stream(&output.stdout);

        Ok(LlmReply {
            provider: "openclaw".to_string(),
            provider_session_id: parsed
                .session_id
                .or_else(|| session.provider_session_id.clone()),
            text: parsed
                .text
                .or_else(|| fallback_text(&output.stdout))
                .context("openclaw response did not include assistant text")?,
        })
    }

    async fn ask_hermes(
        &self,
        session: &StoredSession,
        prompt: &str,
        resume_existing: bool,
    ) -> Result<LlmReply> {
        let mut args = vec![
            "chat".to_string(),
            "-q".to_string(),
            prompt_for_native_session(session, prompt, resume_existing),
            "-Q".to_string(),
        ];
        if let Some(session_id) = session.provider_session_id.as_deref() {
            args.push("--resume".to_string());
            args.push(session_id.to_string());
        }
        if let Some(model) = self.config.model.as_deref() {
            args.push("--model".to_string());
            args.push(model.to_string());
        }

        let output = run_command("hermes", &args, &self.runtime.cwd).await?;
        ensure_success("hermes", &output)?;

        Ok(LlmReply {
            provider: "hermes".to_string(),
            provider_session_id: parse_session_id_from_text(&output.stdout)
                .or_else(|| session.provider_session_id.clone()),
            text: final_text_from_plain_output(&output.stdout)?,
        })
    }
}

impl LlmSessionStore {
    pub async fn load(path: std::path::PathBuf) -> Result<Self> {
        let sessions = load_sessions_file(&path).await?;
        Ok(Self {
            path,
            sessions: tokio::sync::Mutex::new(sessions),
        })
    }

    pub async fn get(&self, transport_session_id: &str) -> Option<StoredSession> {
        self.sessions
            .lock()
            .await
            .get(transport_session_id)
            .cloned()
    }

    pub async fn upsert(&self, session: StoredSession) -> Result<()> {
        let mut sessions = self.sessions.lock().await;
        sessions.insert(session.transport_session_id.clone(), session);
        save_sessions_file(&self.path, &sessions).await
    }

    pub async fn reset(&self, transport_session_id: &str) -> Result<bool> {
        let mut sessions = self.sessions.lock().await;
        let removed = sessions.remove(transport_session_id).is_some();
        save_sessions_file(&self.path, &sessions).await?;
        Ok(removed)
    }
}

pub fn detected_provider_options() -> Vec<ProviderOption> {
    provider_presets()
        .into_iter()
        .map(|preset| {
            let installed = binary_exists(preset.binary);
            ProviderOption {
                id: preset.id.to_string(),
                label: preset.label.to_string(),
                binary: Some(preset.binary.to_string()),
                installed,
                install_command: Some(preset.install_command.to_string()),
                supports_native_sessions: preset.supports_native_sessions,
                setup_hint: Some(preset.setup_hint.to_string()),
            }
        })
        .collect()
}

pub fn provider_option_for(id: &str) -> Option<ProviderOption> {
    if id == "custom-api" {
        return Some(ProviderOption {
            id: "custom-api".to_string(),
            label: "Configure Model API".to_string(),
            binary: None,
            installed: true,
            install_command: None,
            supports_native_sessions: false,
            setup_hint: Some("Requires a compatible OpenAI Chat Completions API endpoint URL".to_string()),
        });
    }

    detected_provider_options()
        .into_iter()
        .find(|option| option.id == id)
}

pub async fn install_provider(option: &ProviderOption) -> Result<()> {
    let command = option
        .install_command
        .as_deref()
        .context("selected provider does not define an install command")?;
    let output = run_command(
        "/bin/sh",
        &["-lc".to_string(), command.to_string()],
        &std::env::current_dir()?,
    )
    .await?;
    ensure_success(&option.label, &output)
}

fn provider_presets() -> Vec<ProviderPreset> {
    vec![
        ProviderPreset {
            id: "claude",
            label: "Claude Code",
            binary: "claude",
            install_command: "curl -fsSL https://claude.ai/install.sh | bash",
            setup_hint: "After installation, usually run `claude auth login` or complete the Claude Code login",
            supports_native_sessions: true,
        },
        ProviderPreset {
            id: "codex",
            label: "Codex CLI",
            binary: "codex",
            install_command: "npm install -g @openai/codex",
            setup_hint: "After installation, run `codex login` to complete authentication",
            supports_native_sessions: true,
        },
    ]
}

fn binary_exists(binary: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| {
            std::env::split_paths(&paths)
                .map(|dir| dir.join(binary))
                .any(|path| path.exists())
        })
        .unwrap_or(false)
}

async fn run_command(program: &str, args: &[String], cwd: &Path) -> Result<CommandOutput> {
    let output = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .with_context(|| format!("failed to spawn provider command `{program}`"))?;

    Ok(CommandOutput {
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        status: output.status.code().unwrap_or(1),
    })
}

fn ensure_success(name: &str, output: &CommandOutput) -> Result<()> {
    if output.status == 0 {
        Ok(())
    } else if output.stderr.is_empty() {
        bail!("{name} exited with code {}", output.status)
    } else {
        bail!(
            "{name} exited with code {}: {}",
            output.status,
            output.stderr
        )
    }
}

fn final_text_from_plain_output(output: &str) -> Result<String> {
    let text = output.trim();
    if text.is_empty() {
        bail!("provider returned empty output")
    } else {
        Ok(text.to_string())
    }
}

fn render_prompt_with_history(session: &StoredSession, prompt: &str) -> String {
    if session.messages.is_empty() {
        return prompt.to_string();
    }

    let mut body = String::from("Continue this existing chat. Recent transcript:\n");
    for message in session
        .messages
        .iter()
        .skip(session.messages.len().saturating_sub(MAX_HISTORY_MESSAGES))
    {
        body.push_str(&format!("{}: {}\n", message.role, message.content));
    }
    body.push_str("\nNew user message:\n");
    body.push_str(prompt);
    body
}

fn prompt_for_native_session(
    session: &StoredSession,
    prompt: &str,
    resume_existing: bool,
) -> String {
    if resume_existing && session.provider_session_id.is_some() {
        prompt.to_string()
    } else {
        render_prompt_with_history(session, prompt)
    }
}

fn history_messages(session: &StoredSession) -> Vec<Value> {
    session
        .messages
        .iter()
        .skip(session.messages.len().saturating_sub(MAX_HISTORY_MESSAGES))
        .map(|message| {
            serde_json::json!({
                "role": message.role,
                "content": message.content,
            })
        })
        .collect()
}

fn push_message(messages: &mut Vec<StoredMessage>, role: &str, content: &str) {
    messages.push(StoredMessage {
        role: role.to_string(),
        content: content.to_string(),
    });
    if messages.len() > MAX_HISTORY_MESSAGES {
        let overflow = messages.len() - MAX_HISTORY_MESSAGES;
        messages.drain(0..overflow);
    }
}

fn append_codex_event_log(
    runtime: &RuntimeConfig,
    transport_session_id: &str,
    provider_session_id: Option<&str>,
    output: &str,
) -> Result<()> {
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        append_codex_event_log_line(runtime, transport_session_id, provider_session_id, line)?;
    }
    Ok(())
}

fn append_codex_event_log_line(
    runtime: &RuntimeConfig,
    transport_session_id: &str,
    provider_session_id: Option<&str>,
    line: &str,
) -> Result<()> {
    let path = runtime.state_dir.join("codex-events.jsonl");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let payload = match serde_json::from_str::<Value>(line) {
        Ok(value) => serde_json::json!({
            "ts_unix_ms": crate::audit::now_unix_ms(),
            "transport_session_id": transport_session_id,
            "provider_session_id": provider_session_id,
            "event": value,
        }),
        Err(_) => serde_json::json!({
            "ts_unix_ms": crate::audit::now_unix_ms(),
            "transport_session_id": transport_session_id,
            "provider_session_id": provider_session_id,
            "raw_line": line,
        }),
    };

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, &payload)?;
    file.write_all(b"\n")?;
    Ok(())
}

async fn load_sessions_file(path: &Path) -> Result<HashMap<String, StoredSession>> {
    match tokio::fs::read(path).await {
        Ok(body) => Ok(serde_json::from_slice(&body)
            .with_context(|| format!("failed to parse session store {}", path.display()))?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(error) => Err(error.into()),
    }
}

async fn save_sessions_file(path: &Path, sessions: &HashMap<String, StoredSession>) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, serde_json::to_vec_pretty(sessions)?).await?;
    Ok(())
}

struct ParsedJsonStream {
    text: Option<String>,
    session_id: Option<String>,
}

#[derive(Default)]
struct ClaudeStreamingState {
    display_blocks: Vec<String>,
    assistant_blocks: Vec<String>,
    latest_status: Option<String>,
    session_id: Option<String>,
    partial_text: Option<String>,
}

struct ClaudePendingApproval {
    request_id: String,
}

impl ClaudePermissionRequest {
    fn as_llm_approval(&self) -> LlmApprovalRequest {
        let command = self
            .tool_input
            .get("tool_input")
            .or_else(|| self.tool_input.get("toolInput"))
            .and_then(extract_command_from_permission_input)
            .or_else(|| extract_command_from_permission_input(&self.tool_input));
        let reason = self
            .tool_input
            .get("message")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| {
                self.tool_input
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .or_else(|| {
                self.tool_name
                    .as_ref()
                    .map(|tool_name| format!("Claude requested permission for {tool_name}"))
            });

        LlmApprovalRequest {
            request_id: self.request_id.clone(),
            kind: if command.is_some() {
                LlmApprovalKind::ExecCommand
            } else {
                LlmApprovalKind::Permissions
            },
            command,
            reason,
            allow_accept_for_session: self
                .permission_suggestions
                .as_ref()
                .and_then(Value::as_array)
                .map(|suggestions| {
                    suggestions.iter().any(|suggestion| {
                        suggestion
                            .get("destination")
                            .and_then(Value::as_str)
                            .map(|destination| destination == "session")
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false),
            allow_cancel: true,
        }
    }
}

#[derive(Default)]
struct CodexAppServerStreamingState {
    display_blocks: Vec<String>,
    assistant_blocks: Vec<String>,
    partial_agent_messages: HashMap<String, String>,
    latest_status: Option<String>,
    session_id: Option<String>,
}

enum CodexAppServerEvent {
    None,
    Content(String),
    ApprovalRequested(LlmApprovalRequest),
    ApprovalResolved { request_id: String },
    TurnCompleted,
    TurnFailed(String),
}

struct CodexPendingApproval {
    request_id: String,
    kind: LlmApprovalKind,
}

impl CodexAppServerStreamingState {
    fn ingest_value(&mut self, value: &Value) -> CodexAppServerEvent {
        let Some(map) = value.as_object() else {
            return CodexAppServerEvent::None;
        };

        if map.get("id").is_some() && map.get("method").is_some() {
            return self.ingest_app_server_request(value);
        }

        if let Some(method) = map.get("method").and_then(Value::as_str) {
            let params = map.get("params").unwrap_or(&Value::Null);
            return match method {
                "thread/started" => {
                    if self.session_id.is_none() {
                        self.session_id = params
                            .pointer("/thread/id")
                            .and_then(Value::as_str)
                            .map(ToString::to_string);
                    }
                    CodexAppServerEvent::None
                }
                "item/agentMessage/delta" => {
                    let item_id = params
                        .get("itemId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let delta = params
                        .get("delta")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if item_id.is_empty() || delta.is_empty() {
                        CodexAppServerEvent::None
                    } else {
                        let entry = self.partial_agent_messages.entry(item_id).or_default();
                        entry.push_str(delta);
                        self.render_display_text()
                            .map(CodexAppServerEvent::Content)
                            .unwrap_or(CodexAppServerEvent::None)
                    }
                }
                "item/started" | "item/completed" => self
                    .ingest_app_server_item(params.get("item").unwrap_or(&Value::Null), method)
                    .map(CodexAppServerEvent::Content)
                    .unwrap_or(CodexAppServerEvent::None),
                "serverRequest/resolved" => {
                    let request_id = params
                        .get("requestId")
                        .and_then(jsonrpc_id_to_string)
                        .unwrap_or_default();
                    if !request_id.is_empty() {
                        self.latest_status = None;
                        CodexAppServerEvent::ApprovalResolved { request_id }
                    } else {
                        CodexAppServerEvent::None
                    }
                }
                "turn/completed" => {
                    let status = params
                        .pointer("/turn/status")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    match status {
                        "completed" | "interrupted" => CodexAppServerEvent::TurnCompleted,
                        "failed" => {
                            let message = params
                                .pointer("/turn/error/message")
                                .and_then(Value::as_str)
                                .unwrap_or("codex turn failed")
                                .to_string();
                            CodexAppServerEvent::TurnFailed(message)
                        }
                        _ => CodexAppServerEvent::None,
                    }
                }
                "error" => {
                    let message = params
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("codex app-server error")
                        .to_string();
                    CodexAppServerEvent::TurnFailed(message)
                }
                _ => CodexAppServerEvent::None,
            };
        }

        CodexAppServerEvent::None
    }

    fn ingest_app_server_request(&mut self, value: &Value) -> CodexAppServerEvent {
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let request_id = value
            .get("id")
            .and_then(jsonrpc_id_to_string)
            .unwrap_or_default();
        let params = value.get("params").unwrap_or(&Value::Null);
        if request_id.is_empty() {
            return CodexAppServerEvent::None;
        }

        let request = match method {
            "item/commandExecution/requestApproval" => Some(LlmApprovalRequest {
                request_id: request_id.clone(),
                kind: LlmApprovalKind::CommandExecution,
                command: params
                    .get("command")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                reason: params
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                allow_accept_for_session: approval_decisions_contain(
                    params.get("availableDecisions"),
                    "acceptForSession",
                ),
                allow_cancel: approval_decisions_contain(
                    params.get("availableDecisions"),
                    "cancel",
                ),
            }),
            "execCommandApproval" => Some(LlmApprovalRequest {
                request_id: request_id.clone(),
                kind: LlmApprovalKind::ExecCommand,
                command: params
                    .get("command")
                    .and_then(Value::as_array)
                    .map(|parts| {
                        parts
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .filter(|command| !command.is_empty()),
                reason: params
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                allow_accept_for_session: true,
                allow_cancel: true,
            }),
            "item/permissions/requestApproval" => Some(LlmApprovalRequest {
                request_id: request_id.clone(),
                kind: LlmApprovalKind::Permissions,
                command: None,
                reason: params
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                allow_accept_for_session: false,
                allow_cancel: true,
            }),
            _ => None,
        };

        if let Some(request) = request {
            self.latest_status = Some(render_approval_summary(&request));
            CodexAppServerEvent::ApprovalRequested(request)
        } else {
            CodexAppServerEvent::None
        }
    }

    fn ingest_app_server_item(&mut self, item: &Value, method: &str) -> Option<String> {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        match item_type {
            "agentMessage" if method == "item/completed" => {
                let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
                let text = item.get("text").and_then(Value::as_str)?.trim().to_string();
                if text.is_empty() {
                    return None;
                }
                self.partial_agent_messages.remove(item_id);
                self.display_blocks.push(text.clone());
                self.assistant_blocks.push(text);
                self.render_display_text()
            }
            "commandExecution" => {
                self.latest_status = Some(render_command_summary(
                    item.get("command")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    match item
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                    {
                        "inProgress" => "running",
                        other => other,
                    },
                    item.get("exitCode").and_then(Value::as_i64),
                ));
                self.render_display_text()
            }
            "webSearch" => {
                self.latest_status = render_web_search_summary(
                    item.get("query")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    "completed",
                    method,
                );
                self.render_display_text()
            }
            _ => None,
        }
    }

    fn mark_idle_waiting(&mut self) -> Option<String> {
        let waiting = "[status] waiting for approval or blocked by sandbox/network";
        if self.latest_status.as_deref() == Some(waiting) {
            None
        } else {
            self.latest_status = Some(waiting.to_string());
            self.render_display_text()
        }
    }

    fn finish_text(&self) -> Option<String> {
        let text = self.assistant_blocks.join("\n\n");
        if text.is_empty() { None } else { Some(text) }
    }

    fn render_display_text(&self) -> Option<String> {
        let mut blocks = self.display_blocks.clone();
        if let Some((_, partial)) = self.partial_agent_messages.iter().last() {
            let partial = partial.trim();
            if !partial.is_empty() {
                blocks.push(partial.to_string());
            }
        }
        if let Some(status) = self.latest_status.as_ref() {
            blocks.push(status.clone());
        }
        let text = blocks.join("\n\n").trim().to_string();
        if text.is_empty() { None } else { Some(text) }
    }
}

impl ClaudeStreamingState {
    fn ingest_line(&mut self, line: &str) -> Option<String> {
        let value = serde_json::from_str::<Value>(line).ok()?;
        if self.session_id.is_none() {
            self.session_id = extract_session_id_from_json(&value);
        }

        let Some(map) = value.as_object() else {
            return None;
        };
        let event_type = map.get("type").and_then(Value::as_str).unwrap_or_default();
        let subtype = map
            .get("subtype")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let content_preview = map
            .get("message")
            .and_then(extract_text_from_json)
            .or_else(|| map.get("content").and_then(extract_text_from_json))
            .or_else(|| map.get("result").and_then(extract_text_from_json))
            .or_else(|| map.get("output_text").and_then(extract_text_from_json))
            .or_else(|| map.get("text").and_then(extract_text_from_json));
        tracing::debug!(
            target: "edgeai::llm::claude_stream",
            event_type,
            subtype,
            has_text = content_preview.is_some(),
            text_len = content_preview.as_ref().map(|text| text.len()).unwrap_or_default(),
            text_preview = content_preview
                .as_deref()
                .map(|text| text.chars().take(120).collect::<String>())
                .unwrap_or_default(),
            "received Claude stream event"
        );

        match event_type {
            "system" => {
                self.latest_status = parse_claude_status_event(map, subtype);
                self.render_display_text()
            }
            "user" => {
                self.latest_status = map
                    .get("message")
                    .and_then(extract_text_from_json)
                    .or_else(|| map.get("content").and_then(extract_text_from_json))
                    .or_else(|| map.get("text").and_then(extract_text_from_json))
                    .and_then(|text| summarize_claude_user_event(&text));
                self.render_display_text()
            }
            "assistant" => self.ingest_assistant_text(
                map.get("message")
                    .and_then(extract_text_from_json)
                    .or_else(|| map.get("content").and_then(extract_text_from_json))
                    .or_else(|| map.get("text").and_then(extract_text_from_json)),
                subtype,
            ),
            "stream_event" => self.ingest_stream_event(map),
            "result" => {
                if let Some(text) = map
                    .get("result")
                    .and_then(extract_text_from_json)
                    .or_else(|| map.get("output_text").and_then(extract_text_from_json))
                    .or_else(|| map.get("text").and_then(extract_text_from_json))
                {
                    self.partial_text = None;
                    if self.assistant_blocks.last() != Some(&text) {
                        self.display_blocks.push(text.clone());
                        self.assistant_blocks.push(text);
                    }
                    self.latest_status = None;
                    self.render_display_text()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn ingest_stream_event(&mut self, map: &serde_json::Map<String, Value>) -> Option<String> {
        let event = map.get("event")?.as_object()?;
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or_default();
        match event_type {
            "content_block_start" => {
                let block = event.get("content_block").unwrap_or(&Value::Null);
                if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("tool");
                    self.latest_status = Some(format!("[tool] {name}"));
                    self.render_display_text()
                } else {
                    None
                }
            }
            "content_block_delta" => {
                let delta = event.get("delta").unwrap_or(&Value::Null);
                if delta.get("type").and_then(Value::as_str) == Some("text_delta") {
                    let text = delta.get("text").and_then(Value::as_str).unwrap_or_default();
                    if !text.is_empty() {
                        self.latest_status = None;
                        self.partial_text
                            .get_or_insert_with(String::new)
                            .push_str(text);
                        self.render_display_text()
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn ingest_assistant_text(&mut self, text: Option<String>, subtype: &str) -> Option<String> {
        let text = text
            .map(|text| text.trim().to_string())
            .filter(|text| !text.is_empty())?;

        if matches!(subtype, "partial" | "delta") {
            self.partial_text = Some(text);
            return self.render_display_text();
        }

        self.partial_text = None;
        if self.assistant_blocks.last() == Some(&text) {
            return None;
        }
        self.display_blocks.push(text.clone());
        self.assistant_blocks.push(text);
        self.latest_status = None;
        self.render_display_text()
    }

    fn finish_text(&self) -> Option<String> {
        let text = self.assistant_blocks.join("\n\n");
        if text.is_empty() { None } else { Some(text) }
    }

    fn ingest_external_status(&mut self, status: String) -> Option<String> {
        self.latest_status = Some(status);
        self.render_display_text()
    }

    fn render_display_text(&self) -> Option<String> {
        let mut blocks = self.display_blocks.clone();
        if let Some(partial) = self.partial_text.as_ref() {
            blocks.push(partial.clone());
        }
        if let Some(status) = self.latest_status.as_ref() {
            blocks.push(status.clone());
        }
        let text = blocks.join("\n\n").trim().to_string();
        if text.is_empty() { None } else { Some(text) }
    }
}

#[derive(Default)]
struct CodexStreamingState {
    display_blocks: Vec<String>,
    assistant_blocks: Vec<String>,
    seen_event_keys: HashSet<String>,
    latest_status: Option<String>,
    session_id: Option<String>,
}

impl CodexStreamingState {
    fn ingest_line(&mut self, line: &str) -> Option<String> {
        let value = serde_json::from_str::<Value>(line).ok()?;
        if self.session_id.is_none() {
            self.session_id = extract_session_id_from_json(&value);
        }

        let event = parse_codex_event(&value)?;
        let event_key = event.key.unwrap_or_else(|| format!("line:{}", line.trim()));
        if !self.seen_event_keys.insert(event_key) {
            return None;
        }

        let mut changed = false;
        if let Some(display_text) = event.display_text.filter(|text| !text.trim().is_empty()) {
            if event.assistant_text.is_some() {
                self.display_blocks.push(display_text);
            } else {
                self.latest_status = Some(display_text);
            }
            changed = true;
        }
        if let Some(assistant_text) = event.assistant_text.filter(|text| !text.trim().is_empty()) {
            self.assistant_blocks.push(assistant_text);
            changed = true;
        }

        if changed {
            self.render_display_text()
        } else {
            None
        }
    }

    fn finish_text(&self) -> Option<String> {
        let text = self.assistant_blocks.join("\n\n");
        if text.is_empty() { None } else { Some(text) }
    }

    fn render_display_text(&self) -> Option<String> {
        let mut blocks = self.display_blocks.clone();
        if let Some(status) = self.latest_status.as_ref() {
            blocks.push(status.clone());
        }
        let text = blocks.join("\n\n").trim().to_string();
        if text.is_empty() { None } else { Some(text) }
    }
}

struct CodexParsedEvent {
    key: Option<String>,
    display_text: Option<String>,
    assistant_text: Option<String>,
}

fn parse_json_stream(output: &str) -> ParsedJsonStream {
    let mut best_text = None;
    let mut session_id = None;

    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            if best_text.is_none() {
                best_text = extract_text_from_json(&value);
            }
            if session_id.is_none() {
                session_id = extract_session_id_from_json(&value);
            }
        }
    }

    ParsedJsonStream {
        text: best_text,
        session_id,
    }
}

fn parse_codex_output(output: &str) -> ParsedJsonStream {
    let mut state = CodexStreamingState::default();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let _ = state.ingest_line(line);
    }

    ParsedJsonStream {
        text: state.finish_text(),
        session_id: state.session_id,
    }
}

async fn read_to_string<R>(reader: R) -> Result<String>
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    let mut output = String::new();
    while let Some(line) = lines.next_line().await? {
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&line);
    }
    Ok(output)
}

async fn write_json_line<W>(writer: &mut W, value: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let line = serde_json::to_string(value)?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn send_json_rpc_request<W>(
    writer: &mut W,
    next_request_id: &mut u64,
    method: &str,
    params: Value,
) -> Result<u64>
where
    W: AsyncWrite + Unpin,
{
    let request_id = *next_request_id;
    *next_request_id += 1;
    write_json_line(
        writer,
        &serde_json::json!({
            "id": request_id,
            "method": method,
            "params": params,
        }),
    )
    .await?;
    Ok(request_id)
}

async fn send_json_rpc_response<W>(writer: &mut W, request_id: &str, result: Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let id = request_id
        .parse::<u64>()
        .map(Value::from)
        .unwrap_or_else(|_| Value::String(request_id.to_string()));
    write_json_line(
        writer,
        &serde_json::json!({
            "id": id,
            "result": result,
        }),
    )
    .await
}

async fn wait_for_json_rpc_response(
    runtime: &RuntimeConfig,
    transport_session_id: &str,
    provider_session_id: Option<&str>,
    stdout_rx: &mut mpsc::UnboundedReceiver<std::result::Result<String, String>>,
    raw_stdout: &mut String,
    request_id: u64,
) -> Result<Value> {
    loop {
        let line = stdout_rx
            .recv()
            .await
            .context("codex app-server closed before responding")?
            .map_err(anyhow::Error::msg)?;
        if !raw_stdout.is_empty() {
            raw_stdout.push('\n');
        }
        raw_stdout.push_str(&line);
        append_codex_event_log_line(runtime, transport_session_id, provider_session_id, &line)?;
        let value: Value = serde_json::from_str(&line)
            .with_context(|| "codex app-server produced invalid JSON")?;
        if value.get("id").and_then(jsonrpc_id_to_string).as_deref()
            == Some(&request_id.to_string())
        {
            if let Some(error) = value.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("codex app-server request failed");
                bail!("{message}");
            }
            return Ok(value.get("result").cloned().unwrap_or(Value::Null));
        }
    }
}

fn jsonrpc_id_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn approval_decisions_contain(value: Option<&Value>, expected: &str) -> bool {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items.iter().any(|item| {
                item.as_str() == Some(expected)
                    || item
                        .as_object()
                        .map(|map| map.contains_key(expected))
                        .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn render_approval_summary(request: &LlmApprovalRequest) -> String {
    let command = request
        .command
        .as_deref()
        .map(|command| truncate_for_status(command, 72));
    let reason = request
        .reason
        .as_deref()
        .map(|reason| truncate_for_status(reason, 72));
    match (command, reason) {
        (Some(command), Some(reason)) => format!("[approval] {command} ({reason})"),
        (Some(command), None) => format!("[approval] {command}"),
        (None, Some(reason)) => format!("[approval] {reason}"),
        (None, None) => "[approval] waiting for a decision".to_string(),
    }
}

fn approval_result_json(choice: &LlmApprovalChoice, kind: &LlmApprovalKind) -> Value {
    match kind {
        LlmApprovalKind::CommandExecution => serde_json::json!({
            "decision": match choice {
                LlmApprovalChoice::Accept | LlmApprovalChoice::AlwaysAllow => Value::String("accept".to_string()),
                LlmApprovalChoice::AcceptForSession => Value::String("acceptForSession".to_string()),
                LlmApprovalChoice::Decline => Value::String("decline".to_string()),
                LlmApprovalChoice::Cancel => Value::String("cancel".to_string()),
            }
        }),
        LlmApprovalKind::ExecCommand => serde_json::json!({
            "decision": match choice {
                LlmApprovalChoice::Accept | LlmApprovalChoice::AlwaysAllow => Value::String("approved".to_string()),
                LlmApprovalChoice::AcceptForSession => Value::String("approved_for_session".to_string()),
                LlmApprovalChoice::Decline => Value::String("denied".to_string()),
                LlmApprovalChoice::Cancel => Value::String("abort".to_string()),
            }
        }),
        LlmApprovalKind::Permissions => match choice {
            LlmApprovalChoice::Accept | LlmApprovalChoice::AcceptForSession | LlmApprovalChoice::AlwaysAllow => serde_json::json!({
                "permissions": {
                    "network": { "enabled": true }
                },
                "scope": if matches!(choice, LlmApprovalChoice::AcceptForSession) { "session" } else { "turn" },
            }),
            LlmApprovalChoice::Decline | LlmApprovalChoice::Cancel => serde_json::json!({
                "permissions": {},
                "scope": "turn",
            }),
        },
    }
}

fn parse_codex_event(value: &Value) -> Option<CodexParsedEvent> {
    let map = value.as_object()?;
    let event_type = map
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| map.get("event").and_then(Value::as_str))
        .unwrap_or_default();

    match event_type {
        "item.completed" => parse_codex_item_event(map, "completed"),
        "item.started" => parse_codex_item_event(map, "started"),
        "thread.started" => Some(CodexParsedEvent {
            key: Some(format!(
                "thread:{}",
                map.get("thread_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
            )),
            display_text: None,
            assistant_text: None,
        }),
        "turn.started" => Some(CodexParsedEvent {
            key: Some("turn.started".to_string()),
            display_text: None,
            assistant_text: None,
        }),
        _ => parse_codex_status_event(map, event_type),
    }
}

fn parse_codex_status_event(
    map: &serde_json::Map<String, Value>,
    event_type: &str,
) -> Option<CodexParsedEvent> {
    let lower = event_type.to_ascii_lowercase();
    if ![
        "approval",
        "approve",
        "blocked",
        "block",
        "permission",
        "sandbox",
        "denied",
        "error",
        "failed",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return None;
    }

    let message = map
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| map.get("reason").and_then(Value::as_str))
        .or_else(|| map.get("error").and_then(Value::as_str))
        .or_else(|| map.get("text").and_then(Value::as_str))
        .unwrap_or(event_type)
        .trim();
    if message.is_empty() {
        return None;
    }

    Some(CodexParsedEvent {
        key: Some(format!("status:{event_type}:{message}")),
        display_text: Some(format!("[status] {}", truncate_for_status(message, 96))),
        assistant_text: None,
    })
}

fn parse_codex_item_event(
    map: &serde_json::Map<String, Value>,
    phase: &str,
) -> Option<CodexParsedEvent> {
    let item = map.get("item")?.as_object()?;
    let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
    let status = item.get("status").and_then(Value::as_str).unwrap_or(phase);

    match item_type {
        "agent_message" if phase == "completed" => {
            let text = item.get("text").and_then(Value::as_str)?.trim().to_string();
            if text.is_empty() {
                None
            } else {
                Some(CodexParsedEvent {
                    key: Some(format!("agent:{item_id}")),
                    display_text: Some(text.clone()),
                    assistant_text: Some(text),
                })
            }
        }
        "command_execution" => {
            let command = item
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let exit_code = item.get("exit_code").and_then(Value::as_i64);
            Some(CodexParsedEvent {
                key: Some(format!("command:{phase}:{item_id}:{status}")),
                display_text: Some(render_command_summary(command, status, exit_code)),
                assistant_text: None,
            })
        }
        "web_search" => {
            let query = item
                .get("query")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let rendered = render_web_search_summary(query, status, phase)?;
            Some(CodexParsedEvent {
                key: Some(format!("search:{phase}:{item_id}:{status}:{query}")),
                display_text: Some(rendered),
                assistant_text: None,
            })
        }
        _ => None,
    }
}

fn render_command_summary(command: &str, status: &str, exit_code: Option<i64>) -> String {
    let snippet = truncate_for_status(command, 72);
    match status {
        "completed" => match exit_code {
            Some(0) => format!("[command] done: {snippet}"),
            Some(code) => format!("[command] failed ({code}): {snippet}"),
            None => format!("[command] done: {snippet}"),
        },
        "failed" => format!("[command] failed: {snippet}"),
        _ => format!("[command] running: {snippet}"),
    }
}

fn render_web_search_summary(query: &str, status: &str, phase: &str) -> Option<String> {
    let query = query.trim();
    if query.is_empty() && phase == "started" {
        return None;
    }
    let snippet = truncate_for_status(query, 72);
    Some(match status {
        "completed" => format!("[search] done: {snippet}"),
        "failed" => format!("[search] failed: {snippet}"),
        _ => format!("[search] running: {snippet}"),
    })
}

fn parse_claude_status_event(
    map: &serde_json::Map<String, Value>,
    subtype: &str,
) -> Option<String> {
    match subtype {
        "api_retry" => {
            let attempt = map.get("attempt").and_then(Value::as_u64)?;
            let max_retries = map.get("max_retries").and_then(Value::as_u64);
            let error = map
                .get("error")
                .and_then(Value::as_str)
                .filter(|error| !error.trim().is_empty())
                .unwrap_or("retrying");
            Some(match max_retries {
                Some(max) => format!("[status] API retry {attempt}/{max}: {error}"),
                None => format!("[status] API retry {attempt}: {error}"),
            })
        }
        _ => None,
    }
}

fn summarize_claude_user_event(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }

    if let Some(skill) = text.strip_prefix("Launching skill:") {
        let skill = skill.trim();
        if skill.is_empty() {
            return Some("[skill] launching".to_string());
        }
        return Some(format!("[skill] launching: {skill}"));
    }

    if text.starts_with("Base directory for this skill:") {
        return Some("[skill] loaded skill instructions".to_string());
    }

    if text.eq_ignore_ascii_case("This command requires approval") {
        return Some("[approval] waiting for your decision".to_string());
    }

    if text.chars().count() > 200 {
        return None;
    }

    Some(format!("[status] {}", truncate_for_status(text, 120)))
}

fn extract_command_from_permission_input(value: &Value) -> Option<String> {
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
            if let Some(command) = map.get("command") {
                return extract_command_from_permission_input(command);
            }
            if let Some(command) = map.get("commands") {
                return extract_command_from_permission_input(command);
            }
            if let Some(query) = map.get("query").and_then(Value::as_str) {
                return Some(format!("[search] {query}"));
            }
            if let Some(url) = map.get("url").and_then(Value::as_str) {
                return Some(format!("[fetch] {url}"));
            }
            None
        }
        _ => None,
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

async fn next_claude_permission_request(
    state_dir: &Path,
    run_id: &str,
    seen: &mut HashSet<String>,
) -> Result<Option<ClaudePermissionRequest>> {
    let mut entries = match tokio::fs::read_dir(claude_permission_requests_dir(state_dir)).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if seen.contains(stem) {
            continue;
        }
        let body = tokio::fs::read(&path).await?;
        let request: ClaudePermissionRequest =
            serde_json::from_slice(&body).with_context(|| {
                format!(
                    "failed to parse Claude permission request {}",
                    path.display()
                )
            })?;
        if request.run_id != run_id {
            continue;
        }
        seen.insert(request.request_id.clone());
        return Ok(Some(request));
    }

    Ok(None)
}

async fn next_claude_status_event(
    state_dir: &Path,
    run_id: &str,
    seen: &mut HashSet<String>,
) -> Result<Option<ClaudeHookStatusEvent>> {
    let mut entries = match tokio::fs::read_dir(claude_status_events_dir(state_dir)).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if seen.contains(stem) {
            continue;
        }
        let body = tokio::fs::read(&path).await?;
        let event: ClaudeHookStatusEvent = serde_json::from_slice(&body).with_context(|| {
            format!("failed to parse Claude status event {}", path.display())
        })?;
        if event.run_id != run_id {
            continue;
        }
        seen.insert(event.event_id.clone());
        let _ = tokio::fs::remove_file(claude_status_event_path(state_dir, &event.event_id)).await;
        return Ok(Some(event));
    }

    Ok(None)
}

async fn write_claude_permission_decision(
    state_dir: &Path,
    decision: &LlmApprovalDecision,
) -> Result<()> {
    let response = ClaudePermissionResponse {
        request_id: decision.request_id.clone(),
        behavior: match decision.choice {
            LlmApprovalChoice::Accept | LlmApprovalChoice::AcceptForSession | LlmApprovalChoice::AlwaysAllow => "allow".to_string(),
            LlmApprovalChoice::Decline | LlmApprovalChoice::Cancel => "deny".to_string(),
        },
        message: match decision.choice {
            LlmApprovalChoice::AcceptForSession => Some("accept_for_session".to_string()),
            LlmApprovalChoice::Decline | LlmApprovalChoice::Cancel => {
                Some("Denied by Telegram approval".to_string())
            }
            _ => None,
        },
    };
    tokio::fs::write(
        claude_permission_response_path(state_dir, &decision.request_id),
        serde_json::to_vec_pretty(&response)?,
    )
    .await?;
    let _ = tokio::fs::remove_file(claude_permission_request_path(
        state_dir,
        &decision.request_id,
    ))
    .await;
    Ok(())
}

fn truncate_for_status(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn extract_text_from_json(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Array(items) => items.iter().find_map(extract_text_from_json),
        Value::Object(map) => {
            for key in [
                "output_text",
                "text",
                "content",
                "message",
                "response",
                "final",
                "result",
            ] {
                if let Some(found) = map.get(key).and_then(extract_text_from_json) {
                    return Some(found);
                }
            }

            map.values().find_map(|value| match value {
                Value::Array(_) | Value::Object(_) => extract_text_from_json(value),
                _ => None,
            })
        }
        _ => None,
    }
}

fn extract_session_id_from_json(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => items.iter().find_map(extract_session_id_from_json),
        Value::Object(map) => {
            let event_name = map
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| map.get("event").and_then(Value::as_str))
                .unwrap_or_default()
                .to_ascii_lowercase();
            for key in [
                "session_id",
                "sessionId",
                "conversation_id",
                "conversationId",
                "thread_id",
                "threadId",
            ] {
                if let Some(Value::String(session_id)) = map.get(key) {
                    return Some(session_id.clone());
                }
            }

            if (event_name.contains("thread")
                || event_name.contains("session")
                || event_name.contains("conversation"))
                && let Some(Value::String(id)) = map.get("id")
                && !id.trim().is_empty()
            {
                return Some(id.clone());
            }

            map.values().find_map(extract_session_id_from_json)
        }
        _ => None,
    }
}

fn parse_session_id_from_text(output: &str) -> Option<String> {
    for line in output.lines() {
        let lower = line.to_ascii_lowercase();
        if lower.contains("session id") {
            return line
                .split([':', ' '])
                .map(str::trim)
                .find(|part| {
                    part.len() >= 8
                        && part
                            .chars()
                            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
                })
                .map(str::to_string);
        }
    }
    None
}

fn normalize_thread_title(input: &str) -> Option<String> {
    let mut title = input
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default()
        .trim()
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '#' | '*' | '-' | ':' | ' '))
        .to_string();

    if title.is_empty() {
        return None;
    }

    title = title.replace('\t', " ");
    while title.contains("  ") {
        title = title.replace("  ", " ");
    }

    let max_chars = 40;
    let mut shortened: String = title.chars().take(max_chars).collect();
    if title.chars().count() > max_chars {
        shortened = shortened
            .trim_end_matches(|ch: char| ch.is_ascii_punctuation() || ch.is_whitespace())
            .to_string();
    }

    let normalized = shortened
        .trim()
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '#' | '*' | '-' | ':' | ' '))
        .to_string();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn generate_title_from_messages(messages: &[StoredMessage]) -> Option<String> {
    messages
        .iter()
        .find(|message| message.role == "user")
        .and_then(|message| normalize_thread_title(&message.content))
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.to_string()),
        Value::Array(items) => {
            let mut parts = Vec::new();
            for item in items {
                if let Some(text) = item
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .or_else(|| {
                        item.get("content")
                            .and_then(Value::as_str)
                            .map(ToString::to_string)
                    })
                {
                    parts.push(text);
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        }
        _ => None,
    }
}

fn fallback_text(output: &str) -> Option<String> {
    let text = output.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn json_stream_parser_finds_text_and_session_id() {
        let parsed = parse_json_stream(
            r#"{"event":"session","session_id":"abc-123"}
{"event":"message","text":"hello"}"#,
        );

        assert_eq!(parsed.session_id.as_deref(), Some("abc-123"));
        assert_eq!(parsed.text.as_deref(), Some("hello"));
    }

    #[test]
    fn history_prompt_includes_recent_messages() {
        let session = StoredSession {
            transport_session_id: "1".to_string(),
            provider: "claude".to_string(),
            provider_session_id: Some("sess".to_string()),
            messages: vec![
                StoredMessage {
                    role: "user".to_string(),
                    content: "hi".to_string(),
                },
                StoredMessage {
                    role: "assistant".to_string(),
                    content: "hello".to_string(),
                },
            ],
            updated_at_unix_ms: 0,
        };

        let rendered = render_prompt_with_history(&session, "next");
        assert!(rendered.contains("user: hi"));
        assert!(rendered.contains("assistant: hello"));
        assert!(rendered.contains("next"));
    }

    #[test]
    fn codex_sample_parses_all_agent_messages_and_thread_id() {
        let sample = r#"{"type":"thread.started","thread_id":"019d9667-3905-7a71-990a-ccaff3ffd9da"}
{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"echo loading","status":"in_progress"}}
{"type":"item.completed","item":{"id":"msg_1","type":"agent_message","text":"I'll start by using `polymarket-news-impact` to check today's major news."}}
{"type":"item.completed","item":{"id":"msg_2","type":"agent_message","text":"The local data layer is available, we can continue querying further."}}
{"type":"item.completed","item":{"id":"msg_3","type":"agent_message","text":"News candidates have converged, an initial result can be provided as of the evening of `2026-04-16`."}}
{"type":"turn.completed"}"#;
        let parsed = parse_codex_output(sample);

        assert_eq!(
            parsed.session_id.as_deref(),
            Some("019d9667-3905-7a71-990a-ccaff3ffd9da")
        );
        let text = parsed.text.expect("expected codex assistant text");
        assert!(text.contains("I'll start by using `polymarket-news-impact`"));
        assert!(text.contains("The local data layer is available"));
        assert!(text.contains("News candidates have converged"));
        assert!(text.contains("as of the evening of `2026-04-16`"));
    }

    #[test]
    fn codex_streaming_state_keeps_tool_summaries_out_of_final_text() {
        let mut state = CodexStreamingState::default();

        assert!(
            state
                .ingest_line(r#"{"type":"thread.started","thread_id":"thread-123"}"#)
                .is_none()
        );
        assert!(state.ingest_line(r#"{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"echo hi","status":"in_progress"}}"#).is_some());
        assert!(state.ingest_line(r#"{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"hello world"}}"#).is_some());

        let rendered = state
            .render_display_text()
            .expect("expected rendered display text");
        assert!(rendered.contains("[command] running: echo hi"));
        assert!(rendered.contains("hello world"));
        assert_eq!(state.finish_text().as_deref(), Some("hello world"));
    }

    #[test]
    fn codex_streaming_state_replaces_old_status_with_new_status() {
        let mut state = CodexStreamingState::default();

        let _ = state.ingest_line(r#"{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"echo one","status":"in_progress"}}"#);
        let _ = state.ingest_line(r#"{"type":"item.completed","item":{"id":"item_2","type":"web_search","query":"latest fed news","status":"completed"}}"#);

        let rendered = state
            .render_display_text()
            .expect("expected rendered display text");
        assert!(!rendered.contains("echo one"));
        assert!(rendered.contains("[search] done: latest fed news"));
    }

    #[test]
    fn codex_status_events_are_rendered_as_latest_status() {
        let value: Value = serde_json::from_str(
            r#"{"type":"approval.required","message":"Command needs approval"}"#,
        )
        .unwrap();
        let event = parse_codex_event(&value).expect("expected status event");
        assert_eq!(
            event.display_text.as_deref(),
            Some("[status] Command needs approval")
        );
        assert!(event.assistant_text.is_none());
    }

    #[test]
    fn session_id_parser_accepts_thread_events_with_id_field() {
        let value: Value =
            serde_json::from_str(r#"{"type":"thread.started","id":"thread-123"}"#).unwrap();
        assert_eq!(
            extract_session_id_from_json(&value).as_deref(),
            Some("thread-123")
        );
    }

    #[test]
    fn codex_event_log_is_written_to_state_dir() {
        let dir = tempdir().unwrap();
        let runtime = RuntimeConfig {
            shell: "/bin/zsh".to_string(),
            cwd: dir.path().to_path_buf(),
            state_dir: dir.path().join("state"),
            audit_log_file: dir.path().join("state/audit.log.jsonl"),
            timeout_secs: 60,
            user_config_file: dir.path().join("config.json"),
            llm_sessions_file: dir.path().join("state/llm-sessions.json"),
            telegram_chat_sessions_file: dir.path().join("state/telegram-chat-sessions.json"),
        };

        append_codex_event_log_line(
            &runtime,
            "chat:1:thread:2",
            Some("thread-123"),
            r#"{"type":"approval.required","message":"needs approval"}"#,
        )
        .unwrap();

        let content =
            std::fs::read_to_string(runtime.state_dir.join("codex-events.jsonl")).unwrap();
        assert!(content.contains("\"transport_session_id\":\"chat:1:thread:2\""));
        assert!(content.contains("\"provider_session_id\":\"thread-123\""));
        assert!(content.contains("\"approval.required\""));
    }

    #[test]
    fn claude_streaming_state_tracks_partial_and_final_text() {
        let mut state = ClaudeStreamingState::default();

        // system/status "requesting" is no longer forwarded
        assert!(state
            .ingest_line(
                r#"{"type":"system","subtype":"status","status":"requesting","session_id":"sess-1"}"#,
            )
            .is_none());

        let rendered = state
            .ingest_line(
                r#"{"type":"assistant","subtype":"partial","message":{"content":[{"type":"text","text":"draft"}]}}"#,
            )
            .expect("expected partial text");
        assert!(rendered.contains("draft"));

        let rendered = state
            .ingest_line(
                r#"{"type":"result","subtype":"success","result":"final answer","session_id":"sess-1"}"#,
            )
            .expect("expected final text");
        assert!(rendered.contains("final answer"));
        assert_eq!(state.session_id.as_deref(), Some("sess-1"));
        assert_eq!(state.finish_text().as_deref(), Some("final answer"));
    }

    #[test]
    fn claude_stream_event_tool_use_shows_tool_name() {
        let mut state = ClaudeStreamingState::default();

        let rendered = state
            .ingest_line(r#"{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_01","name":"Bash","input":{}}}}"#)
            .expect("expected tool status");
        assert!(rendered.contains("[tool] Bash"));
    }

    #[test]
    fn claude_stream_event_text_delta_accumulates_and_clears_tool_status() {
        let mut state = ClaudeStreamingState::default();

        // tool use sets status
        state.ingest_line(r#"{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t1","name":"Read","input":{}}}}"#);

        // first text_delta clears tool status and starts partial text
        let rendered = state
            .ingest_line(r#"{"type":"stream_event","event":{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Hello "}}}"#)
            .expect("expected text");
        assert!(rendered.contains("Hello"));
        assert!(!rendered.contains("[tool]"));

        // second delta appends
        let rendered = state
            .ingest_line(r#"{"type":"stream_event","event":{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"world"}}}"#)
            .expect("expected accumulated text");
        assert!(rendered.contains("Hello world"));

        // final assistant event deduplicates and finalizes
        let rendered = state
            .ingest_line(r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Hello world"}]}}"#)
            .expect("expected final text");
        assert!(rendered.contains("Hello world"));
        assert_eq!(state.finish_text().as_deref(), Some("Hello world"));
        assert!(state.partial_text.is_none());
    }

    #[test]
    fn claude_api_retry_status_is_rendered() {
        let value: Value = serde_json::from_str(
            r#"{"type":"system","subtype":"api_retry","attempt":2,"max_retries":5,"error":"unknown"}"#,
        )
        .unwrap();
        let status =
            parse_claude_status_event(value.as_object().expect("expected object"), "api_retry")
                .expect("expected status");
        assert_eq!(status, "[status] API retry 2/5: unknown");
    }

    #[test]
    fn claude_user_events_render_status_summaries() {
        let mut state = ClaudeStreamingState::default();

        let rendered = state
            .ingest_line(r#"{"type":"user","text":"Launching skill: evm-wallet"}"#)
            .expect("expected skill launch status");
        assert!(rendered.contains("[skill] launching: evm-wallet"));

        let rendered = state
            .ingest_line(r#"{"type":"user","text":"This command requires approval"}"#)
            .expect("expected approval status");
        assert!(rendered.contains("[approval] waiting for your decision"));
    }

    #[test]
    fn claude_external_status_updates_are_rendered() {
        let mut state = ClaudeStreamingState::default();
        let rendered = state
            .ingest_external_status("[search] running: latest fed news".to_string())
            .expect("expected rendered external status");
        assert!(rendered.contains("[search] running: latest fed news"));
    }

    #[test]
    fn normalize_thread_title_trims_noise_and_limits_length() {
        assert_eq!(
            normalize_thread_title("  ## `What major news today affected prediction markets`  "),
            Some("What major news today affected prediction markets".to_string())
        );

        let normalized = normalize_thread_title(
            "This is a very long title that should be trimmed before it gets too wide",
        )
        .expect("expected normalized title");
        assert!(normalized.chars().count() <= 40);
        assert!(normalized.starts_with("This is a very long title"));
    }

    #[test]
    fn generate_title_from_messages_uses_first_user_message() {
        let messages = vec![
            StoredMessage {
                role: "assistant".to_string(),
                content: "hello".to_string(),
            },
            StoredMessage {
                role: "user".to_string(),
                content: "  `Recommend the top 5 smart money traders for me` ".to_string(),
            },
            StoredMessage {
                role: "user".to_string(),
                content: "later".to_string(),
            },
        ];

        assert_eq!(
            generate_title_from_messages(&messages).as_deref(),
            Some("Recommend the top 5 smart money traders for me")
        );
    }
}
