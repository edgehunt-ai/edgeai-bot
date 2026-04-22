use crate::config::RuntimeConfig;
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ShellExecutor {
    config: RuntimeConfig,
}

#[derive(Debug, Serialize)]
pub struct ExecutionResult {
    pub command: String,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

impl ExecutionResult {
    pub fn exit_code_as_u8(&self) -> u8 {
        self.exit_code.clamp(0, u8::MAX as i32) as u8
    }
}

impl ShellExecutor {
    pub fn new(config: RuntimeConfig) -> Self {
        Self { config }
    }

    pub async fn run(&self, command: &str, stdin: Option<String>) -> Result<ExecutionResult> {
        let mut child = Command::new(&self.config.shell)
            .arg("-lc")
            .arg(command)
            .current_dir(&self.config.cwd)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to spawn shell {}", self.config.shell))?;

        if let Some(stdin_payload) = stdin {
            let mut child_stdin = child
                .stdin
                .take()
                .context("stdin requested but child stdin was not available")?;
            child_stdin.write_all(stdin_payload.as_bytes()).await?;
            drop(child_stdin);
        }

        match timeout(
            Duration::from_secs(self.config.timeout_secs),
            child.wait_with_output(),
        )
        .await
        {
            Ok(output) => {
                let output = output?;
                Ok(ExecutionResult {
                    command: command.to_string(),
                    exit_code: output.status.code().unwrap_or(1),
                    stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
                    stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
                    timed_out: false,
                })
            }
            Err(_) => {
                bail!(
                    "command timed out after {} seconds: {}",
                    self.config.timeout_secs,
                    command
                );
            }
        }
    }
}

pub struct PersistentShellSession {
    config: RuntimeConfig,
    shell_kind: ShellKind,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    child: tokio::process::Child,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShellKind {
    BashLike,
    PosixSh,
    Fish,
    Unknown,
}

impl PersistentShellSession {
    pub async fn spawn(config: RuntimeConfig) -> Result<Self> {
        let shell_kind = detect_shell_kind(&config.shell);
        let mut command = Command::new(&config.shell);
        for arg in persistent_shell_args(shell_kind) {
            command.arg(arg);
        }
        let mut child = command
            .current_dir(&config.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to spawn persistent shell {}", config.shell))?;

        let stdin = child
            .stdin
            .take()
            .context("persistent shell stdin unavailable")?;
        let stdout = child
            .stdout
            .take()
            .context("persistent shell stdout unavailable")?;

        Ok(Self {
            config,
            shell_kind,
            stdin,
            stdout: BufReader::new(stdout),
            child,
        })
    }

    pub async fn run(&mut self, command: &str) -> Result<ExecutionResult> {
        let marker = format!("__EDGEAI_EXIT_{}__", Uuid::new_v4().simple());
        let wrapped = wrap_persistent_command(self.shell_kind, command, &marker);
        self.stdin.write_all(wrapped.as_bytes()).await?;
        self.stdin.flush().await?;

        match timeout(
            Duration::from_secs(self.config.timeout_secs),
            self.read_until_marker(&marker),
        )
        .await
        {
            Ok(result) => {
                let mut result = result?;
                result.command = command.to_string();
                Ok(result)
            }
            Err(_) => {
                self.shutdown().await?;
                bail!(
                    "persistent session command timed out after {} seconds: {}",
                    self.config.timeout_secs,
                    command
                );
            }
        }
    }

    async fn read_until_marker(&mut self, marker: &str) -> Result<ExecutionResult> {
        let marker_bytes = marker.as_bytes();
        let mut buf = Vec::new();
        let mut byte = [0_u8; 1];

        loop {
            let count = self.stdout.read(&mut byte).await?;
            if count == 0 {
                bail!("persistent shell exited unexpectedly while reading command output");
            }
            buf.extend_from_slice(&byte[..count]);

            if let Some(position) = find_subsequence(&buf, marker_bytes) {
                let tail = &buf[position + marker_bytes.len()..];
                let Some(line_end) = tail.iter().position(|value| *value == b'\n') else {
                    continue;
                };
                let output = &buf[..position];
                let exit_code = parse_exit_code(&tail[..line_end])?;
                return Ok(ExecutionResult {
                    command: String::new(),
                    exit_code,
                    stdout: String::from_utf8_lossy(output).trim().to_string(),
                    stderr: String::new(),
                    timed_out: false,
                });
            }
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        let _ = self.stdin.shutdown().await;
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
        Ok(())
    }
}

fn detect_shell_kind(shell: &str) -> ShellKind {
    match Path::new(shell).file_name().and_then(|name| name.to_str()) {
        Some("bash") | Some("zsh") => ShellKind::BashLike,
        Some("sh") | Some("dash") | Some("ash") => ShellKind::PosixSh,
        Some("fish") => ShellKind::Fish,
        _ => ShellKind::Unknown,
    }
}

fn persistent_shell_args(shell_kind: ShellKind) -> &'static [&'static str] {
    match shell_kind {
        ShellKind::BashLike => &["--noprofile", "--norc", "-s"],
        ShellKind::PosixSh => &["-s"],
        ShellKind::Fish => &[],
        ShellKind::Unknown => &["-s"],
    }
}

fn wrap_persistent_command(shell_kind: ShellKind, command: &str, marker: &str) -> String {
    match shell_kind {
        ShellKind::Fish => format!(
            "begin\n{command}\nend 2>&1\nset __edgeai_status $status\nprintf '\\n{marker}:%s\\n' \"$__edgeai_status\"\n"
        ),
        ShellKind::BashLike | ShellKind::PosixSh | ShellKind::Unknown => format!(
            "{{\n{command}\n}} 2>&1\n__edgeai_status=$?\nprintf '\\n{marker}:%s\\n' \"$__edgeai_status\"\n"
        ),
    }
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_exit_code(tail: &[u8]) -> Result<i32> {
    let tail = String::from_utf8_lossy(tail);
    let code = tail
        .trim_matches(|ch| ch == ':' || ch == '\n' || ch == '\r' || ch == ' ')
        .parse::<i32>()?;
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_command_and_captures_stdout() {
        let executor = ShellExecutor::new(RuntimeConfig {
            shell: "/bin/bash".to_string(),
            cwd: std::env::current_dir().unwrap(),
            state_dir: std::env::current_dir().unwrap().join(".edgeai-test"),
            audit_log_file: std::env::current_dir()
                .unwrap()
                .join(".edgeai-test/audit.log.jsonl"),
            timeout_secs: 5,
            user_config_file: std::env::current_dir()
                .unwrap()
                .join(".edgeai-test/config.json"),
            llm_sessions_file: std::env::current_dir()
                .unwrap()
                .join(".edgeai-test/llm-sessions.json"),
            telegram_chat_sessions_file: std::env::current_dir()
                .unwrap()
                .join(".edgeai-test/telegram-chat-sessions.json"),
        });

        let result = executor.run("printf 'hello'", None).await.unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, "hello");
        assert!(result.stderr.is_empty());
    }

    #[tokio::test]
    async fn persistent_session_keeps_working_directory() {
        let mut session = PersistentShellSession::spawn(RuntimeConfig {
            shell: "/bin/bash".to_string(),
            cwd: std::env::current_dir().unwrap(),
            state_dir: std::env::current_dir().unwrap().join(".edgeai-test"),
            audit_log_file: std::env::current_dir()
                .unwrap()
                .join(".edgeai-test/audit.log.jsonl"),
            timeout_secs: 5,
            user_config_file: std::env::current_dir()
                .unwrap()
                .join(".edgeai-test/config.json"),
            llm_sessions_file: std::env::current_dir()
                .unwrap()
                .join(".edgeai-test/llm-sessions.json"),
            telegram_chat_sessions_file: std::env::current_dir()
                .unwrap()
                .join(".edgeai-test/telegram-chat-sessions.json"),
        })
        .await
        .unwrap();

        let first = session.run("cd /tmp && pwd").await.unwrap();
        let second = session.run("pwd").await.unwrap();

        assert_eq!(first.stdout, "/tmp");
        assert_eq!(second.stdout, "/tmp");
    }

    #[test]
    fn detects_shell_specific_session_wrappers() {
        let fish = wrap_persistent_command(ShellKind::Fish, "echo hi", "MARK");
        let bash = wrap_persistent_command(ShellKind::BashLike, "echo hi", "MARK");

        assert!(fish.contains("set __edgeai_status $status"));
        assert!(bash.contains("__edgeai_status=$?"));
        assert_eq!(
            persistent_shell_args(ShellKind::BashLike),
            &["--noprofile", "--norc", "-s"]
        );
        assert!(persistent_shell_args(ShellKind::Fish).is_empty());
    }
}
