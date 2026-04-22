use anyhow::Result;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct AuditLogger {
    path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct AuditEvent {
    pub ts_unix_ms: u128,
    pub source: String,
    pub status: String,
    pub phase: Option<String>,
    pub chat_id: i64,
    pub session_id: String,
    pub user_id: Option<i64>,
    pub username: Option<String>,
    pub command: String,
    pub exit_code: Option<i32>,
    pub timed_out: Option<bool>,
    pub duration_ms: Option<u128>,
    pub llm_duration_ms: Option<u128>,
    pub send_duration_ms: Option<u128>,
    pub output_bytes: Option<usize>,
    pub output_truncated: Option<bool>,
    pub error: Option<String>,
}

impl AuditLogger {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn append(&self, event: &AuditEvent) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        serde_json::to_writer(&mut file, event)?;
        file.write_all(b"\n")?;
        Ok(())
    }
}

pub fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn appends_jsonl_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log.jsonl");
        let logger = AuditLogger::new(path.clone());

        logger
            .append(&AuditEvent {
                ts_unix_ms: 1,
                source: "telegram".to_string(),
                status: "started".to_string(),
                phase: None,
                chat_id: 1,
                session_id: "1".to_string(),
                user_id: Some(2),
                username: Some("alice".to_string()),
                command: "pwd".to_string(),
                exit_code: None,
                timed_out: None,
                duration_ms: None,
                llm_duration_ms: None,
                send_duration_ms: None,
                output_bytes: None,
                output_truncated: None,
                error: None,
            })
            .unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains("\"status\":\"started\""));
        assert!(content.ends_with('\n'));
    }
}
