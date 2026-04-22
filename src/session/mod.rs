use crate::config::RuntimeConfig;
use crate::shell::{ExecutionResult, PersistentShellSession};
use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SessionStore {
    runtime: RuntimeConfig,
    sessions: Arc<Mutex<HashMap<String, Arc<Mutex<PersistentShellSession>>>>>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct SessionSummary {
    pub session_id: String,
}

impl SessionStore {
    pub fn new(runtime: RuntimeConfig) -> Self {
        Self {
            runtime,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn execute(
        &self,
        session_id: impl Into<String>,
        command: &str,
    ) -> Result<ExecutionResult> {
        let session_id = session_id.into();
        let session = self.get_or_create(&session_id).await?;
        let mut guard = session.lock().await;
        match guard.run(command).await {
            Ok(result) => Ok(result),
            Err(error) => {
                drop(guard);
                let _ = self.reset(&session_id).await;
                Err(error)
            }
        }
    }

    pub async fn reset(&self, session_id: &str) -> Result<bool> {
        let removed = {
            let mut sessions = self.sessions.lock().await;
            sessions.remove(session_id)
        };

        if let Some(session) = removed {
            session.lock().await.shutdown().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[allow(dead_code)]
    pub async fn list(&self) -> Vec<SessionSummary> {
        let sessions = self.sessions.lock().await;
        let mut ids: Vec<String> = sessions.keys().cloned().collect();
        ids.sort();
        ids.into_iter()
            .map(|session_id| SessionSummary { session_id })
            .collect()
    }

    async fn get_or_create(&self, session_id: &str) -> Result<Arc<Mutex<PersistentShellSession>>> {
        if let Some(existing) = self.sessions.lock().await.get(session_id).cloned() {
            return Ok(existing);
        }

        let session = Arc::new(Mutex::new(
            PersistentShellSession::spawn(self.runtime.clone()).await?,
        ));

        let mut sessions = self.sessions.lock().await;
        Ok(sessions
            .entry(session_id.to_string())
            .or_insert_with(|| session.clone())
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn isolates_state_between_sessions() {
        let store = SessionStore::new(RuntimeConfig {
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

        let one = store.execute("chat-1", "cd /tmp && pwd").await.unwrap();
        let two = store.execute("chat-2", "pwd").await.unwrap();
        let one_again = store.execute("chat-1", "pwd").await.unwrap();

        assert_eq!(one.stdout, "/tmp");
        assert_eq!(one_again.stdout, "/tmp");
        assert_ne!(two.stdout, "/tmp");
    }
}
