use crate::cli::Cli;
use crate::cli::serve::TelegramServeCmd;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const CONFIG_DIR_NAME: &str = "edgeai";
const CONFIG_FILE_NAME: &str = "config.json";

#[derive(Debug, Clone, Serialize)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub telegram: TelegramConfig,
    pub llm: LlmConfig,
}

#[derive(Debug, Clone)]
pub struct RuntimeOverrides {
    pub shell: Option<String>,
    pub cwd: Option<String>,
    pub timeout_secs: Option<u64>,
}

impl From<&Cli> for RuntimeOverrides {
    fn from(value: &Cli) -> Self {
        Self {
            shell: value.shell.clone(),
            cwd: value.cwd.clone(),
            timeout_secs: value.timeout_secs,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfig {
    pub shell: String,
    pub cwd: PathBuf,
    pub state_dir: PathBuf,
    pub audit_log_file: PathBuf,
    pub timeout_secs: u64,
    pub user_config_file: PathBuf,
    pub llm_sessions_file: PathBuf,
    pub telegram_chat_sessions_file: PathBuf,
    pub active_wallet_file: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TelegramConfig {
    pub bot_token: Option<String>,
    pub allowed_chat_ids: Vec<i64>,
    pub poll_interval_secs: u64,
    pub offset_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    pub provider: String,
    pub model: Option<String>,
    pub api_url: Option<String>,
    pub api_key: Option<String>,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            provider: "claude".to_string(),
            model: None,
            api_url: None,
            api_key: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedConfig {
    pub telegram: TelegramConfig,
    pub llm: LlmConfig,
}

impl AppConfig {
    pub fn load(overrides: RuntimeOverrides) -> Result<Self> {
        let config_file = default_config_file()?;
        let config_dir = config_file
            .parent()
            .map(Path::to_path_buf)
            .context("config file must have a parent directory")?;
        std::fs::create_dir_all(&config_dir)?;

        let persisted = load_persisted_config(&config_file)?;
        let cwd = overrides
            .cwd
            .or_else(|| std::env::var("EDGEAI_CWD").ok())
            .map(PathBuf::from)
            .unwrap_or(std::env::current_dir()?);
        if !cwd.exists() {
            bail!(
                "configured working directory does not exist: {}",
                cwd.display()
            );
        }

        let state_dir = std::env::var("EDGEAI_STATE_DIR")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| config_dir.join("state"));
        std::fs::create_dir_all(&state_dir)?;

        let timeout_secs = overrides
            .timeout_secs
            .or_else(|| std::env::var("EDGEAI_TIMEOUT_SECS").ok()?.parse().ok())
            .unwrap_or(1800);
        let audit_log_file = std::env::var("EDGEAI_AUDIT_LOG_FILE")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| state_dir.join("audit.log.jsonl"));

        let bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
            .ok()
            .or(persisted.telegram.bot_token.clone());
        let allowed_chat_ids = std::env::var("TELEGRAM_ALLOWED_CHAT_IDS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .filter_map(|value| value.trim().parse::<i64>().ok())
                    .collect()
            })
            .unwrap_or_else(|| persisted.telegram.allowed_chat_ids.clone());
        let poll_interval_secs = std::env::var("TELEGRAM_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_else(|| persisted.telegram.poll_interval_secs.max(2));
        let offset_file = std::env::var("TELEGRAM_OFFSET_FILE")
            .ok()
            .map(PathBuf::from)
            .or(persisted.telegram.offset_file.clone())
            .unwrap_or_else(|| state_dir.join("telegram-offset.txt"));

        Ok(Self {
            runtime: RuntimeConfig {
                shell: overrides
                    .shell
                    .or_else(|| std::env::var("EDGEAI_SHELL").ok())
                    .unwrap_or_else(|| "/bin/bash".to_string()),
                cwd,
                state_dir: state_dir.clone(),
                audit_log_file,
                timeout_secs,
                user_config_file: config_file,
                llm_sessions_file: state_dir.join("llm-sessions.json"),
                telegram_chat_sessions_file: state_dir.join("telegram-chat-sessions.json"),
                active_wallet_file: state_dir.join("active-wallet.json"),
            },
            telegram: TelegramConfig {
                bot_token,
                allowed_chat_ids,
                poll_interval_secs,
                offset_file: Some(offset_file),
            },
            llm: persisted.llm,
        })
    }
}

impl TelegramConfig {
    pub fn merge_with_cli(mut self, cli: TelegramServeCmd) -> Self {
        if cli.token.is_some() {
            self.bot_token = cli.token;
        }
        if !cli.allowed_chat_ids.is_empty() {
            self.allowed_chat_ids = cli.allowed_chat_ids;
        }
        if let Some(poll_interval_secs) = cli.poll_interval_secs {
            self.poll_interval_secs = poll_interval_secs;
        }
        self
    }
}

pub fn save_persisted_config(path: &Path, config: &PersistedConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(config)?)?;
    Ok(())
}

pub fn default_config_file() -> Result<PathBuf> {
    let base_dir = if let Ok(dir) = std::env::var("XDG_CONFIG_HOME") {
        PathBuf::from(dir)
    } else if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join(".config")
    } else {
        std::env::current_dir()?
    };
    Ok(base_dir.join(CONFIG_DIR_NAME).join(CONFIG_FILE_NAME))
}

pub fn load_persisted_config(path: &Path) -> Result<PersistedConfig> {
    match std::fs::read(path) {
        Ok(body) => Ok(serde_json::from_slice(&body)
            .with_context(|| format!("failed to parse config file {}", path.display()))?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(PersistedConfig::default())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved: Vec<(&str, Option<String>)> = vars
            .iter()
            .map(|(key, _)| (*key, std::env::var(key).ok()))
            .collect();

        for (key, value) in vars {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }

        f();

        for (key, value) in saved {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }

    #[test]
    fn cli_runtime_overrides_replace_env_defaults() {
        with_env(
            &[
                ("XDG_CONFIG_HOME", Some("/tmp/edgeai-config-test")),
                ("EDGEAI_SHELL", Some("/bin/sh")),
                ("EDGEAI_TIMEOUT_SECS", Some("30")),
            ],
            || {
                let config = AppConfig::load(RuntimeOverrides {
                    shell: Some("/bin/bash".to_string()),
                    cwd: None,
                    timeout_secs: Some(5),
                })
                .unwrap();

                assert_eq!(config.runtime.shell, "/bin/bash");
                assert_eq!(config.runtime.timeout_secs, 5);
                assert!(config.runtime.user_config_file.ends_with("config.json"));
                assert!(
                    config
                        .runtime
                        .llm_sessions_file
                        .ends_with("llm-sessions.json")
                );
                assert!(
                    config
                        .runtime
                        .telegram_chat_sessions_file
                        .ends_with("telegram-chat-sessions.json")
                );
            },
        );
    }

    #[test]
    fn telegram_cli_values_override_persisted_values() {
        let merged = TelegramConfig {
            bot_token: Some("persisted-token".to_string()),
            allowed_chat_ids: vec![1],
            poll_interval_secs: 2,
            offset_file: Some(PathBuf::from("/tmp/telegram-offset.txt")),
        }
        .merge_with_cli(TelegramServeCmd {
            token: Some("cli-token".to_string()),
            allowed_chat_ids: vec![2, 3],
            poll_interval_secs: Some(10),
        });

        assert_eq!(merged.bot_token.as_deref(), Some("cli-token"));
        assert_eq!(merged.allowed_chat_ids, vec![2, 3]);
        assert_eq!(merged.poll_interval_secs, 10);
    }

    #[test]
    fn telegram_persisted_poll_interval_survives_when_cli_omits_it() {
        let merged = TelegramConfig {
            bot_token: Some("persisted-token".to_string()),
            allowed_chat_ids: vec![1],
            poll_interval_secs: 9,
            offset_file: Some(PathBuf::from("/tmp/telegram-offset.txt")),
        }
        .merge_with_cli(TelegramServeCmd {
            token: None,
            allowed_chat_ids: Vec::new(),
            poll_interval_secs: None,
        });

        assert_eq!(merged.poll_interval_secs, 9);
    }
}
