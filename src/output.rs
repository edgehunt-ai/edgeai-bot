use crate::config::AppConfig;
use crate::shell::ExecutionResult;
use crate::transport::telegram::TelegramStatus;
use anyhow::Result;

#[derive(Debug, Clone, Copy)]
pub enum OutputMode {
    Text,
    Json,
}

impl OutputMode {
    pub fn from(cli: &crate::cli::Cli) -> Self {
        if cli.json { Self::Json } else { Self::Text }
    }
}

pub fn print_exec_result(result: &ExecutionResult, mode: OutputMode) -> Result<()> {
    match mode {
        OutputMode::Json => {
            println!("{}", serde_json::to_string_pretty(result)?);
        }
        OutputMode::Text => {
            println!("exit_code: {}", result.exit_code);
            if !result.stdout.is_empty() {
                println!("stdout:\n{}", result.stdout);
            }
            if !result.stderr.is_empty() {
                println!("stderr:\n{}", result.stderr);
            }
        }
    }
    Ok(())
}

pub fn print_config(config: &AppConfig, mode: OutputMode) -> Result<()> {
    match mode {
        OutputMode::Json => println!("{}", serde_json::to_string_pretty(config)?),
        OutputMode::Text => {
            println!("shell: {}", config.runtime.shell);
            println!("cwd: {}", config.runtime.cwd.display());
            println!("state_dir: {}", config.runtime.state_dir.display());
            println!(
                "audit_log_file: {}",
                config.runtime.audit_log_file.display()
            );
            println!(
                "user_config_file: {}",
                config.runtime.user_config_file.display()
            );
            println!(
                "llm_sessions_file: {}",
                config.runtime.llm_sessions_file.display()
            );
            println!("timeout_secs: {}", config.runtime.timeout_secs);
            println!(
                "telegram_bot_token: {}",
                redact_secret(config.telegram.bot_token.as_deref())
            );
            println!(
                "telegram_allowed_chat_ids: {:?}",
                config.telegram.allowed_chat_ids
            );
            println!(
                "telegram_poll_interval_secs: {}",
                config.telegram.poll_interval_secs
            );
            println!(
                "telegram_offset_file: {}",
                config
                    .telegram
                    .offset_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<unset>".to_string())
            );
            println!("llm_provider: {}", config.llm.provider);
            println!(
                "llm_model: {}",
                config.llm.model.as_deref().unwrap_or("<unset>")
            );
            println!(
                "llm_api_url: {}",
                config.llm.api_url.as_deref().unwrap_or("<unset>")
            );
            println!(
                "llm_api_key: {}",
                redact_secret(config.llm.api_key.as_deref())
            );
        }
    }
    Ok(())
}

pub fn print_transport_status(status: &TelegramStatus, mode: OutputMode) -> Result<()> {
    match mode {
        OutputMode::Json => println!("{}", serde_json::to_string_pretty(status)?),
        OutputMode::Text => {
            println!("transport: telegram");
            println!("shell: {}", status.shell);
            println!("cwd: {}", status.cwd);
            println!("timeout_secs: {}", status.timeout_secs);
            println!("allowed_chat_ids: {:?}", status.allowed_chat_ids);
            println!("poll_interval_secs: {}", status.poll_interval_secs);
            println!("llm_provider: {}", status.llm_provider);
            println!("status: {}", status.status);
            println!("note: {}", status.note);
        }
    }
    Ok(())
}

fn redact_secret(secret: Option<&str>) -> &'static str {
    if secret.is_some() {
        "<configured>"
    } else {
        "<unset>"
    }
}
