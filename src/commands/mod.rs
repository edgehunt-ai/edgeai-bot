use crate::cli::Commands;
use crate::cli::claude_mcp::ClaudePermissionHookCmd;
use crate::cli::run::RunCmd;
use crate::cli::config::ConfigSubcommand;
use crate::cli::logs;
use crate::cli::serve::TransportCommand;
use crate::config::{
    AppConfig, LlmConfig, PersistedConfig, TelegramConfig, default_config_file,
    load_persisted_config, save_persisted_config,
};
use crate::llm::{LlmClient, detected_provider_options, install_provider};
use crate::output::{OutputMode, print_config, print_transport_status};
use crate::session::SessionStore;
use crate::shell::ShellExecutor;
use crate::transport::telegram::TelegramTransport;
use anyhow::{Context, Result, bail};
use inquire::{Confirm, Select, Text};
use serde_json::Value;
use std::io;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::time::Duration;

pub async fn dispatch(
    command: Commands,
    config: AppConfig,
    output_mode: OutputMode,
) -> Result<ExitCode> {
    match command {
        Commands::ClaudePermissionHook(ClaudePermissionHookCmd {
            state_dir,
            transport_session_id,
            run_id,
        }) => {
            crate::claude_mcp::run_permission_hook(state_dir, transport_session_id, run_id)
                .await?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Exec(cmd) => {
            let result = if let Some(session_id) = cmd.session {
                let sessions = SessionStore::new(config.runtime.clone());
                sessions.execute(session_id, &cmd.command).await?
            } else {
                let executor = ShellExecutor::new(config.runtime.clone());
                executor.run(&cmd.command, cmd.stdin).await?
            };
            crate::output::print_exec_result(&result, output_mode)?;
            Ok(ExitCode::from(result.exit_code_as_u8()))
        }
        Commands::Init(_) => {
            run_init_wizard().await?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Config(cmd) => {
            match cmd.command {
                ConfigSubcommand::Show => print_config(&config, output_mode)?,
            }
            Ok(ExitCode::SUCCESS)
        }
        Commands::Logs(cmd) => {
            logs::run(cmd)?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Run(RunCmd { chat_id, thread_id, prompt }) => {
            let llm =
                LlmClient::from_config(config.runtime.clone(), config.llm.clone()).await?;
            let transport = TelegramTransport::from_config(
                config.runtime,
                config.telegram,
                llm,
                crate::cli::serve::TelegramServeCmd {
                    token: None,
                    allowed_chat_ids: vec![],
                    poll_interval_secs: None,
                },
            )
            .await?;
            transport.run_prompt(chat_id, thread_id, &prompt).await?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Serve(cmd) => match cmd.transport {
            TransportCommand::Telegram(telegram) => {
                let config_file = default_config_file()?;
                if !config_file.exists() && config.telegram.bot_token.is_none() {
                    bail!(
                        "Initialization has not been completed. Please run: edgeai init"
                    );
                }
                let llm =
                    LlmClient::from_config(config.runtime.clone(), config.llm.clone()).await?;
                let transport = Arc::new(
                    TelegramTransport::from_config(config.runtime, config.telegram, llm, telegram)
                        .await?,
                );
                let status = transport.describe()?;
                print_transport_status(&status, output_mode)?;
                transport.run().await?;
                Ok(ExitCode::SUCCESS)
            }
        },
    }
}

fn section(title: &str) {
    println!("\n\x1b[1;36m── {title} \x1b[0m\n");
}

fn hint(text: &str) {
    println!("\x1b[2m  {text}\x1b[0m");
}

async fn run_init_wizard() -> Result<()> {
    println!("\x1b[1mWelcome to the edgeai initialization wizard\x1b[0m");
    println!("\x1b[2mUse arrow keys to select, Enter to confirm, Ctrl+C to exit\x1b[0m");

    let config_file = default_config_file()?;
    let existing = load_persisted_config(&config_file)?;

    // ── Step 1: LLM ──────────────────────────────────────
    section("Step 1/3  LLM Configuration");
    let provider = select_provider().await?;

    if !provider.installed {
        println!();
        hint(&format!("{} is not installed", provider.label));
        if let Some(command) = provider.install_command.as_deref() {
            hint(&format!("Install command: {command}"));
            println!();
            let yes = Confirm::new("Run installation now?")
                .with_default(true)
                .prompt()
                .map_err(|e| anyhow::anyhow!(e))?;
            if yes {
                install_provider(&provider).await?;
                println!("\n  \x1b[1;32m✓ Installation complete\x1b[0m");
            } else {
                bail!("Cancelled, initialization not completed");
            }
        }
    }
    if let Some(setup_hint) = provider.setup_hint.as_deref() {
        println!();
        hint(&format!("Hint: {setup_hint}"));
    }
    let provider_config = LlmConfig {
        provider: provider.id.clone(),
        model: None,
        api_url: None,
        api_key: None,
    };

    // ── Step 2: Telegram ─────────────────────────────────
    section("Step 2/3  Telegram Configuration");
    let bot_token = prompt_bot_token(existing.telegram.bot_token.as_deref())?;

    // ── Step 3: Access control ──────────────────────────────────
    section("Step 3/3  Access Control");
    let allowed_chat_ids =
        prompt_allowed_chat_ids(&bot_token, &existing.telegram.allowed_chat_ids).await?;

    // ── Write configuration ──────────────────────────────────────────
    let persisted = PersistedConfig {
        telegram: TelegramConfig {
            bot_token: Some(bot_token),
            allowed_chat_ids,
            poll_interval_secs: 2,
            offset_file: None,
        },
        llm: provider_config,
    };
    save_persisted_config(&config_file, &persisted)?;

    // ── Install skills ───────────────────────────────────────
    section("Skills Installation");
    install_skills().await?;

    // ── Done ──────────────────────────────────────────────
    println!("\n\x1b[1;32m✓ Initialization complete\x1b[0m\n");
    println!("  Config file  {}", config_file.display());
    println!("  Start command  \x1b[1medgeai serve telegram\x1b[0m");
    println!();
    Ok(())
}

async fn install_skills() -> Result<()> {
    const SKILLS: &[&str] = &[
        "DODOEX/ChainPilot",
        "predictradar-ai/predictradar-skills",
    ];

    println!("Installing skills…\n");
    for skill in SKILLS {
        println!("  → npx skills add {skill} -y -g");
        let status = tokio::process::Command::new("npx")
            .args(["skills", "add", skill, "-y", "-g"])
            .status()
            .await
            .with_context(|| format!("Failed to run npx skills add {skill}"))?;
        if !status.success() {
            bail!("{skill} installation failed (exit {})", status.code().unwrap_or(1));
        }
    }
    Ok(())
}

fn prompt_bot_token(existing: Option<&str>) -> Result<String> {
    if let Some(token) = existing {
        let masked = mask_token(token);
        let keep_label = format!("Keep current setting ({masked})");
        let choice = Select::new("Telegram Bot Token", vec![keep_label.as_str(), "Enter a new token"])
            .prompt()
            .map_err(|e| anyhow::anyhow!(e))?;
        if choice == keep_label.as_str() {
            return Ok(token.to_string());
        }
    }
    println!("Create a bot and get a token:");
    println!("  1. Send /newbot to @BotFather in Telegram");
    println!("  2. Follow the prompts to set the bot name and username");
    println!("  3. Copy the HTTP API token returned after successful creation");
    println!("  4. Send /setthreads to @BotFather, select your bot, and enable Topics support");
    Text::new("Telegram Bot Token")
        .prompt()
        .map_err(|e| anyhow::anyhow!(e))
}

fn mask_token(token: &str) -> String {
    if token.len() <= 8 {
        return "****".to_string();
    }
    format!("****{}", &token[token.len() - 4..])
}

async fn select_provider() -> Result<crate::llm::ProviderOption> {
    let options = detected_provider_options();

    let labels: Vec<String> = options
        .iter()
        .map(|o| {
            if o.binary.is_some() && !o.installed {
                format!("{} [not installed]", o.label)
            } else {
                o.label.clone()
            }
        })
        .collect();

    let choice = Select::new("Select the capability for interacting with the LLM", labels.clone())
        .prompt()
        .map_err(|e| anyhow::anyhow!(e))?;
    let index = labels.iter().position(|l| l == &choice).unwrap();
    Ok(options[index].clone())
}

async fn prompt_allowed_chat_ids(bot_token: &str, existing: &[i64]) -> Result<Vec<i64>> {
    #[derive(Debug, PartialEq)]
    enum Choice {
        Auto,
        Unrestricted,
        Manual,
    }
    impl std::fmt::Display for Choice {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Choice::Auto => write!(
                    f,
                    "Auto-detect (recommended) — send the bot a message and your Chat ID will be read automatically"
                ),
                Choice::Unrestricted => {
                    write!(f, "Unrestricted — anyone can interact with the bot (suitable for internal testing)")
                }
                Choice::Manual => write!(f, "Manual entry — enter the list of allowed Chat IDs yourself"),
            }
        }
    }

    let mut options: Vec<String> = Vec::new();
    let mut has_keep = false;
    if !existing.is_empty() {
        let ids = existing
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        options.push(format!("Keep current setting ({ids})"));
        has_keep = true;
    } else if existing.is_empty() {
        // check if it was explicitly set to unrestricted (empty vec from config)
        // we can't distinguish "not set" from "set to empty" easily, so only show Keep
        // when there were actual IDs. Fall through to normal options.
    }
    options.push(Choice::Auto.to_string());
    options.push(Choice::Unrestricted.to_string());
    options.push(Choice::Manual.to_string());

    let choice = Select::new("Access control — which users can use this bot?", options.clone())
        .prompt()
        .map_err(|e| anyhow::anyhow!(e))?;

    if has_keep && choice == options[0] {
        return Ok(existing.to_vec());
    }

    let resolved = if choice == Choice::Auto.to_string() {
        Choice::Auto
    } else if choice == Choice::Unrestricted.to_string() {
        Choice::Unrestricted
    } else {
        Choice::Manual
    };

    match resolved {
        Choice::Auto => {
            let nonce = format!("edgeai-init-{}", uuid::Uuid::new_v4().simple());
            let baseline_update_id = latest_update_id(bot_token).await?;
            println!("Please send this exact message to your bot now: {nonce}");
            println!("After sending, press Enter. The program will only detect this new message to avoid using old updates.");
            let mut buf = String::new();
            io::stdin().read_line(&mut buf)?;
            let chat_id = wait_for_matching_chat_id(bot_token, baseline_update_id, &nonce).await?;

            println!("Chat ID detected: {chat_id}");
            Ok(vec![chat_id])
        }
        Choice::Unrestricted => Ok(Vec::new()),
        Choice::Manual => {
            let raw = Text::new("Chat ID list (separate multiple IDs with commas)")
                .prompt()
                .map_err(|e| anyhow::anyhow!(e))?;
            parse_chat_ids(Some(&raw))
        }
    }
}

async fn latest_update_id(bot_token: &str) -> Result<Option<i64>> {
    let resp = fetch_telegram_updates(bot_token, None, 0).await?;
    Ok(resp["result"]
        .as_array()
        .and_then(|updates| updates.iter().filter_map(|update| update["update_id"].as_i64()).max()))
}

async fn wait_for_matching_chat_id(
    bot_token: &str,
    baseline_update_id: Option<i64>,
    expected_text: &str,
) -> Result<i64> {
    let mut offset = baseline_update_id.map(|id| id + 1);
    for _ in 0..20 {
        let resp = fetch_telegram_updates(bot_token, offset, 25).await?;
        if let Some(updates) = resp["result"].as_array() {
            if let Some(latest_seen) = updates.iter().filter_map(|u| u["update_id"].as_i64()).max() {
                offset = Some(latest_seen + 1);
            }
            if let Some(chat_id) = find_chat_id_for_text(updates, expected_text) {
                return Ok(chat_id);
            }
        }
    }

    bail!("The verification message was not detected. Please confirm you sent the exact text to the bot, then re-run initialization")
}

async fn fetch_telegram_updates(bot_token: &str, offset: Option<i64>, timeout_secs: u64) -> Result<Value> {
    let url = format!("https://api.telegram.org/bot{bot_token}/getUpdates");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs + 5))
        .build()?;
    let mut request = client.get(&url).query(&[("timeout", timeout_secs)]);
    if let Some(offset) = offset {
        request = request.query(&[("offset", offset)]);
    }

    let resp: Value = request
        .send()
        .await
        .context("Failed to call Telegram API")?
        .json()
        .await
        .context("Failed to parse Telegram response")?;

    if resp["ok"].as_bool() == Some(false) {
        let desc = resp["description"].as_str().unwrap_or("unknown error");
        bail!("Telegram API error: {desc}");
    }

    Ok(resp)
}

fn find_chat_id_for_text(updates: &[Value], expected_text: &str) -> Option<i64> {
    updates.iter().find_map(|update| {
        ["message", "channel_post"].iter().find_map(|field| {
            let message = &update[*field];
            (message["text"].as_str() == Some(expected_text))
                .then(|| message["chat"]["id"].as_i64())
                .flatten()
        })
    })
}

fn parse_chat_ids(raw: Option<&str>) -> Result<Vec<i64>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }

    raw.split(',')
        .map(|part| {
            part.trim()
                .parse::<i64>()
                .with_context(|| format!("invalid chat id `{}`", part.trim()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_chat_id_only_for_matching_fresh_message() {
        let updates = vec![
            serde_json::json!({
                "update_id": 10,
                "message": {
                    "text": "older",
                    "chat": { "id": 1 }
                }
            }),
            serde_json::json!({
                "update_id": 11,
                "message": {
                    "text": "edgeai-init-abc",
                    "chat": { "id": 42 }
                }
            }),
        ];

        assert_eq!(find_chat_id_for_text(&updates, "edgeai-init-abc"), Some(42));
        assert_eq!(find_chat_id_for_text(&updates, "missing"), None);
    }
}
