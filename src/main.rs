use anyhow::Result;
use clap::Parser;
use std::process::ExitCode;

mod audit;
mod claude_mcp;
mod cli;
mod commands;
mod config;
mod llm;
mod output;
mod session;
mod shell;
mod transport;

use crate::cli::Cli;
use crate::cli::Commands;
use crate::config::{AppConfig, RuntimeOverrides};
use crate::output::OutputMode;

#[tokio::main]
async fn main() -> Result<ExitCode> {
    dotenvy::dotenv().ok();

    if std::env::var("RUST_LOG").is_ok() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }

    let cli = Cli::parse();
    match &cli.command {
        Commands::ClaudePermissionHook(cmd) => {
            crate::claude_mcp::run_permission_hook(
                cmd.state_dir.clone(),
                cmd.transport_session_id.clone(),
                cmd.run_id.clone(),
            )
            .await?;
            return Ok(ExitCode::SUCCESS);
        }
        _ => {}
    }

    let overrides = RuntimeOverrides::from(&cli);
    let config = AppConfig::load(overrides)?;
    let output_mode = OutputMode::from(&cli);

    commands::dispatch(cli.command, config, output_mode).await
}
