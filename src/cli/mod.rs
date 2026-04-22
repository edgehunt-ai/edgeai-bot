pub mod claude_mcp;
pub mod config;
pub mod exec;
pub mod init;
pub mod logs;
pub mod run;
pub mod serve;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "edgeai",
    version,
    about = "Expose controlled local shell execution through chat bot transports",
    long_about = None
)]
pub struct Cli {
    /// Output as machine-readable JSON
    #[arg(long, global = true)]
    pub json: bool,

    /// Suppress human-oriented output
    #[arg(long, global = true)]
    pub quiet: bool,

    /// Override the shell binary used for command execution
    #[arg(long, env = "EDGEAI_SHELL", global = true)]
    pub shell: Option<String>,

    /// Override the working directory used for command execution
    #[arg(long, env = "EDGEAI_CWD", global = true)]
    pub cwd: Option<String>,

    /// Override the per-command timeout in seconds
    #[arg(long, env = "EDGEAI_TIMEOUT_SECS", global = true)]
    pub timeout_secs: Option<u64>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run a single command locally through the same execution layer used by bot transports
    Exec(exec::ExecCmd),
    /// Interactive initial setup for Telegram and the selected LLM backend
    Init(init::InitCmd),
    /// Show the effective runtime configuration
    Config(config::ConfigCmd),
    /// Start a bot transport
    Serve(serve::ServeCmd),
    /// Show audit logs
    Logs(logs::LogsCmd),
    /// Send a one-shot LLM prompt to a Telegram chat (for use in cron/scheduled tasks)
    Run(run::RunCmd),
    #[command(hide = true)]
    ClaudePermissionHook(claude_mcp::ClaudePermissionHookCmd),
}
