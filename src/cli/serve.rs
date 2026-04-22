use clap::{Args, Subcommand};

#[derive(Args, Debug)]
pub struct ServeCmd {
    #[command(subcommand)]
    pub transport: TransportCommand,
}

#[derive(Subcommand, Debug)]
pub enum TransportCommand {
    /// Start the Telegram transport
    Telegram(TelegramServeCmd),
}

#[derive(Args, Debug)]
pub struct TelegramServeCmd {
    /// Telegram bot token
    #[arg(long, env = "TELEGRAM_BOT_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    /// Comma-delimited list of allowed Telegram chat IDs
    #[arg(long, env = "TELEGRAM_ALLOWED_CHAT_IDS", value_delimiter = ',')]
    pub allowed_chat_ids: Vec<i64>,

    /// Poll interval in seconds for the future long-polling loop
    #[arg(long, env = "TELEGRAM_POLL_INTERVAL_SECS")]
    pub poll_interval_secs: Option<u64>,
}
