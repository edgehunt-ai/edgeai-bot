use clap::{Args, Subcommand};

#[derive(Args, Debug)]
pub struct ConfigCmd {
    #[command(subcommand)]
    pub command: ConfigSubcommand,
}

#[derive(Subcommand, Debug)]
pub enum ConfigSubcommand {
    /// Print the effective configuration after env and CLI overrides are applied
    Show,
}
