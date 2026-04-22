use clap::Args;

#[derive(Args, Debug)]
pub struct ExecCmd {
    /// Raw command string passed to the configured shell via `-lc`
    pub command: String,

    /// Optional stdin payload forwarded to the spawned shell
    #[arg(long)]
    pub stdin: Option<String>,

    /// Optional session ID for persistent shell execution within the current process
    #[arg(long)]
    pub session: Option<String>,
}
