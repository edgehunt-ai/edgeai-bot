use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
pub struct ClaudePermissionHookCmd {
    #[arg(long)]
    pub state_dir: PathBuf,

    #[arg(long)]
    pub transport_session_id: String,

    #[arg(long)]
    pub run_id: String,
}
