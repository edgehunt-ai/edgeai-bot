use clap::Args;

#[derive(Args, Debug)]
pub struct RunCmd {
    /// Target chat ID to send the result to
    #[arg(long)]
    pub chat_id: i64,

    /// Thread/topic ID (for forum supergroups)
    #[arg(long)]
    pub thread_id: Option<i32>,

    /// Prompt to send to the LLM
    #[arg(long)]
    pub prompt: String,
}
