use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct LogsCmd {
    /// Number of lines to show (default 30)
    #[arg(short, long, default_value = "30")]
    pub lines: usize,

    /// Path to audit log file (defaults to ~/.config/edgeai/state/audit.log.jsonl)
    #[arg(short, long)]
    pub file: Option<PathBuf>,

    /// Follow log file like tail -f
    #[arg(long)]
    pub follow: bool,
}

pub fn run(cmd: LogsCmd) -> Result<()> {
    let path = cmd
        .file
        .unwrap_or_else(|| default_audit_path());

    if !path.exists() {
        eprintln!("log file not found: {}", path.display());
        eprintln!("hint: start the bot first with `edgeai serve telegram`");
        return Ok(());
    }

    let content = std::fs::read_to_string(&path)?;
    let lines: Vec<&str> = content.lines().rev().take(cmd.lines).collect();

    for line in lines.iter().rev() {
        if let Ok(e) = serde_json::from_str::<serde_json::Value>(line) {
            print_entry(&e);
        } else {
            println!("{line}");
        }
    }

    if cmd.follow {
        follow(&path)?;
    }

    Ok(())
}

fn follow(path: &PathBuf) -> Result<()> {
    use std::io::{BufRead, BufReader, Seek};

    let mut reader = BufReader::new(std::fs::File::open(path)?);
    reader.seek(std::io::SeekFrom::End(0))?;

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => {
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            Ok(_) => {
                let line = line.trim_end();
                if !line.is_empty() {
                    if let Ok(e) = serde_json::from_str::<serde_json::Value>(line) {
                        print_entry(&e);
                    } else {
                        println!("{line}");
                    }
                }
            }
            Err(e) => {
                eprintln!("read error: {e}");
                return Ok(());
            }
        }
    }
}

fn default_audit_path() -> PathBuf {
    std::env::var("HOME")
        .map(|h| PathBuf::from(h).join(".config/edgeai/state/audit.log.jsonl"))
        .unwrap_or_else(|_| PathBuf::from(".config/edgeai/state/audit.log.jsonl"))
}

fn print_entry(e: &serde_json::Value) {
    let ts_ms = e["ts_unix_ms"].as_u64().unwrap_or(0);
    let secs = ts_ms / 1000;
    let hours = (secs / 3600) % 24;
    let mins = (secs / 60) % 60;
    let s = secs % 60;
    let dt = format!("{:02}:{:02}:{:02}", hours, mins, s);

    let _source = e["source"].as_str().unwrap_or("?");
    let status = e["status"].as_str().unwrap_or("?");
    let phase = e["phase"].as_str().unwrap_or("");
    let chat_id = e["chat_id"].as_i64().unwrap_or(0);
    let command = e["command"].as_str().unwrap_or("");
    let error = e["error"].as_str().unwrap_or("");

    let status_color = match status {
        "finished" => "\x1b[32m",
        "failed" => "\x1b[31m",
        "started" => "\x1b[36m",
        _ => "\x1b[0m",
    };

    let phase_str = if phase.is_empty() {
        String::new()
    } else {
        format!("[{}] ", phase)
    };

    let error_str = if error.is_empty() {
        String::new()
    } else {
        format!(" \x1b[31m{error}\x1b[0m")
    };

    println!(
        "{status_color}{status}\x1b[0m \x1b[90m{dt}\x1b[0m {phase_str}\x1b[2m{command}\x1b[0m\x1b[90m (chat:{chat_id})\x1b[0m{error_str}"
    );
}
