# edgeai

`edgeai` is a Rust CLI that bridges Telegram with a selected local LLM toolchain such as Claude Code, Codex CLI, OpenCode, OpenClaw, Hermes, or a custom OpenAI-compatible model API.

The Telegram transport is implemented with `frankenstein` and Telegram Bot API long polling.

## Current shape

- `edgeai init`: interactive initial setup wizard
- `edgeai exec`: run one local command through the shared shell execution layer
- `edgeai config show`: inspect effective runtime config
- `edgeai serve telegram`: start a Telegram long-polling bot backed by per-chat LLM sessions

## Architecture

- `src/cli`: clap argument definitions
- `src/commands`: top-level command dispatch and init wizard
- `src/config`: user config loading, env merge, and config persistence
- `src/llm`: provider registry, provider execution, and per-chat session persistence
- `src/audit`: append-only JSONL audit logging
- `src/session`: local persistent shell sessions for `edgeai exec --session`
- `src/shell`: local shell execution boundary
- `src/transport`: IM transport adapters

## Session model

- For Telegram private chats and non-forum groups, each chat can hold multiple local threads and one active thread at a time.
- For Telegram forum groups, each Telegram topic maps to its own LLM conversation session.
- Provider-native session IDs are reused when the backend supports them.
- When a provider does not expose a stable session continuation API, `edgeai` falls back to replaying recent transcript history.
- Different chats remain isolated from each other.
- Control commands: `/start`, `/help`, `/news_watch`, `/smart_money`, `/stop`, `/reset`, `/threads`
- Transport state such as Telegram update offsets is stored under the user config state directory.
- Audit events are stored in JSON Lines format.

## Example

```bash
edgeai init
edgeai config show
edgeai serve telegram
```

## Installation

**macOS / Linux (Homebrew):**
```bash
brew tap edgehunt-ai/edgeai
brew install edgeai
```

**Linux / macOS (install script):**
```bash
curl -fsSL https://raw.githubusercontent.com/edgehunt-ai/edgeai-bot/main/scripts/install.sh | bash
```

This installs `edgeai` into `~/.edgeai/bin` and adds that directory to your shell `PATH`.

**Windows (PowerShell):**
```bash
powershell -ExecutionPolicy Bypass -Command "iwr https://raw.githubusercontent.com/edgehunt-ai/edgeai-bot/main/scripts/install.ps1 -UseBasicParsing | iex"
```

This installs `edgeai.exe` into `%USERPROFILE%\.edgeai\bin` and adds that directory to your user `PATH`.

## Telegram transport

- Long polling via `getUpdates`
- Persistent `update_id` offset file so restarts do not re-consume old updates
- Private chats and non-forum groups support multiple local threads per `chat.id`
- Telegram forum groups use native Telegram topics as the conversation boundary
- Active threads preserve conversation state through provider-native session IDs when available
- Different chats remain isolated
- Supported control commands: `/start`, `/help`, `/news_watch`, `/smart_money`, `/stop`, `/reset`, `/threads`

Messages are forwarded to the configured LLM backend for allowed chats only. Replies are chunked before sending back to Telegram to stay under message length limits.

`/threads` behavior depends on chat type:

- Forum topics: `/threads`, `/threads new [title]`, `/threads rename <title>`, `/threads delete`
- Private chats or non-forum groups: `/threads`, `/threads new [title]`, `/threads use <id>`

## Running And Logs

**Run the service in release mode:**
```bash
edgeai serve telegram
```

**Run the service with debug logs enabled:**
```bash
RUST_LOG=edgeai=debug edgeai serve telegram
```

`edgeai` only initializes Rust tracing when `RUST_LOG` is set, so if you want runtime debug output, set it explicitly.

**Inspect audit logs from another terminal:**
```bash
edgeai logs --follow
```

**Read the raw audit log file directly:**
```bash
tail -f ~/.config/edgeai/state/audit.log.jsonl
```

## Config

- Default config file: `~/.config/edgeai/config.json`
- Default state directory: `~/.config/edgeai/state`
- Default Telegram offset file: `~/.config/edgeai/state/telegram-offset.txt`
- Default runtime timeout: `300` seconds
- Default Telegram poll interval: `2` seconds minimum
- Useful environment variables:
  - `EDGEAI_SHELL`
  - `EDGEAI_CWD`
  - `EDGEAI_TIMEOUT_SECS`
  - `EDGEAI_STATE_DIR`
  - `EDGEAI_AUDIT_LOG_FILE`
  - `TELEGRAM_BOT_TOKEN`
  - `TELEGRAM_ALLOWED_CHAT_IDS`
  - `TELEGRAM_POLL_INTERVAL_SECS`
  - `TELEGRAM_OFFSET_FILE`
- `edgeai init` can:
  - detect installed `claude` / `codex` / `opencode` / `openclaw` / `hermes`
  - optionally install a missing CLI with the provider's recommended install command
  - configure Telegram bot token and allowed chat IDs
  - configure a custom OpenAI-compatible model API endpoint, model, and API key

## Audit log

- Default path: `~/.config/edgeai/state/audit.log.jsonl`
- One JSON object per line
- Records inbound Telegram messages and success/failure metadata
- Does not persist full stdout/stderr by default
