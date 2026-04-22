use crate::audit::{AuditEvent, AuditLogger, now_unix_ms};
use crate::claude_mcp::append_always_allow_command;
use crate::cli::serve::TelegramServeCmd;
use crate::config::{RuntimeConfig, TelegramConfig};
use crate::llm::{
    LlmApprovalChoice, LlmApprovalDecision, LlmApprovalRequest, LlmClient, LlmStreamEvent,
};
use anyhow::{Context, Result, bail};
use chrono::Local;
use frankenstein::AsyncTelegramApi;
use frankenstein::client_reqwest::Bot;
use frankenstein::methods::{
    AnswerCallbackQueryParams, CreateForumTopicParams, DeleteForumTopicParams, DeleteMessageParams,
    DeleteWebhookParams, EditForumTopicParams, EditMessageTextParams, GetMyDescriptionParams,
    GetUpdatesParams, SendChatActionParams, SendMessageParams, SetMyCommandsParams,
    SetMyDescriptionParams,
};
use frankenstein::types::{
    AllowedUpdate, BotCommand, BotDescription, ChatAction, ForceReply, InlineKeyboardButton,
    InlineKeyboardMarkup, MaybeInaccessibleMessage, Message, ReplyMarkup,
};
use frankenstein::updates::UpdateContent;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, sleep, timeout};
use uuid::Uuid;

const TELEGRAM_REPLY_CHUNK_LIMIT: usize = 3500;
const TELEGRAM_REPLY_TOTAL_LIMIT: usize = 12000;
const TELEGRAM_STREAM_UPDATE_INTERVAL_MS: u128 = 800;
const TELEGRAM_STREAM_MIN_DELTA_CHARS: usize = 24;
const TELEGRAM_TYPING_ACTION_INTERVAL_MS: u128 = 4000;
const TELEGRAM_STREAM_THINKING_SUFFIX: &str = "🧠thinking...";
const SHORTCUT_NEWS_WATCH: &str = "/news_watch";
const SHORTCUT_SMART_MONEY: &str = "/smart_money";
const PROMPT_NEWS_WATCH: &str = "What major news today has impacted the prediction markets";
const PROMPT_SMART_MONEY: &str = "Recommend the top 5 smart money traders for me";
const COMMAND_THREADS: &str = "/threads";
const COMMAND_STOP: &str = "/stop";
const CB_WALLET_MENU: &str = "cb_wallet_menu";
const CB_ONCHAIN_TOOLS: &str = "cb_onchain_tools";
const CB_PREDICT_MARKET: &str = "cb_predict_market";
const CB_SESSION_MGMT: &str = "cb_session_mgmt";
const CB_WALLET_NEW: &str = "cb_wallet_new";
const CB_WALLET_IMPORT: &str = "cb_wallet_import";
const CB_WALLET_SELECT_LIST: &str = "cb_wallet_select_list";
const CB_WALLET_DELETE_LIST: &str = "cb_wallet_delete_list";
const CB_WALLET_SELECT_PREFIX: &str = "cb_select:";
const CB_WALLET_DELETE_PREFIX: &str = "cb_delete:";
const CB_THREADS_NEW: &str = "cb_threads_new";
const CB_THREADS_RESET: &str = "cb_threads_reset";
const CB_THREADS_RENAME: &str = "cb_threads_rename";
const CB_THREADS_DELETE: &str = "cb_threads_delete";
const CB_USE_THREAD_PREFIX: &str = "cb_use_thread:";

#[derive(Debug, Serialize)]
pub struct TelegramStatus {
    pub shell: String,
    pub cwd: String,
    pub timeout_secs: u64,
    pub allowed_chat_ids: Vec<i64>,
    pub poll_interval_secs: u64,
    pub offset_file: String,
    pub llm_provider: String,
    pub status: String,
    pub note: String,
    pub session_mode: String,
}

pub struct TelegramTransport {
    runtime: RuntimeConfig,
    config: TelegramConfig,
    llm: LlmClient,
    chat_sessions: ChatSessionStore,
    topic_state: TopicStateStore,
    scope_locks: ScopeLockStore,
    typing_actions: TypingActionStore,
    active_runs: ActiveRunStore,
    wallet_wizard: WalletWizardStore,
    active_wallet: ActiveWalletStore,
    bot: Bot,
    audit: AuditLogger,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct TelegramReply {
    pub chat_id: i64,
    pub message_thread_id: Option<i32>,
    pub session_id: String,
    pub body: String,
    #[serde(skip)]
    pub reply_markup: Option<InlineKeyboardMarkup>,
}

enum MessageAction {
    Immediate {
        session_id: String,
        message_thread_id: Option<i32>,
        body: String,
        extra_messages: Vec<PendingTelegramMessage>,
        post_action: Option<PostAction>,
        reply_markup: Option<InlineKeyboardMarkup>,
    },
    LlmPrompt {
        session_id: String,
        message_thread_id: Option<i32>,
        prompt: String,
    },
}

struct PendingTelegramMessage {
    message_thread_id: Option<i32>,
    body: String,
}

enum PostAction {
    DeleteTopic {
        chat_id: i64,
        message_thread_id: i32,
    },
}

struct NewThreadResult {
    session_id: String,
    created_topic_id: Option<i32>,
    created_topic_name: Option<String>,
}

enum ThreadsCommand<'a> {
    Show,
    New { title: Option<&'a str> },
    Rename { title: &'a str },
    Delete,
    Use { id: &'a str },
}

impl<'a> ThreadsCommand<'a> {
    fn parse(input: &'a str) -> Result<Option<Self>> {
        let Some(rest) = thread_command_rest(input) else {
            return Ok(None);
        };

        if rest.is_empty() {
            return Ok(Some(Self::Show));
        }

        if let Some(rest) = rest.strip_prefix("new") {
            let title = rest.trim();
            return Ok(Some(Self::New {
                title: if title.is_empty() { None } else { Some(title) },
            }));
        }

        if let Some(rest) = rest.strip_prefix("rename") {
            let title = rest.trim();
            if title.is_empty() {
                bail!("usage: /threads rename <new title>");
            }
            return Ok(Some(Self::Rename { title }));
        }

        if rest == "delete" {
            return Ok(Some(Self::Delete));
        }

        if let Some(rest) = rest.strip_prefix("use") {
            let id = rest.trim();
            if id.is_empty() {
                bail!("usage: /threads use <id>");
            }
            return Ok(Some(Self::Use { id }));
        }

        bail!(
            "unknown threads command; use /threads, /threads new [title], /threads rename <title>, /threads delete, or /threads use <id>"
        )
    }
}

#[derive(Default)]
struct StreamedTelegramReply {
    message_ids: Vec<i32>,
    last_chunks: Vec<String>,
    last_body: String,
    last_rendered_body: String,
    last_markup_key: Option<String>,
    approval_request: Option<TelegramApprovalPrompt>,
    last_rendered_at: u128,
}

#[derive(Debug, Clone)]
struct TelegramApprovalPrompt {
    request_id: String,
    summary: String,
    command: Option<String>,
    allow_accept_for_session: bool,
    allow_cancel: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedChatSessions {
    chats: HashMap<String, ChatSessions>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ChatSessions {
    active_session_id: Option<String>,
    sessions: Vec<ChatThread>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatThread {
    id: String,
    created_at_unix_ms: u128,
}

struct ChatSessionStore {
    path: PathBuf,
    inner: Mutex<PersistedChatSessions>,
}

struct ScopeLockStore {
    inner: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

#[derive(Debug, Clone, Default)]
struct TypingActionState {
    last_sent_at: u128,
    blocked_until: u128,
}

struct TypingActionStore {
    inner: Mutex<HashMap<String, TypingActionState>>,
}

struct ActiveRunStore {
    inner: Mutex<HashMap<String, VecDeque<ActiveRun>>>,
}

struct ActiveRun {
    id: String,
    abort_handle: tokio::task::AbortHandle,
    approval_tx: mpsc::UnboundedSender<LlmApprovalDecision>,
    pending_approval_request_id: Option<String>,
    pending_approval_command: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct TopicState {
    manual_named: bool,
    auto_renamed: bool,
}

struct TopicStateStore {
    inner: Mutex<HashMap<String, TopicState>>,
}

impl TelegramTransport {
    pub async fn from_config(
        runtime: RuntimeConfig,
        config: TelegramConfig,
        llm: LlmClient,
        cli: TelegramServeCmd,
    ) -> Result<Self> {
        let config = config.merge_with_cli(cli);
        let token = config.bot_token.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "telegram bot token is required; run `edgeai init` or set TELEGRAM_BOT_TOKEN"
            )
        })?;

        Ok(Self {
            bot: Bot::new(&token),
            audit: AuditLogger::new(runtime.audit_log_file.clone()),
            chat_sessions: ChatSessionStore::load(runtime.telegram_chat_sessions_file.clone())
                .await?,
            topic_state: TopicStateStore::default(),
            scope_locks: ScopeLockStore::default(),
            typing_actions: TypingActionStore::default(),
            active_runs: ActiveRunStore::default(),
            wallet_wizard: WalletWizardStore::default(),
            active_wallet: ActiveWalletStore::load(runtime.active_wallet_file.clone()).await,
            runtime,
            config,
            llm,
        })
    }

    pub fn describe(&self) -> Result<TelegramStatus> {
        Ok(TelegramStatus {
            shell: self.runtime.shell.clone(),
            cwd: self.runtime.cwd.display().to_string(),
            timeout_secs: self.runtime.timeout_secs,
            allowed_chat_ids: self.config.allowed_chat_ids.clone(),
            poll_interval_secs: self.config.poll_interval_secs,
            offset_file: self
                .config
                .offset_file
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<unset>".to_string()),
            llm_provider: self.llm.provider_label(),
            status: "not_started".to_string(),
            note: "transport is prepared for per-chat multi-thread llm conversations".to_string(),
            session_mode: "multiple logical threads per chat_id/topic, one active thread at a time"
                .to_string(),
        })
    }

    async fn build_message_action(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        is_forum: bool,
        text: &str,
    ) -> Result<MessageAction> {
        self.ensure_chat_allowed(chat_id)?;

        let trimmed = text.trim();
        let scope_key = scope_key(chat_id, message_thread_id);
        let shortcut_prompt = shortcut_prompt(trimmed);
        let threads_command = ThreadsCommand::parse(trimmed)?;
        let new_thread_result = if is_help_command(trimmed) {
            NewThreadResult {
                session_id: self
                    .resolve_active_thread(chat_id, message_thread_id, is_forum)
                    .await?,
                created_topic_id: None,
                created_topic_name: None,
            }
        } else if matches_telegram_command(trimmed, "/reset") {
            NewThreadResult {
                session_id: self
                    .resolve_active_thread(chat_id, message_thread_id, is_forum)
                    .await?,
                created_topic_id: None,
                created_topic_name: None,
            }
        } else if let Some(ThreadsCommand::New { title }) = threads_command {
            self.create_new_thread(chat_id, message_thread_id, is_forum, title)
                .await?
        } else if let Some(ThreadsCommand::Use { id }) = threads_command {
            if is_forum {
                bail!("forum topics use native Telegram threads; switch topics in Telegram UI");
            }
            NewThreadResult {
                session_id: self.chat_sessions.switch_to(&scope_key, id).await?,
                created_topic_id: None,
                created_topic_name: None,
            }
        } else if threads_command.is_some() {
            NewThreadResult {
                session_id: self
                    .resolve_active_thread(chat_id, message_thread_id, is_forum)
                    .await?,
                created_topic_id: None,
                created_topic_name: None,
            }
        } else {
            NewThreadResult {
                session_id: self
                    .resolve_active_thread(chat_id, message_thread_id, is_forum)
                    .await?,
                created_topic_id: None,
                created_topic_name: None,
            }
        };
        let session_id = new_thread_result.session_id.clone();

        if matches_telegram_command(trimmed, "/start") {
            return Ok(MessageAction::Immediate {
                session_id,
                message_thread_id,
                body: start_welcome_text(),
                extra_messages: Vec::new(),
                post_action: None,
                reply_markup: Some(start_keyboard()),
            });
        }

        if matches_telegram_command(trimmed, "/wallet") {
            return Ok(MessageAction::Immediate {
                session_id,
                message_thread_id,
                body: wallet_menu_text(),
                extra_messages: Vec::new(),
                post_action: None,
                reply_markup: Some(wallet_menu_keyboard()),
            });
        }

        if matches_telegram_command(trimmed, "/market") {
            return Ok(MessageAction::Immediate {
                session_id,
                message_thread_id,
                body: "📈 Prediction Markets\n\nSelect a direction to query:".to_string(),
                extra_messages: Vec::new(),
                post_action: None,
                reply_markup: Some(predict_market_keyboard()),
            });
        }

        if is_help_command(trimmed) {
            return Ok(MessageAction::Immediate {
                session_id,
                message_thread_id,
                body: help_text(),
                extra_messages: Vec::new(),
                post_action: None,
                reply_markup: Some(start_keyboard()),
            });
        }

        if let Some(prompt) = shortcut_prompt {
            return Ok(MessageAction::LlmPrompt {
                session_id,
                message_thread_id,
                prompt: prompt.to_string(),
            });
        }

        let (body, extra_messages, post_action) = if let Some(ThreadsCommand::New { .. }) =
            threads_command
        {
            if is_forum {
                let created_topic_id = new_thread_result.created_topic_id;
                let created_topic_name = new_thread_result
                    .created_topic_name
                    .clone()
                    .unwrap_or_else(|| forum_topic_name());
                (
                    format!(
                        "new Telegram topic created\nid: {}\ntitle: {}\nopen the new topic to continue there",
                        created_topic_id.unwrap_or_default(),
                        created_topic_name
                    ),
                    created_topic_id
                        .map(|thread_id| {
                            vec![PendingTelegramMessage {
                                message_thread_id: Some(thread_id),
                                body: "new topic ready\nsend messages here to continue this thread"
                                    .to_string(),
                            }]
                        })
                        .unwrap_or_default(),
                    None,
                )
            } else {
                (
                    format!(
                        "new thread created and selected: {}\nuse /threads to view all threads",
                        short_session_id(&session_id)
                    ),
                    Vec::new(),
                    None,
                )
            }
        } else if let Some(ThreadsCommand::Rename { title }) = threads_command {
            let Some(thread_id) = message_thread_id else {
                bail!("threads rename is only available inside a Telegram forum topic");
            };
            if !is_forum {
                bail!("threads rename is only available inside a Telegram forum topic");
            }
            self.bot
                .edit_forum_topic(
                    &EditForumTopicParams::builder()
                        .chat_id(chat_id)
                        .message_thread_id(thread_id)
                        .name(title.to_string())
                        .build(),
                )
                .await?;
            self.topic_state.mark_manual_named(chat_id, thread_id).await;
            (format!("topic renamed to: {}", title), Vec::new(), None)
        } else if let Some(ThreadsCommand::Delete) = threads_command {
            let Some(thread_id) = message_thread_id else {
                bail!("threads delete is only available inside a Telegram forum topic");
            };
            if !is_forum {
                bail!("threads delete is only available inside a Telegram forum topic");
            }
            (
                "topic will be deleted now".to_string(),
                Vec::new(),
                Some(PostAction::DeleteTopic {
                    chat_id,
                    message_thread_id: thread_id,
                }),
            )
        } else if matches_telegram_command(trimmed, "/reset") {
            let _ = self.llm.reset_session(&session_id).await?;
            if is_forum {
                (
                    format!(
                        "current topic context cleared: {}\nthe Telegram topic remains, but the AI session history was reset",
                        short_session_id(&session_id)
                    ),
                    Vec::new(),
                    None,
                )
            } else {
                let fresh_session_id = self.chat_sessions.reset_current(&scope_key).await?;
                (
                    format!(
                        "current thread history cleared\nold thread removed: {}\nnew empty thread selected: {}",
                        short_session_id(&session_id),
                        short_session_id(&fresh_session_id)
                    ),
                    Vec::new(),
                    None,
                )
            }
        } else if let Some(ThreadsCommand::Show) = threads_command {
            let body = self.render_chat_sessions(chat_id, message_thread_id, is_forum).await?;
            let keyboard = self.session_management_keyboard(chat_id, message_thread_id, is_forum).await;
            return Ok(MessageAction::Immediate {
                session_id,
                message_thread_id,
                body,
                extra_messages: Vec::new(),
                post_action: None,
                reply_markup: Some(keyboard),
            });
        } else if let Some(ThreadsCommand::Use { .. }) = threads_command {
            (
                format!("switched to thread: {}", short_session_id(&session_id)),
                Vec::new(),
                None,
            )
        } else {
            return Ok(MessageAction::LlmPrompt {
                session_id,
                message_thread_id,
                prompt: trimmed.to_string(),
            });
        };

        Ok(MessageAction::Immediate {
            session_id: session_id.clone(),
            message_thread_id,
            body,
            extra_messages,
            post_action,
            reply_markup: None,
        })
    }

    fn ensure_chat_allowed(&self, chat_id: i64) -> Result<()> {
        if self.config.allowed_chat_ids.is_empty()
            || self.config.allowed_chat_ids.contains(&chat_id)
        {
            Ok(())
        } else {
            bail!("chat {} is not allowed", chat_id)
        }
    }

    pub async fn run_prompt(
        &self,
        chat_id: i64,
        thread_id: Option<i32>,
        prompt: &str,
    ) -> Result<()> {
        self.ensure_chat_allowed(chat_id)?;
        let session_id = format!("run:{}:{}", chat_id, uuid::Uuid::new_v4());
        let reply_text = if self.llm.supports_streaming() {
            let (approval_tx, approval_rx) = mpsc::unbounded_channel::<LlmApprovalDecision>();
            let collected = Arc::new(Mutex::new(String::new()));
            let collected_ref = Arc::clone(&collected);
            let ask_result = self
                .llm
                .ask_streaming(&session_id, prompt, None, approval_rx, |event| {
                    let tx = approval_tx.clone();
                    let collected = Arc::clone(&collected_ref);
                    async move {
                        match event {
                            LlmStreamEvent::Content(text) => {
                                *collected.lock().await = text;
                            }
                            LlmStreamEvent::ApprovalRequested(request) => {
                                let _ = tx.send(LlmApprovalDecision {
                                    request_id: request.request_id,
                                    choice: LlmApprovalChoice::Decline,
                                });
                            }
                            _ => {}
                        }
                    }
                })
                .await?;
            ask_result.text
        } else {
            self.llm.ask(&session_id, prompt).await?.text
        };

        self.send_reply_chunks(chat_id, thread_id, &reply_text, None)
            .await
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        self.bot
            .delete_webhook(
                &DeleteWebhookParams::builder()
                    .drop_pending_updates(false)
                    .build(),
            )
            .await?;
        self.register_commands().await?;
        self.ensure_bot_description().await?;

        let me = self.bot.get_me().await?.result;
        println!(
            "telegram transport started for @{}",
            me.username.unwrap_or_else(|| "<unnamed-bot>".to_string())
        );

        tokio::spawn({
            let transport = Arc::clone(&self);
            async move { transport.run_cron_scheduler().await }
        });

        let offset_path = self
            .config
            .offset_file
            .as_ref()
            .context("telegram offset file is not configured")?;
        let mut offset = load_offset(offset_path)?;

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("telegram transport stopping");
                    return Ok(());
                }
                result = self.poll_once(offset) => {
                    match result {
                        Ok(next_offset) => {
                            offset = next_offset;
                            save_offset(offset_path, offset)?;
                        }
                        Err(error) => {
                            eprintln!("telegram poll error: {error}");
                            sleep(Duration::from_secs(self.config.poll_interval_secs.max(1))).await;
                        }
                    }
                }
            }
        }
    }

    async fn poll_once(self: &Arc<Self>, offset: Option<i64>) -> Result<Option<i64>> {
        let params = if let Some(offset) = offset {
            GetUpdatesParams::builder()
                .allowed_updates(vec![AllowedUpdate::Message, AllowedUpdate::CallbackQuery])
                .timeout(self.config.poll_interval_secs as u32)
                .offset(offset)
                .build()
        } else {
            GetUpdatesParams::builder()
                .allowed_updates(vec![AllowedUpdate::Message, AllowedUpdate::CallbackQuery])
                .timeout(self.config.poll_interval_secs as u32)
                .build()
        };
        let updates = self.bot.get_updates(&params).await?.result;

        let mut next_offset = offset;
        for update in updates {
            next_offset = Some((update.update_id + 1).into());
            match update.content {
                UpdateContent::Message(message) => {
                    if is_stop_message(&message) {
                        let transport = Arc::clone(self);
                        tokio::spawn(async move {
                            if let Err(error) = transport.process_stop_message(*message).await {
                                eprintln!("telegram stop command error: {error}");
                            }
                        });
                        continue;
                    }

                    let transport = Arc::clone(self);
                    let scope = scope_key(message.chat.id, message.message_thread_id);
                    let (approval_tx, approval_rx) = mpsc::unbounded_channel();
                    let run_id = Uuid::new_v4().to_string();
                    let task_run_id = run_id.clone();
                    let join = tokio::spawn(async move {
                        if let Err(error) = transport
                            .process_message(*message, approval_rx, task_run_id)
                            .await
                        {
                            eprintln!("telegram message processing error: {error}");
                        }
                    });
                    self.active_runs
                        .insert(scope, run_id, join.abort_handle(), approval_tx)
                        .await;
                }
                UpdateContent::CallbackQuery(callback_query) => {
                    let transport = Arc::clone(self);
                    tokio::spawn(async move {
                        if let Err(error) = transport.process_callback_query(*callback_query).await
                        {
                            eprintln!("telegram callback query error: {error}");
                        }
                    });
                }
                _ => {}
            }
        }

        Ok(next_offset)
    }

    async fn process_stop_message(self: Arc<Self>, message: Message) -> Result<()> {
        let chat_id = message.chat.id;
        let message_thread_id = message.message_thread_id;
        let scope = scope_key(chat_id, message_thread_id);
        let stopped = self.active_runs.abort(&scope).await;
        let body = if stopped {
            "stopped the current LLM task"
        } else {
            "no active LLM task to stop"
        };
        self.send_message_to_location(chat_id, message_thread_id, body)
            .await?;
        Ok(())
    }

    async fn process_callback_query(
        self: Arc<Self>,
        callback_query: frankenstein::types::CallbackQuery,
    ) -> Result<()> {
        let data = callback_query.data.as_deref().unwrap_or_default();

        if let Some(prompt) = shortcut_prompt(data) {
            let Some(message) = callback_query.message.as_ref() else {
                return Ok(());
            };
            let (chat_id, message_thread_id) = callback_message_location(message);
            self.bot
                .answer_callback_query(
                    &AnswerCallbackQueryParams::builder()
                        .callback_query_id(callback_query.id.clone())
                        .text("Processing...".to_string())
                        .build(),
                )
                .await?;
            if let Err(e) = self.ensure_chat_allowed(chat_id) {
                eprintln!("callback shortcut blocked: {e}");
                return Ok(());
            }
            let session_id = self
                .resolve_active_thread(
                    chat_id,
                    message_thread_id,
                    callback_message_is_forum_topic(message),
                )
                .await?;
            let scope = scope_key(chat_id, message_thread_id);
            let run_id = Uuid::new_v4().to_string();
            let (approval_tx, approval_rx) = mpsc::unbounded_channel();
            let task_run_id = run_id.clone();
            let transport = Arc::clone(&self);
            let join = tokio::spawn(async move {
                if let Err(e) = transport
                    .run_shortcut_prompt(
                        chat_id,
                        message_thread_id,
                        session_id,
                        prompt,
                        approval_rx,
                        task_run_id,
                    )
                    .await
                {
                    eprintln!("shortcut llm error: {e}");
                }
            });
            self.active_runs
                .insert(scope, run_id, join.abort_handle(), approval_tx)
                .await;
            return Ok(());
        }

        if is_nav_callback(data) {
            let Some(message) = callback_query.message.as_ref() else {
                return Ok(());
            };
            let (chat_id, message_thread_id) = callback_message_location(message);
            let is_forum = callback_message_is_forum_topic(message);
            self.bot
                .answer_callback_query(
                    &AnswerCallbackQueryParams::builder()
                        .callback_query_id(callback_query.id)
                        .build(),
                )
                .await?;
            if let Err(e) = self.ensure_chat_allowed(chat_id) {
                eprintln!("nav callback blocked: {e}");
                return Ok(());
            }
            let scope = scope_key(chat_id, message_thread_id);
            if let Err(e) = self
                .handle_nav_callback(chat_id, message_thread_id, &scope, data, is_forum)
                .await
            {
                eprintln!("nav callback error: {e}");
            }
            return Ok(());
        }

        let Some((decision, request_id)) = parse_approval_callback_data(data) else {
            self.bot
                .answer_callback_query(
                    &AnswerCallbackQueryParams::builder()
                        .callback_query_id(callback_query.id)
                        .text("unsupported action")
                        .build(),
                )
                .await?;
            return Ok(());
        };

        let Some(message) = callback_query.message.as_ref() else {
            self.bot
                .answer_callback_query(
                    &AnswerCallbackQueryParams::builder()
                        .callback_query_id(callback_query.id)
                        .text("approval message is no longer available")
                        .build(),
                )
                .await?;
            return Ok(());
        };
        let (chat_id, message_thread_id) = callback_message_location(message);
        let scope = scope_key(chat_id, message_thread_id);

        if decision == LlmApprovalChoice::Cancel {
            self.active_runs.abort(&scope).await;
            if let MaybeInaccessibleMessage::Message(msg) = message {
                let current = msg.text.as_deref().unwrap_or("").trim_end();
                let stripped = current
                    .strip_suffix(TELEGRAM_STREAM_THINKING_SUFFIX)
                    .unwrap_or(current)
                    .trim_end();
                let final_text = if stripped.is_empty() {
                    "⛔ Session terminated".to_string()
                } else {
                    format!("{stripped}\n\n⛔ Session terminated")
                };
                let _ = self
                    .bot
                    .edit_message_text(
                        &EditMessageTextParams::builder()
                            .chat_id(chat_id)
                            .message_id(msg.message_id)
                            .text(final_text)
                            .reply_markup(InlineKeyboardMarkup::builder().inline_keyboard(vec![]).build())
                            .build(),
                    )
                    .await;
            }
            self.bot
                .answer_callback_query(
                    &AnswerCallbackQueryParams::builder()
                        .callback_query_id(callback_query.id)
                        .text("Terminated".to_string())
                        .build(),
                )
                .await?;
            return Ok(());
        }

        let always_allow = decision == LlmApprovalChoice::AlwaysAllow;
        let submit_choice = if always_allow {
            LlmApprovalChoice::Accept
        } else {
            decision
        };
        let (submitted, command) = self
            .active_runs
            .submit_approval(
                &scope,
                LlmApprovalDecision {
                    request_id,
                    choice: submit_choice,
                },
            )
            .await;

        if always_allow && submitted {
            if let Some(cmd) = command {
                let _ = append_always_allow_command(&self.runtime.state_dir, &cmd).await;
            }
        }

        let text = if submitted {
            "approval submitted"
        } else {
            "no active approval"
        };
        self.bot
            .answer_callback_query(
                &AnswerCallbackQueryParams::builder()
                    .callback_query_id(callback_query.id)
                    .text(text.to_string())
                    .build(),
            )
            .await?;
        Ok(())
    }

    async fn process_message(
        self: Arc<Self>,
        message: Message,
        approval_rx: mpsc::UnboundedReceiver<LlmApprovalDecision>,
        run_id: String,
    ) -> Result<()> {
        let chat_id = message.chat.id;
        let message_thread_id = message.message_thread_id;
        let scope = scope_key(chat_id, message_thread_id);
        let Some(text) = message.text.as_deref() else {
            self.active_runs.remove(&scope, &run_id).await;
            return Ok(());
        };
        let started_at = now_unix_ms();
        let is_forum = is_forum_message(&message);
        let scope_lock = self.scope_locks.get(&scope).await;
        let _scope_guard = scope_lock.lock().await;
        let current_session = self.peek_thread(chat_id, message_thread_id, is_forum).await;
        let username = message.from.as_ref().and_then(|user| user.username.clone());
        let user_id = message
            .from
            .as_ref()
            .and_then(|user| i64::try_from(user.id).ok());
        let trimmed_text = text.trim();

        if self.wallet_wizard.get(&scope).await.is_some() {
            let result = self
                .handle_wallet(chat_id, message_thread_id, message.message_id, trimmed_text)
                .await;
            self.active_runs.remove(&scope, &run_id).await;
            return result;
        }

        self.audit.append(&AuditEvent {
            ts_unix_ms: started_at,
            source: "telegram".to_string(),
            status: "started".to_string(),
            phase: Some("overall".to_string()),
            chat_id,
            session_id: current_session.clone(),
            user_id,
            username: username.clone(),
            command: trimmed_text.to_string(),
            exit_code: None,
            timed_out: None,
            duration_ms: None,
            llm_duration_ms: None,
            send_duration_ms: None,
            output_bytes: None,
            output_truncated: None,
            error: None,
        })?;

        self.send_typing_action(&scope, chat_id, message_thread_id)
            .await;

        let llm_started_at = now_unix_ms();
        self.audit.append(&AuditEvent {
            ts_unix_ms: llm_started_at,
            source: "telegram".to_string(),
            status: "started".to_string(),
            phase: Some("llm".to_string()),
            chat_id,
            session_id: current_session.clone(),
            user_id,
            username: username.clone(),
            command: trimmed_text.to_string(),
            exit_code: None,
            timed_out: None,
            duration_ms: None,
            llm_duration_ms: None,
            send_duration_ms: None,
            output_bytes: None,
            output_truncated: None,
            error: None,
        })?;

        let mut forum_topic_to_auto_rename = None;

        match self
            .build_message_action(chat_id, message_thread_id, is_forum, text)
            .await
        {
            Ok(action) => {
                let mut send_duration_ms = None;
                let (reply, streamed_send_done) = match action {
                    MessageAction::Immediate {
                        session_id,
                        message_thread_id,
                        body,
                        extra_messages,
                        post_action,
                        reply_markup,
                    } => {
                        for extra in extra_messages {
                            self.send_message_to_location(
                                chat_id,
                                extra.message_thread_id,
                                &extra.body,
                            )
                            .await?;
                        }
                        if let Some(post_action) = post_action {
                            self.run_post_action(post_action).await?;
                        }
                        (
                            TelegramReply {
                                chat_id,
                                message_thread_id,
                                session_id,
                                body,
                                reply_markup,
                            },
                            false,
                        )
                    }
                    MessageAction::LlmPrompt {
                        session_id,
                        message_thread_id,
                        prompt,
                    } => {
                        let is_new_session = self.llm.message_count(&session_id).await == 0;
                        let wallet_hint = self.active_wallet_hint().await;
                        let schedule_hint = is_new_session
                            .then(|| scheduled_task_system_hint(chat_id, message_thread_id));
                        let extra_system: Option<String> = match (wallet_hint, schedule_hint) {
                            (Some(w), Some(s)) => Some(format!("{w}\n\n{s}")),
                            (Some(w), None) => Some(w),
                            (None, Some(s)) => Some(s),
                            (None, None) => None,
                        };
                        if let Some(thread_id) = message_thread_id {
                            if is_forum
                                && is_new_session
                                && self
                                    .topic_state
                                    .should_auto_rename(chat_id, thread_id)
                                    .await
                            {
                                forum_topic_to_auto_rename = Some(thread_id);
                            }
                        }
                        if self.llm.supports_streaming() {
                            let send_started_at = now_unix_ms();
                            self.audit.append(&AuditEvent {
                                ts_unix_ms: send_started_at,
                                source: "telegram".to_string(),
                                status: "started".to_string(),
                                phase: Some("send".to_string()),
                                chat_id,
                                session_id: session_id.clone(),
                                user_id,
                                username: username.clone(),
                                command: text.trim().to_string(),
                                exit_code: None,
                                timed_out: None,
                                duration_ms: None,
                                llm_duration_ms: None,
                                send_duration_ms: None,
                                output_bytes: None,
                                output_truncated: None,
                                error: None,
                            })?;

                            let streamed = Arc::new(Mutex::new(StreamedTelegramReply::default()));
                            {
                                let mut streamed = streamed.lock().await;
                                streamed.last_body.clear();
                                self.sync_streamed_reply(
                                    &mut streamed,
                                    chat_id,
                                    message_thread_id,
                                    false,
                                )
                                .await?;
                            }
                            let streamed_updates = Arc::clone(&streamed);
                            let transport = Arc::clone(&self);
                            let approval_scope = scope.clone();
                            let ask_streaming_result = self
                                .llm
                                .ask_streaming(&session_id, &prompt, extra_system.as_deref(), approval_rx, |event| {
                                    let transport = Arc::clone(&transport);
                                    let streamed_updates = Arc::clone(&streamed_updates);
                                    let scope = approval_scope.clone();
                                    async move {
                                        let mut streamed = streamed_updates.lock().await;
                                        let should_sync = match event {
                                            LlmStreamEvent::Content(content) => {
                                                streamed.last_body = content;
                                                should_attempt_stream_sync(&streamed)
                                            }
                                            LlmStreamEvent::ApprovalRequested(request) => {
                                                let prompt =
                                                    TelegramApprovalPrompt::from(request.clone());
                                                streamed.last_body = prompt.summary.clone();
                                                streamed.approval_request = Some(prompt);
                                                let _ = transport
                                                    .active_runs
                                                    .set_pending_approval(&scope, request)
                                                    .await;
                                                true
                                            }
                                            LlmStreamEvent::ApprovalResolved { request_id } => {
                                                if streamed
                                                    .approval_request
                                                    .as_ref()
                                                    .map(|approval| {
                                                        approval.request_id == request_id
                                                    })
                                                    .unwrap_or(false)
                                                {
                                                    streamed.approval_request = None;
                                                }
                                                transport
                                                    .active_runs
                                                    .clear_pending_approval(&scope, &request_id)
                                                    .await;
                                                true
                                            }
                                        };
                                        if !should_sync {
                                            return;
                                        }
                                        let _ = transport
                                            .send_typing_action(&scope, chat_id, message_thread_id)
                                            .await;
                                        let _ = transport
                                            .sync_streamed_reply(
                                                &mut streamed,
                                                chat_id,
                                                message_thread_id,
                                                false,
                                            )
                                            .await;
                                    }
                                })
                                .await;

                            if ask_streaming_result.is_err() {
                                self.active_runs.remove(&scope, &run_id).await;
                            }
                            let llm_reply = match ask_streaming_result {
                                Ok(reply) => reply,
                                Err(error) => {
                                    let error_body = format!("error: {error}");
                                    let mut streamed = streamed.lock().await;
                                    streamed.last_body = error_body.clone();
                                    streamed.approval_request = None;
                                    if self
                                        .sync_streamed_reply(
                                            &mut streamed,
                                            chat_id,
                                            message_thread_id,
                                            true,
                                        )
                                        .await
                                        .is_err()
                                    {
                                        let _ = self
                                            .send_message_to_location(
                                                chat_id,
                                                message_thread_id,
                                                &error_body,
                                            )
                                            .await;
                                    }
                                    return Ok(());
                                }
                            };

                            let mut streamed = streamed.lock().await;
                            streamed.last_body = llm_reply.text.clone();
                            streamed.approval_request = None;
                            self.sync_streamed_reply(
                                &mut streamed,
                                chat_id,
                                message_thread_id,
                                true,
                            )
                            .await?;

                            let finished_at = now_unix_ms();
                            let current_send_duration = finished_at.saturating_sub(send_started_at);
                            send_duration_ms = Some(current_send_duration);
                            let truncated =
                                is_truncated(&llm_reply.text, TELEGRAM_REPLY_TOTAL_LIMIT);
                            self.audit.append(&AuditEvent {
                                ts_unix_ms: finished_at,
                                source: "telegram".to_string(),
                                status: "finished".to_string(),
                                phase: Some("send".to_string()),
                                chat_id,
                                session_id: session_id.clone(),
                                user_id,
                                username: username.clone(),
                                command: text.trim().to_string(),
                                exit_code: Some(0),
                                timed_out: Some(false),
                                duration_ms: Some(current_send_duration),
                                llm_duration_ms: None,
                                send_duration_ms: Some(current_send_duration),
                                output_bytes: Some(llm_reply.text.len()),
                                output_truncated: Some(truncated),
                                error: None,
                            })?;

                            (
                                TelegramReply {
                                    chat_id,
                                    message_thread_id,
                                    session_id,
                                    body: llm_reply.text,
                                    reply_markup: None,
                                },
                                true,
                            )
                        } else {
                            let llm_reply = match self.llm.ask(&session_id, &prompt).await {
                                Ok(reply) => reply,
                                Err(error) => {
                                    let error_body = format!("error: {error}");
                                    let _ = self
                                        .send_message_to_location(
                                            chat_id,
                                            message_thread_id,
                                            &error_body,
                                        )
                                        .await;
                                    self.active_runs.remove(&scope, &run_id).await;
                                    return Ok(());
                                }
                            };
                            (
                                TelegramReply {
                                    chat_id,
                                    message_thread_id,
                                    session_id,
                                    body: llm_reply.text,
                                    reply_markup: None,
                                },
                                false,
                            )
                        }
                    }
                };

                let llm_finished_at = now_unix_ms();
                let llm_duration_ms = llm_finished_at.saturating_sub(llm_started_at);
                let truncated = is_truncated(&reply.body, TELEGRAM_REPLY_TOTAL_LIMIT);
                self.audit.append(&AuditEvent {
                    ts_unix_ms: llm_finished_at,
                    source: "telegram".to_string(),
                    status: "finished".to_string(),
                    phase: Some("llm".to_string()),
                    chat_id,
                    session_id: reply.session_id.clone(),
                    user_id,
                    username: username.clone(),
                    command: text.trim().to_string(),
                    exit_code: Some(0),
                    timed_out: Some(false),
                    duration_ms: Some(llm_duration_ms),
                    llm_duration_ms: Some(llm_duration_ms),
                    send_duration_ms: send_duration_ms,
                    output_bytes: Some(reply.body.len()),
                    output_truncated: Some(truncated),
                    error: None,
                })?;

                if !streamed_send_done {
                    let send_started_at = now_unix_ms();
                    self.audit.append(&AuditEvent {
                        ts_unix_ms: send_started_at,
                        source: "telegram".to_string(),
                        status: "started".to_string(),
                        phase: Some("send".to_string()),
                        chat_id,
                        session_id: reply.session_id.clone(),
                        user_id,
                        username: username.clone(),
                        command: text.trim().to_string(),
                        exit_code: None,
                        timed_out: None,
                        duration_ms: None,
                        llm_duration_ms: Some(llm_duration_ms),
                        send_duration_ms: None,
                        output_bytes: Some(reply.body.len()),
                        output_truncated: Some(truncated),
                        error: None,
                    })?;

                    self.send_reply_chunks(
                        chat_id,
                        reply.message_thread_id,
                        &reply.body,
                        reply.reply_markup.clone(),
                    )
                    .await?;

                    let finished_at = now_unix_ms();
                    let current_send_duration = finished_at.saturating_sub(send_started_at);
                    send_duration_ms = Some(current_send_duration);
                    self.audit.append(&AuditEvent {
                        ts_unix_ms: finished_at,
                        source: "telegram".to_string(),
                        status: "finished".to_string(),
                        phase: Some("send".to_string()),
                        chat_id,
                        session_id: reply.session_id.clone(),
                        user_id,
                        username: username.clone(),
                        command: text.trim().to_string(),
                        exit_code: Some(0),
                        timed_out: Some(false),
                        duration_ms: Some(current_send_duration),
                        llm_duration_ms: Some(llm_duration_ms),
                        send_duration_ms: Some(current_send_duration),
                        output_bytes: Some(reply.body.len()),
                        output_truncated: Some(truncated),
                        error: None,
                    })?;
                }

                if let Some(thread_id) = forum_topic_to_auto_rename {
                    self.auto_rename_forum_topic(chat_id, thread_id, &reply.session_id)
                        .await;
                }

                let finished_at = now_unix_ms();
                self.audit.append(&AuditEvent {
                    ts_unix_ms: finished_at,
                    source: "telegram".to_string(),
                    status: "finished".to_string(),
                    phase: Some("overall".to_string()),
                    chat_id,
                    session_id: reply.session_id.clone(),
                    user_id,
                    username: username.clone(),
                    command: text.trim().to_string(),
                    exit_code: Some(0),
                    timed_out: Some(false),
                    duration_ms: Some(finished_at.saturating_sub(started_at)),
                    llm_duration_ms: Some(llm_duration_ms),
                    send_duration_ms,
                    output_bytes: Some(reply.body.len()),
                    output_truncated: Some(truncated),
                    error: None,
                })?;
                self.active_runs.remove(&scope, &run_id).await;
                return Ok(());
            }
            Err(error) => {
                let failed_at = now_unix_ms();
                let llm_duration_ms = failed_at.saturating_sub(llm_started_at);
                let error_text = error.to_string();
                self.audit.append(&AuditEvent {
                    ts_unix_ms: failed_at,
                    source: "telegram".to_string(),
                    status: "failed".to_string(),
                    phase: Some("llm".to_string()),
                    chat_id,
                    session_id: current_session.clone(),
                    user_id,
                    username: username.clone(),
                    command: text.trim().to_string(),
                    exit_code: None,
                    timed_out: Some(error_text.contains("timed out")),
                    duration_ms: Some(llm_duration_ms),
                    llm_duration_ms: Some(llm_duration_ms),
                    send_duration_ms: None,
                    output_bytes: None,
                    output_truncated: None,
                    error: Some(error_text.clone()),
                })?;

                let body = format!("error: {error_text}");
                let truncated = is_truncated(&body, TELEGRAM_REPLY_TOTAL_LIMIT);
                let send_started_at = now_unix_ms();
                self.audit.append(&AuditEvent {
                    ts_unix_ms: send_started_at,
                    source: "telegram".to_string(),
                    status: "started".to_string(),
                    phase: Some("send".to_string()),
                    chat_id,
                    session_id: current_session.clone(),
                    user_id,
                    username: username.clone(),
                    command: text.trim().to_string(),
                    exit_code: None,
                    timed_out: None,
                    duration_ms: None,
                    llm_duration_ms: Some(llm_duration_ms),
                    send_duration_ms: None,
                    output_bytes: Some(body.len()),
                    output_truncated: Some(truncated),
                    error: None,
                })?;

                for chunk in split_message(
                    &truncate_reply(&body, TELEGRAM_REPLY_TOTAL_LIMIT),
                    TELEGRAM_REPLY_CHUNK_LIMIT,
                ) {
                    let params = if let Some(thread_id) = message_thread_id {
                        SendMessageParams::builder()
                            .chat_id(chat_id)
                            .message_thread_id(thread_id)
                            .text(chunk)
                            .build()
                    } else {
                        SendMessageParams::builder()
                            .chat_id(chat_id)
                            .text(chunk)
                            .build()
                    };
                    self.bot.send_message(&params).await?;
                }

                let finished_at = now_unix_ms();
                let send_duration_ms = finished_at.saturating_sub(send_started_at);
                self.audit.append(&AuditEvent {
                    ts_unix_ms: finished_at,
                    source: "telegram".to_string(),
                    status: "failed".to_string(),
                    phase: Some("overall".to_string()),
                    chat_id,
                    session_id: current_session.clone(),
                    user_id,
                    username: username.clone(),
                    command: text.trim().to_string(),
                    exit_code: None,
                    timed_out: Some(error_text.contains("timed out")),
                    duration_ms: Some(finished_at.saturating_sub(started_at)),
                    llm_duration_ms: Some(llm_duration_ms),
                    send_duration_ms: Some(send_duration_ms),
                    output_bytes: Some(body.len()),
                    output_truncated: Some(truncated),
                    error: Some(error_text),
                })?;
                self.active_runs.remove(&scope, &run_id).await;
                return Ok(());
            }
        }
    }

    async fn send_typing_action(&self, scope: &str, chat_id: i64, message_thread_id: Option<i32>) {
        if !self.typing_actions.should_send(scope).await {
            return;
        }
        let params = if let Some(thread_id) = message_thread_id {
            SendChatActionParams::builder()
                .chat_id(chat_id)
                .message_thread_id(thread_id)
                .action(ChatAction::Typing)
                .build()
        } else {
            SendChatActionParams::builder()
                .chat_id(chat_id)
                .action(ChatAction::Typing)
                .build()
        };
        if let Err(error) = self.bot.send_chat_action(&params).await {
            self.typing_actions
                .record_failure(scope, parse_telegram_retry_after_secs(&error.to_string()))
                .await;
            eprintln!("telegram send_chat_action error: {error}");
        } else {
            self.typing_actions.record_success(scope).await;
        }
    }

    async fn send_reply_chunks(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        body: &str,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> Result<()> {
        let chunks = split_message(
            &truncate_reply(body, TELEGRAM_REPLY_TOTAL_LIMIT),
            TELEGRAM_REPLY_CHUNK_LIMIT,
        );
        let total = chunks.len();
        for (index, chunk) in chunks.into_iter().enumerate() {
            let is_last = index + 1 == total;
            let params = if let Some(thread_id) = message_thread_id {
                if is_last {
                    if let Some(markup) = reply_markup.clone() {
                        SendMessageParams::builder()
                            .chat_id(chat_id)
                            .message_thread_id(thread_id)
                            .text(chunk)
                            .reply_markup(ReplyMarkup::InlineKeyboardMarkup(markup))
                            .build()
                    } else {
                        SendMessageParams::builder()
                            .chat_id(chat_id)
                            .message_thread_id(thread_id)
                            .text(chunk)
                            .build()
                    }
                } else {
                    SendMessageParams::builder()
                        .chat_id(chat_id)
                        .message_thread_id(thread_id)
                        .text(chunk)
                        .build()
                }
            } else if is_last {
                if let Some(markup) = reply_markup.clone() {
                    SendMessageParams::builder()
                        .chat_id(chat_id)
                        .text(chunk)
                        .reply_markup(ReplyMarkup::InlineKeyboardMarkup(markup))
                        .build()
                } else {
                    SendMessageParams::builder()
                        .chat_id(chat_id)
                        .text(chunk)
                        .build()
                }
            } else {
                SendMessageParams::builder()
                    .chat_id(chat_id)
                    .text(chunk)
                    .build()
            };
            self.bot.send_message(&params).await?;
        }
        Ok(())
    }

    async fn sync_streamed_reply(
        &self,
        state: &mut StreamedTelegramReply,
        chat_id: i64,
        message_thread_id: Option<i32>,
        force: bool,
    ) -> Result<()> {
        let now = now_unix_ms();
        let markup_key = state
            .approval_request
            .as_ref()
            .map(|approval| approval.markup_key());
        let markup_changed = state.last_markup_key != markup_key;
        if !force
            && !markup_changed
            && !state.message_ids.is_empty()
            && now.saturating_sub(state.last_rendered_at) < TELEGRAM_STREAM_UPDATE_INTERVAL_MS
        {
            return Ok(());
        }
        if !force
            && !markup_changed
            && !should_render_stream_update(&state.last_rendered_body, &state.last_body)
        {
            return Ok(());
        }

        let rendered_body = render_streamed_body(&state.last_body, force);
        let chunks = split_message(
            &truncate_reply(&rendered_body, TELEGRAM_REPLY_TOTAL_LIMIT),
            TELEGRAM_REPLY_CHUNK_LIMIT,
        );
        if chunks.is_empty() {
            return Ok(());
        }
        let markup = state.approval_request.as_ref().map(approval_reply_markup);

        for (index, chunk) in chunks.iter().enumerate() {
            let reply_markup = if index + 1 == chunks.len() {
                markup.clone()
            } else {
                None
            };
            if let Some(message_id) = state.message_ids.get(index).copied() {
                let markup_changed =
                    index + 1 == chunks.len() && state.last_markup_key != markup_key;
                if state.last_chunks.get(index) == Some(chunk) && !markup_changed {
                    continue;
                }
                let params = if let Some(reply_markup) = reply_markup {
                    EditMessageTextParams::builder()
                        .chat_id(chat_id)
                        .message_id(message_id)
                        .text(chunk.clone())
                        .reply_markup(reply_markup)
                        .build()
                } else {
                    EditMessageTextParams::builder()
                        .chat_id(chat_id)
                        .message_id(message_id)
                        .text(chunk.clone())
                        .build()
                };
                self.bot.edit_message_text(&params).await?;
            } else {
                let params = if let Some(thread_id) = message_thread_id {
                    if let Some(reply_markup) = reply_markup {
                        SendMessageParams::builder()
                            .chat_id(chat_id)
                            .message_thread_id(thread_id)
                            .text(chunk.clone())
                            .reply_markup(ReplyMarkup::InlineKeyboardMarkup(reply_markup))
                            .build()
                    } else {
                        SendMessageParams::builder()
                            .chat_id(chat_id)
                            .message_thread_id(thread_id)
                            .text(chunk.clone())
                            .build()
                    }
                } else {
                    if let Some(reply_markup) = reply_markup {
                        SendMessageParams::builder()
                            .chat_id(chat_id)
                            .text(chunk.clone())
                            .reply_markup(ReplyMarkup::InlineKeyboardMarkup(reply_markup))
                            .build()
                    } else {
                        SendMessageParams::builder()
                            .chat_id(chat_id)
                            .text(chunk.clone())
                            .build()
                    }
                };
                let message = self.bot.send_message(&params).await?.result;
                state.message_ids.push(message.message_id);
            }
        }

        if chunks.len() < state.message_ids.len() || force {
            for message_id in state.message_ids[chunks.len()..].iter().copied() {
                let params = DeleteMessageParams::builder()
                    .chat_id(chat_id)
                    .message_id(message_id)
                    .build();
                self.bot.delete_message(&params).await?;
            }
            state.message_ids.truncate(chunks.len());
        }

        state.last_chunks = chunks;
        state.last_rendered_body = state.last_body.clone();
        state.last_markup_key = markup_key;
        state.last_rendered_at = now;
        Ok(())
    }

    async fn render_chat_sessions(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        is_forum: bool,
    ) -> Result<String> {
        if is_forum {
            return Ok(match message_thread_id {
                Some(thread_id) => format!(
                    "forum thread status:\ncurrent topic id: {}\ncurrent topic key: {}\n\nquick actions:\n- /threads rename <title>\n- /threads delete\n- /threads new [title]\n\nnotes:\n- keep chatting here to stay in this topic\n- use Telegram's topic list to open another topic",
                    thread_id,
                    scope_key(chat_id, Some(thread_id))
                ),
                None => "forum thread status:\nno active topic in this message context\n\nquick actions:\n- /threads new [title]\n\nnotes:\n- open an existing Telegram topic to work in that topic\n- use Telegram's topic list to choose a topic".to_string(),
            });
        }

        let view = self
            .chat_sessions
            .list(&scope_key(chat_id, message_thread_id))
            .await?;
        let mut lines = vec!["threads for this chat:".to_string()];
        if let Some(thread_id) = message_thread_id {
            lines[0] = format!("threads for this topic (telegram thread {}):", thread_id);
        }
        let active_session_id = view.active_session_id.clone();
        for session in &view.sessions {
            let active = if active_session_id.as_deref() == Some(session.id.as_str()) {
                " *"
            } else {
                ""
            };
            lines.push(format!(
                "- {}{}  /use {}",
                short_session_id(&session.id),
                active,
                short_session_id(&session.id)
            ));
        }
        if view.sessions.is_empty() {
            lines.push("- <none>".to_string());
        }
        lines.push(String::new());
        lines.push("subcommands:".to_string());
        lines.push("- /threads new [title]".to_string());
        lines.push("- /threads use <id>".to_string());
        lines.push("- /reset".to_string());
        Ok(lines.join("\n"))
    }

    async fn resolve_active_thread(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        is_forum: bool,
    ) -> Result<String> {
        if is_forum {
            Ok(scope_key(chat_id, message_thread_id))
        } else {
            self.chat_sessions
                .current_or_create(&scope_key(chat_id, message_thread_id))
                .await
        }
    }

    async fn create_new_thread(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        is_forum: bool,
        title: Option<&str>,
    ) -> Result<NewThreadResult> {
        if is_forum {
            let topic_name = title
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .unwrap_or_else(forum_topic_name);
            let topic = self
                .bot
                .create_forum_topic(
                    &CreateForumTopicParams::builder()
                        .chat_id(chat_id)
                        .name(topic_name)
                        .build(),
                )
                .await?
                .result;
            if title.is_some() {
                self.topic_state
                    .mark_manual_named(chat_id, topic.message_thread_id)
                    .await;
            }
            Ok(NewThreadResult {
                session_id: scope_key(chat_id, Some(topic.message_thread_id)),
                created_topic_id: Some(topic.message_thread_id),
                created_topic_name: Some(topic.name),
            })
        } else {
            Ok(NewThreadResult {
                session_id: self
                    .chat_sessions
                    .create_new(&scope_key(chat_id, message_thread_id))
                    .await?,
                created_topic_id: None,
                created_topic_name: None,
            })
        }
    }

    async fn peek_thread(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        is_forum: bool,
    ) -> String {
        if is_forum {
            scope_key(chat_id, message_thread_id)
        } else {
            self.chat_sessions
                .peek_or_default(&scope_key(chat_id, message_thread_id))
                .await
        }
    }

    async fn send_message_to_location(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        text: &str,
    ) -> Result<()> {
        let params = if let Some(thread_id) = message_thread_id {
            SendMessageParams::builder()
                .chat_id(chat_id)
                .message_thread_id(thread_id)
                .text(text.to_string())
                .build()
        } else {
            SendMessageParams::builder()
                .chat_id(chat_id)
                .text(text.to_string())
                .build()
        };
        self.bot.send_message(&params).await?;
        Ok(())
    }

    async fn auto_rename_forum_topic(
        &self,
        chat_id: i64,
        message_thread_id: i32,
        session_id: &str,
    ) {
        if !self
            .topic_state
            .should_auto_rename(chat_id, message_thread_id)
            .await
        {
            return;
        }

        let title = match self.llm.suggest_thread_title(session_id).await {
            Ok(Some(title)) => title,
            Ok(None) => return,
            Err(error) => {
                eprintln!("telegram auto topic rename title lookup error: {error}");
                return;
            }
        };

        let result = self
            .bot
            .edit_forum_topic(
                &EditForumTopicParams::builder()
                    .chat_id(chat_id)
                    .message_thread_id(message_thread_id)
                    .name(title)
                    .build(),
            )
            .await;

        match result {
            Ok(_) => {
                self.topic_state
                    .mark_auto_renamed(chat_id, message_thread_id)
                    .await;
            }
            Err(error) => {
                eprintln!("telegram auto topic rename failed: {error}");
            }
        }
    }

    async fn run_post_action(&self, post_action: PostAction) -> Result<()> {
        match post_action {
            PostAction::DeleteTopic {
                chat_id,
                message_thread_id,
            } => {
                self.bot
                    .delete_forum_topic(
                        &DeleteForumTopicParams::builder()
                            .chat_id(chat_id)
                            .message_thread_id(message_thread_id)
                            .build(),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn session_management_keyboard(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        is_forum: bool,
    ) -> InlineKeyboardMarkup {
        let action_row = vec![
            InlineKeyboardButton::builder()
                .text("📝 New Thread".to_string())
                .callback_data(CB_THREADS_NEW.to_string())
                .build(),
            InlineKeyboardButton::builder()
                .text("🔄 Reset Context".to_string())
                .callback_data(CB_THREADS_RESET.to_string())
                .build(),
        ];
        if is_forum {
            let mut rows = vec![action_row];
            if message_thread_id.is_some() {
                rows.push(vec![
                    InlineKeyboardButton::builder()
                        .text("✏️ Rename Topic".to_string())
                        .callback_data(CB_THREADS_RENAME.to_string())
                        .build(),
                    InlineKeyboardButton::builder()
                        .text("🗑️ Delete Topic".to_string())
                        .callback_data(CB_THREADS_DELETE.to_string())
                        .build(),
                ]);
            }
            return InlineKeyboardMarkup::builder().inline_keyboard(rows).build();
        }
        let scope = scope_key(chat_id, message_thread_id);
        let Ok(view) = self.chat_sessions.list(&scope).await else {
            return InlineKeyboardMarkup::builder()
                .inline_keyboard(vec![action_row])
                .build();
        };
        let mut rows: Vec<Vec<InlineKeyboardButton>> = view
            .sessions
            .iter()
            .map(|session| {
                let short = short_session_id(&session.id).to_string();
                let is_active = view.active_session_id.as_deref() == Some(session.id.as_str());
                let label = if is_active {
                    format!("✅ {short}")
                } else {
                    format!("💬 {short}")
                };
                vec![InlineKeyboardButton::builder()
                    .text(label)
                    .callback_data(format!("{CB_USE_THREAD_PREFIX}{short}"))
                    .build()]
            })
            .collect();
        rows.push(action_row);
        InlineKeyboardMarkup::builder().inline_keyboard(rows).build()
    }

    async fn run_shortcut_prompt(
        self: Arc<Self>,
        chat_id: i64,
        message_thread_id: Option<i32>,
        session_id: String,
        prompt: &'static str,
        approval_rx: mpsc::UnboundedReceiver<LlmApprovalDecision>,
        run_id: String,
    ) -> Result<()> {
        let scope = scope_key(chat_id, message_thread_id);
        let extra_system = self.active_wallet_hint().await;
        let streamed = Arc::new(Mutex::new(StreamedTelegramReply::default()));
        {
            let mut s = streamed.lock().await;
            s.last_body.clear();
            self.sync_streamed_reply(&mut s, chat_id, message_thread_id, false)
                .await?;
        }
        let streamed_updates = Arc::clone(&streamed);
        let transport = Arc::clone(&self);
        let approval_scope = scope.clone();
        let ask_result = self
            .llm
            .ask_streaming(
                &session_id,
                prompt,
                extra_system.as_deref(),
                approval_rx,
                |event| {
                    let transport = Arc::clone(&transport);
                    let streamed_updates = Arc::clone(&streamed_updates);
                    let scope = approval_scope.clone();
                    async move {
                        let mut streamed = streamed_updates.lock().await;
                        let should_sync = match event {
                            LlmStreamEvent::Content(content) => {
                                streamed.last_body = content;
                                should_attempt_stream_sync(&streamed)
                            }
                            LlmStreamEvent::ApprovalRequested(request) => {
                                let approval = TelegramApprovalPrompt::from(request.clone());
                                streamed.last_body = approval.summary.clone();
                                streamed.approval_request = Some(approval);
                                let _ = transport
                                    .active_runs
                                    .set_pending_approval(&scope, request)
                                    .await;
                                true
                            }
                            LlmStreamEvent::ApprovalResolved { request_id } => {
                                if streamed
                                    .approval_request
                                    .as_ref()
                                    .map(|a| a.request_id == request_id)
                                    .unwrap_or(false)
                                {
                                    streamed.approval_request = None;
                                }
                                transport
                                    .active_runs
                                    .clear_pending_approval(&scope, &request_id)
                                    .await;
                                true
                            }
                        };
                        if !should_sync {
                            return;
                        }
                        let _ = transport
                            .send_typing_action(&scope, chat_id, message_thread_id)
                            .await;
                        let _ = transport
                            .sync_streamed_reply(&mut streamed, chat_id, message_thread_id, false)
                            .await;
                    }
                },
            )
            .await;
        self.active_runs.remove(&scope, &run_id).await;
        let mut streamed = streamed.lock().await;
        match ask_result {
            Ok(reply) => {
                streamed.last_body = reply.text;
                streamed.approval_request = None;
            }
            Err(e) => {
                streamed.last_body = format!("error: {e}");
                streamed.approval_request = None;
            }
        }
        self.sync_streamed_reply(&mut streamed, chat_id, message_thread_id, true)
            .await?;
        Ok(())
    }

    async fn active_wallet_hint(&self) -> Option<String> {
        let wallet = match self.active_wallet.get().await {
            Some(w) => w,
            None => {
                let wallets = list_keystore_wallets().ok()?;
                let (name, address) = wallets.into_iter().next()?;
                let w = ActiveWallet { account_name: name, address };
                self.active_wallet.set(w.clone()).await;
                w
            }
        };
        let hint = if wallet.address.is_empty() {
            format!("ACTIVE WALLET: account={}", wallet.account_name)
        } else {
            format!(
                "ACTIVE WALLET: account={}, address={}",
                wallet.account_name, wallet.address
            )
        };
        Some(hint)
    }

    async fn handle_nav_callback(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        scope: &str,
        data: &str,
        is_forum: bool,
    ) -> Result<()> {
        match data {
            CB_WALLET_MENU => {
                self.send_reply_chunks(
                    chat_id,
                    message_thread_id,
                    &wallet_menu_text(),
                    Some(wallet_menu_keyboard()),
                )
                .await?;
            }
            CB_ONCHAIN_TOOLS => {
                self.send_reply_chunks(
                    chat_id,
                    message_thread_id,
                    "⛓ On-chain Tools\n\nDescribe the on-chain operation you want to perform in natural language, for example:\n• Query the ETH balance of an address\n• Send a transaction\n• Query ERC-20 token information\n• Interact with a contract\n\nStart the conversation to begin.",
                    None,
                )
                .await?;
            }
            CB_PREDICT_MARKET => {
                self.send_reply_chunks(
                    chat_id,
                    message_thread_id,
                    "📈 Prediction Markets\n\nSelect a direction to query:",
                    Some(predict_market_keyboard()),
                )
                .await?;
            }
            CB_SESSION_MGMT => {
                let body = self
                    .render_chat_sessions(chat_id, message_thread_id, is_forum)
                    .await
                    .unwrap_or_else(|_| "💬 Session Management".to_string());
                let keyboard = self
                    .session_management_keyboard(chat_id, message_thread_id, is_forum)
                    .await;
                self.send_reply_chunks(chat_id, message_thread_id, &body, Some(keyboard))
                    .await?;
            }
            CB_THREADS_NEW => {
                match self
                    .create_new_thread(chat_id, message_thread_id, is_forum, None)
                    .await
                {
                    Ok(result) => {
                        let msg = if is_forum {
                            "✅ New topic created, please open it in the Telegram topic list".to_string()
                        } else {
                            format!(
                                "✅ New thread created: {}",
                                short_session_id(&result.session_id)
                            )
                        };
                        let keyboard = self
                            .session_management_keyboard(chat_id, message_thread_id, is_forum)
                            .await;
                        self.send_reply_chunks(chat_id, message_thread_id, &msg, Some(keyboard))
                            .await?;
                    }
                    Err(e) => {
                        self.send_message_to_location(
                            chat_id,
                            message_thread_id,
                            &format!("❌ Creation failed: {e}"),
                        )
                        .await?;
                    }
                }
            }
            CB_THREADS_RESET => {
                let session_id = self
                    .resolve_active_thread(chat_id, message_thread_id, is_forum)
                    .await?;
                let msg = if is_forum {
                    let _ = self.llm.reset_session(&session_id).await;
                    "✅ Current topic context cleared".to_string()
                } else {
                    match self.chat_sessions.reset_current(scope).await {
                        Ok(new_id) => {
                            format!("✅ Reset successful, new thread: {}", short_session_id(&new_id))
                        }
                        Err(e) => format!("❌ Reset failed: {e}"),
                    }
                };
                let keyboard = self
                    .session_management_keyboard(chat_id, message_thread_id, is_forum)
                    .await;
                self.send_reply_chunks(chat_id, message_thread_id, &msg, Some(keyboard))
                    .await?;
            }
            CB_THREADS_RENAME => {
                let Some(topic_id) = message_thread_id else {
                    self.send_message_to_location(chat_id, message_thread_id, "❌ Only available inside a Forum topic")
                        .await?;
                    return Ok(());
                };
                self.wallet_wizard
                    .set(scope, WalletWizardStep::ThreadRenameAwaitingTitle { topic_thread_id: topic_id })
                    .await;
                self.send_wallet_prompt(chat_id, message_thread_id, "Please reply to this message with the new topic name:")
                    .await?;
            }
            CB_THREADS_DELETE => {
                let Some(topic_id) = message_thread_id else {
                    self.send_message_to_location(chat_id, message_thread_id, "❌ Only available inside a Forum topic")
                        .await?;
                    return Ok(());
                };
                match self
                    .bot
                    .delete_forum_topic(
                        &DeleteForumTopicParams::builder()
                            .chat_id(chat_id)
                            .message_thread_id(topic_id)
                            .build(),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        self.send_message_to_location(
                            chat_id,
                            message_thread_id,
                            &format!("❌ Failed to delete topic: {e}"),
                        )
                        .await?;
                    }
                }
            }
            CB_WALLET_NEW => {
                self.wallet_wizard
                    .set(scope, WalletWizardStep::NewWalletAwaitingPassword)
                    .await;
                self.send_wallet_prompt(
                    chat_id,
                    message_thread_id,
                    "Please reply to this message to set the keystore password (will not enter LLM context):",
                )
                .await?;
            }
            CB_WALLET_IMPORT => {
                self.wallet_wizard
                    .set(scope, WalletWizardStep::ImportWalletAwaitingName)
                    .await;
                self.send_wallet_prompt(chat_id, message_thread_id, "Please enter the account name (will not enter LLM context):")
                    .await?;
            }
            CB_WALLET_SELECT_LIST => match list_keystore_wallets() {
                Ok(wallets) if wallets.is_empty() => {
                    self.send_message_to_location(
                        chat_id,
                        message_thread_id,
                        "No wallets found, please create or import a wallet first.",
                    )
                    .await?;
                }
                Ok(wallets) => {
                    let active = self.active_wallet.get().await;
                    let active_name = active.as_ref().map(|w| w.account_name.as_str());
                    self.send_reply_chunks(
                        chat_id,
                        message_thread_id,
                        "Please select the wallet to activate:",
                        Some(wallet_list_keyboard(&wallets, CB_WALLET_SELECT_PREFIX, active_name)),
                    )
                    .await?;
                }
                Err(e) => {
                    self.send_message_to_location(
                        chat_id,
                        message_thread_id,
                        &format!("❌ Failed to retrieve wallet list: {e}"),
                    )
                    .await?;
                }
            },
            CB_WALLET_DELETE_LIST => match list_keystore_wallets() {
                Ok(wallets) if wallets.is_empty() => {
                    self.send_message_to_location(chat_id, message_thread_id, "No wallets found.")
                        .await?;
                }
                Ok(wallets) => {
                    let active = self.active_wallet.get().await;
                    let active_name = active.as_ref().map(|w| w.account_name.as_str());
                    self.send_reply_chunks(
                        chat_id,
                        message_thread_id,
                        "Please select the wallet to delete:",
                        Some(wallet_list_keyboard(&wallets, CB_WALLET_DELETE_PREFIX, active_name)),
                    )
                    .await?;
                }
                Err(e) => {
                    self.send_message_to_location(
                        chat_id,
                        message_thread_id,
                        &format!("❌ Failed to retrieve wallet list: {e}"),
                    )
                    .await?;
                }
            },
            _ if data.starts_with(CB_USE_THREAD_PREFIX) => {
                let short_id = &data[CB_USE_THREAD_PREFIX.len()..];
                if is_forum {
                    self.send_message_to_location(
                        chat_id,
                        message_thread_id,
                        "Please switch Forum topics through the Telegram interface",
                    )
                    .await?;
                } else {
                    match self.chat_sessions.switch_to(scope, short_id).await {
                        Ok(_) => {
                            let keyboard = self
                                .session_management_keyboard(chat_id, message_thread_id, is_forum)
                                .await;
                            let body = self
                                .render_chat_sessions(chat_id, message_thread_id, is_forum)
                                .await
                                .unwrap_or_else(|_| format!("✅ Switched to thread: {short_id}"));
                            self.send_reply_chunks(
                                chat_id,
                                message_thread_id,
                                &body,
                                Some(keyboard),
                            )
                            .await?;
                        }
                        Err(e) => {
                            self.send_message_to_location(
                                chat_id,
                                message_thread_id,
                                &format!("❌ Switch failed: {e}"),
                            )
                            .await?;
                        }
                    }
                }
            }
            _ if data.starts_with(CB_WALLET_SELECT_PREFIX) => {
                let name = &data[CB_WALLET_SELECT_PREFIX.len()..];
                self.wallet_wizard
                    .set(scope, WalletWizardStep::SelectWalletAwaitingPassword {
                        account_name: name.to_string(),
                    })
                    .await;
                self.send_wallet_prompt(
                    chat_id,
                    message_thread_id,
                    &format!("Please enter the keystore password for wallet [{name}] (will not enter LLM context):"),
                )
                .await?;
            }
            _ if data.starts_with(CB_WALLET_DELETE_PREFIX) => {
                let name = &data[CB_WALLET_DELETE_PREFIX.len()..];
                let keystore_path = foundry_keystore_dir()?.join(name);
                match std::fs::remove_file(&keystore_path) {
                    Ok(()) => {
                        if self
                            .active_wallet
                            .get()
                            .await
                            .map(|w| w.account_name == name)
                            .unwrap_or(false)
                        {
                            self.active_wallet.clear().await;
                        }
                        self.send_message_to_location(
                            chat_id,
                            message_thread_id,
                            &format!("✅ Wallet deleted: {name}"),
                        )
                        .await?;
                    }
                    Err(e) => {
                        self.send_message_to_location(
                            chat_id,
                            message_thread_id,
                            &format!("❌ Deletion failed: {e}"),
                        )
                        .await?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_wallet(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        message_id: i32,
        text: &str,
    ) -> Result<()> {
        let scope = scope_key(chat_id, message_thread_id);

        let Some(step) = self.wallet_wizard.get(&scope).await else {
            return Ok(());
        };

        match step {
            WalletWizardStep::NewWalletAwaitingPassword => {
                let password = text.to_string();
                self.wallet_wizard.clear(&scope).await;
                let deleted = self.delete_message(chat_id, message_id).await;
                let notice = if deleted { "Keystore password received: ⚫⚫⚫⚫⚫⚫, original message deleted" } else { "Keystore password received: ⚫⚫⚫⚫⚫⚫" };
                self.send_message_to_location(chat_id, message_thread_id, notice)
                    .await?;
                let result = self.run_cast_new_wallet(&password).await;
                let reply = match result {
                    Ok(output) => format!("✅ Wallet created successfully\n\n{output}"),
                    Err(err) => format!("❌ Creation failed: {err}"),
                };
                self.send_message_to_location(chat_id, message_thread_id, &reply)
                    .await?;
            }
            WalletWizardStep::ImportWalletAwaitingName => {
                let account_name = text.trim().to_string();
                self.wallet_wizard
                    .set(
                        &scope,
                        WalletWizardStep::ImportWalletAwaitingKey { account_name },
                    )
                    .await;
                self.send_wallet_prompt(
                    chat_id,
                    message_thread_id,
                    "Please reply to this message with your private key (will not enter LLM context):",
                )
                .await?;
            }
            WalletWizardStep::ImportWalletAwaitingKey { account_name } => {
                let private_key = text.to_string();
                let deleted = self.delete_message(chat_id, message_id).await;
                let notice = if deleted { "Private key received: ⚫⚫⚫⚫⚫⚫, original message deleted" } else { "Private key received: ⚫⚫⚫⚫⚫⚫" };
                self.send_message_to_location(chat_id, message_thread_id, notice)
                    .await?;
                self.wallet_wizard
                    .set(
                        &scope,
                        WalletWizardStep::ImportWalletAwaitingPassword { account_name, private_key },
                    )
                    .await;
                self.send_wallet_prompt(
                    chat_id,
                    message_thread_id,
                    "Please reply to this message to set the keystore password (will not enter LLM context):",
                )
                .await?;
            }
            WalletWizardStep::ImportWalletAwaitingPassword { account_name, private_key } => {
                let password = text.to_string();
                self.wallet_wizard.clear(&scope).await;
                let deleted = self.delete_message(chat_id, message_id).await;
                let notice = if deleted { "Keystore password received: ⚫⚫⚫⚫⚫⚫, original message deleted" } else { "Keystore password received: ⚫⚫⚫⚫⚫⚫" };
                self.send_message_to_location(chat_id, message_thread_id, notice)
                    .await?;
                let result = self
                    .run_cast_import_wallet(&account_name, &private_key, &password)
                    .await;
                let reply = match result {
                    Ok(output) => format!("✅ Wallet imported successfully\n\n{output}"),
                    Err(err) => format!("❌ Import failed: {err}"),
                };
                self.send_message_to_location(chat_id, message_thread_id, &reply)
                    .await?;
            }
            WalletWizardStep::SelectWalletAwaitingPassword { account_name } => {
                let password = text.to_string();
                self.wallet_wizard.clear(&scope).await;
                let deleted = self.delete_message(chat_id, message_id).await;
                let notice = if deleted { "Keystore password received: ⚫⚫⚫⚫⚫⚫, original message deleted" } else { "Keystore password received: ⚫⚫⚫⚫⚫⚫" };
                self.send_message_to_location(chat_id, message_thread_id, notice)
                    .await?;
                match self.run_cast_wallet_address(&account_name, &password).await {
                    Ok(address) => {
                        self.active_wallet
                            .set(ActiveWallet {
                                account_name: account_name.clone(),
                                address: address.clone(),
                            })
                            .await;
                        self.send_message_to_location(
                            chat_id,
                            message_thread_id,
                            &format!("✅ Wallet selected: {account_name}\nAddress: {address}\n\nSubsequent conversations will automatically inject wallet context."),
                        )
                        .await?;
                    }
                    Err(err) => {
                        self.send_message_to_location(
                            chat_id,
                            message_thread_id,
                            &format!("❌ Verification failed: {err}"),
                        )
                        .await?;
                    }
                }
            }
            WalletWizardStep::ThreadRenameAwaitingTitle { topic_thread_id } => {
                let title = text.trim().to_string();
                self.wallet_wizard.clear(&scope).await;
                match self
                    .bot
                    .edit_forum_topic(
                        &EditForumTopicParams::builder()
                            .chat_id(chat_id)
                            .message_thread_id(topic_thread_id)
                            .name(title.clone())
                            .build(),
                    )
                    .await
                {
                    Ok(_) => {
                        self.topic_state.mark_manual_named(chat_id, topic_thread_id).await;
                        self.send_message_to_location(
                            chat_id,
                            message_thread_id,
                            &format!("✅ Topic renamed to: {title}"),
                        )
                        .await?;
                    }
                    Err(e) => {
                        self.send_message_to_location(
                            chat_id,
                            message_thread_id,
                            &format!("❌ Rename failed: {e}"),
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_wallet_prompt(
        &self,
        chat_id: i64,
        message_thread_id: Option<i32>,
        prompt: &str,
    ) -> Result<()> {
        let force_reply = ReplyMarkup::ForceReply(ForceReply {
            force_reply: true,
            input_field_placeholder: None,
            selective: Some(true),
        });
        let params = if let Some(thread_id) = message_thread_id {
            SendMessageParams::builder()
                .chat_id(chat_id)
                .message_thread_id(thread_id)
                .text(prompt)
                .reply_markup(force_reply)
                .build()
        } else {
            SendMessageParams::builder()
                .chat_id(chat_id)
                .text(prompt)
                .reply_markup(force_reply)
                .build()
        };
        self.bot.send_message(&params).await?;
        Ok(())
    }

    async fn delete_message(&self, chat_id: i64, message_id: i32) -> bool {
        let params = DeleteMessageParams::builder()
            .chat_id(chat_id)
            .message_id(message_id)
            .build();
        self.bot.delete_message(&params).await.is_ok()
    }

    async fn run_cast_new_wallet(&self, password: &str) -> Result<String> {
        let keystore_dir = foundry_keystore_dir()?;
        let mut command = Command::new("script");
        command
            .arg("-qec")
            .arg(format!(
                "cast wallet new {}",
                shell_escape(&keystore_dir.display().to_string())
            ))
            .arg("/dev/null");
        let result = self
            .run_wallet_command(command, Some(format!("{password}\n")), &[password])
            .await?;
        if result.exit_code != 0 {
            anyhow::bail!("{}", result.stderr.trim());
        }
        let address = extract_eth_address(&result.stdout)
            .unwrap_or_else(|| result.stdout.trim().to_string());
        Ok(format!("Wallet created: {address}"))
    }

    async fn run_cast_import_wallet(
        &self,
        account_name: &str,
        private_key: &str,
        password: &str,
    ) -> Result<String> {
        let safe_name = account_name
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect::<String>();
        anyhow::ensure!(!safe_name.is_empty(), "Invalid account name");
        let keystore_dir = foundry_keystore_dir()?;
        let mut command = Command::new("script");
        command
            .arg("-qec")
            .arg(format!(
                "cast wallet import -i {} -k {}",
                shell_escape(&safe_name),
                shell_escape(&keystore_dir.display().to_string())
            ))
            .arg("/dev/null");
        let result = self
            .run_wallet_command(
                command,
                Some(format!("{}\n{password}\n", private_key.trim())),
                &[private_key.trim(), password],
            )
            .await?;
        if result.exit_code != 0 {
            anyhow::bail!("{}", result.stderr.trim());
        }
        let address = extract_eth_address(&result.stdout)
            .unwrap_or_else(|| result.stdout.trim().to_string());
        Ok(format!("Wallet imported: {safe_name} ({address})"))
    }

    async fn run_cast_wallet_address(&self, account_name: &str, password: &str) -> Result<String> {
        let tmp_path = std::env::temp_dir()
            .join(format!("edgeai_wpass_{}", Uuid::new_v4().simple()));
        tokio::fs::write(&tmp_path, password).await?;
        let mut command = Command::new("cast");
        command
            .arg("wallet")
            .arg("address")
            .arg("--account")
            .arg(account_name)
            .arg("--password-file")
            .arg(&tmp_path);
        let result = self.run_wallet_command(command, None, &[password]).await;
        let _ = tokio::fs::remove_file(&tmp_path).await;
        let result = result?;
        if result.exit_code != 0 {
            anyhow::bail!("{}", result.stderr.trim());
        }
        extract_eth_address(&result.stdout)
            .ok_or_else(|| anyhow::anyhow!("Failed to extract address from output: {}", result.stdout.trim()))
    }

    async fn run_wallet_command(
        &self,
        mut command: Command,
        stdin: Option<String>,
        secrets: &[&str],
    ) -> Result<WalletCommandResult> {
        command
            .current_dir(&self.runtime.cwd)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().context("failed to spawn wallet command")?;

        if let Some(stdin_payload) = stdin {
            let mut child_stdin = child
                .stdin
                .take()
                .context("wallet command stdin unavailable")?;
            child_stdin.write_all(stdin_payload.as_bytes()).await?;
            drop(child_stdin);
        }

        let output = match timeout(
            Duration::from_secs(self.runtime.timeout_secs),
            child.wait_with_output(),
        )
        .await
        {
            Ok(output) => output?,
            Err(_) => {
                anyhow::bail!(
                    "wallet command timed out after {} seconds",
                    self.runtime.timeout_secs
                );
            }
        };

        Ok(WalletCommandResult {
            exit_code: output.status.code().unwrap_or(1),
            stdout: sanitize_wallet_output(&String::from_utf8_lossy(&output.stdout), secrets),
            stderr: sanitize_wallet_output(&String::from_utf8_lossy(&output.stderr), secrets),
        })
    }

    async fn register_commands(&self) -> Result<()> {
        let params = SetMyCommandsParams::builder()
            .commands(vec![
                BotCommand::builder()
                    .command("news_watch".to_string())
                    .description("News tracker: send a preset question analyzing today's major news affecting prediction markets".to_string())
                    .build(),
                BotCommand::builder()
                    .command("smart_money".to_string())
                    .description("Smart money: send a preset question recommending the top 5 smart money traders".to_string())
                    .build(),
                BotCommand::builder()
                    .command("threads".to_string())
                    .description("Manage threads: view, create, rename, delete, switch".to_string())
                    .build(),
                BotCommand::builder()
                    .command("stop".to_string())
                    .description("Stop the currently in-progress LLM reply for this thread".to_string())
                    .build(),
                BotCommand::builder()
                    .command("reset".to_string())
                    .description("Reset the AI context for the current thread without creating a new thread".to_string())
                    .build(),
                BotCommand::builder()
                    .command("wallet".to_string())
                    .description("Wallet management: create, import, select, delete wallets (keys do not pass through LLM)".to_string())
                    .build(),
                BotCommand::builder()
                    .command("market".to_string())
                    .description("Prediction markets: smart money tracking & news tracking".to_string())
                    .build(),
                BotCommand::builder()
                    .command("help".to_string())
                    .description("View all available commands and thread usage instructions".to_string())
                    .build(),
            ])
            .build();
        self.bot.set_my_commands(&params).await?;
        Ok(())
    }

    const BOT_DESCRIPTION: &'static str = "Your local trading expert";

    async fn ensure_bot_description(&self) -> Result<()> {
        let BotDescription { description } = self
            .bot
            .get_my_description(&GetMyDescriptionParams::builder().build())
            .await?
            .result;
        if description.is_empty() {
            self.bot
                .set_my_description(&SetMyDescriptionParams::builder()
                    .description(Self::BOT_DESCRIPTION.to_string())
                    .build())
                .await?;
            println!("bot description set to: {}", Self::BOT_DESCRIPTION);
        } else {
            println!("bot description already set: {}", description);
        }
        Ok(())
    }

    async fn run_cron_scheduler(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        let mut fired: std::collections::HashSet<(String, u64)> = std::collections::HashSet::new();
        loop {
            interval.tick().await;
            let now = Local::now();
            let minute_bucket = now.timestamp() as u64 / 60;

            let tasks_file = self.runtime.cwd.join(".claude/scheduled_tasks.json");
            let content = match tokio::fs::read_to_string(&tasks_file).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let json: serde_json::Value = match serde_json::from_str(&content) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(tasks) = json["tasks"].as_array() else { continue };

            let current_exe = match std::env::current_exe() {
                Ok(p) => p,
                Err(_) => continue,
            };

            for task in tasks {
                let Some(id) = task["id"].as_str() else { continue };
                let Some(cron_expr) = task["cron"].as_str() else { continue };
                let Some(prompt) = task["prompt"].as_str() else { continue };

                if !cron_matches(cron_expr, &now) {
                    continue;
                }
                let key = (id.to_string(), minute_bucket);
                if fired.contains(&key) {
                    continue;
                }
                fired.insert(key);

                let cmd = if let Some(rest) = prompt.strip_prefix("edgeai ") {
                    format!("{} {rest}", current_exe.display())
                } else {
                    prompt.to_string()
                };
                tracing::debug!(task_id = id, cron = cron_expr, "firing scheduled task");
                let _ = tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(&cmd)
                    .current_dir(&self.runtime.cwd)
                    .spawn();
            }

            fired.retain(|(_, bucket)| *bucket >= minute_bucket.saturating_sub(2));
        }
    }
}

impl ChatSessionStore {
    async fn load(path: PathBuf) -> Result<Self> {
        let data = match tokio::fs::read(&path).await {
            Ok(body) => serde_json::from_slice(&body)
                .with_context(|| format!("failed to parse {}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                PersistedChatSessions::default()
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            inner: Mutex::new(data),
        })
    }

    async fn current_or_create(&self, scope_key: &str) -> Result<String> {
        let mut inner = self.inner.lock().await;
        let chat = inner
            .chats
            .entry(scope_key.to_string())
            .or_insert_with(ChatSessions::default);
        if let Some(active) = chat.active_session_id.clone() {
            return Ok(active);
        }

        let session_id = build_transport_session_id(scope_key);
        chat.sessions.push(ChatThread {
            id: session_id.clone(),
            created_at_unix_ms: now_unix_ms(),
        });
        chat.active_session_id = Some(session_id.clone());
        self.save_locked(&inner).await?;
        Ok(session_id)
    }

    async fn create_new(&self, scope_key: &str) -> Result<String> {
        let mut inner = self.inner.lock().await;
        let chat = inner
            .chats
            .entry(scope_key.to_string())
            .or_insert_with(ChatSessions::default);
        let session_id = build_transport_session_id(scope_key);
        chat.sessions.push(ChatThread {
            id: session_id.clone(),
            created_at_unix_ms: now_unix_ms(),
        });
        chat.active_session_id = Some(session_id.clone());
        self.save_locked(&inner).await?;
        Ok(session_id)
    }

    async fn reset_current(&self, scope_key: &str) -> Result<String> {
        let mut inner = self.inner.lock().await;
        let chat = inner
            .chats
            .entry(scope_key.to_string())
            .or_insert_with(ChatSessions::default);

        if let Some(active) = chat.active_session_id.clone() {
            chat.sessions.retain(|session| session.id != active);
        }

        let session_id = build_transport_session_id(scope_key);
        chat.sessions.push(ChatThread {
            id: session_id.clone(),
            created_at_unix_ms: now_unix_ms(),
        });
        chat.active_session_id = Some(session_id.clone());
        self.save_locked(&inner).await?;
        Ok(session_id)
    }

    async fn switch_to(&self, scope_key: &str, short_id: &str) -> Result<String> {
        let mut inner = self.inner.lock().await;
        let chat = inner
            .chats
            .entry(scope_key.to_string())
            .or_insert_with(ChatSessions::default);
        let Some(found) = chat
            .sessions
            .iter()
            .find(|session| session.id.ends_with(short_id))
            .map(|session| session.id.clone())
        else {
            bail!("session `{short_id}` not found in this chat");
        };
        chat.active_session_id = Some(found.clone());
        self.save_locked(&inner).await?;
        Ok(found)
    }

    async fn list(&self, scope_key: &str) -> Result<ChatSessions> {
        let current = self.current_or_create(scope_key).await?;
        let inner = self.inner.lock().await;
        let mut chat = inner.chats.get(scope_key).cloned().unwrap_or_default();
        if chat.active_session_id.is_none() {
            chat.active_session_id = Some(current);
        }
        Ok(chat)
    }

    async fn peek_or_default(&self, scope_key: &str) -> String {
        let inner = self.inner.lock().await;
        inner
            .chats
            .get(scope_key)
            .and_then(|chat| chat.active_session_id.clone())
            .unwrap_or_else(|| format!("{scope_key}:pending"))
    }

    async fn save_locked(&self, data: &PersistedChatSessions) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&self.path, serde_json::to_vec_pretty(data)?).await?;
        Ok(())
    }
}

impl ScopeLockStore {
    async fn get(&self, scope_key: &str) -> Arc<Mutex<()>> {
        let mut inner = self.inner.lock().await;
        Arc::clone(
            inner
                .entry(scope_key.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }
}

impl Default for ScopeLockStore {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl TypingActionStore {
    async fn should_send(&self, scope_key: &str) -> bool {
        let now = now_unix_ms();
        let inner = self.inner.lock().await;
        inner
            .get(scope_key)
            .map(|state| {
                now >= state.blocked_until
                    && now.saturating_sub(state.last_sent_at) >= TELEGRAM_TYPING_ACTION_INTERVAL_MS
            })
            .unwrap_or(true)
    }

    async fn record_success(&self, scope_key: &str) {
        let now = now_unix_ms();
        let mut inner = self.inner.lock().await;
        let state = inner.entry(scope_key.to_string()).or_default();
        state.last_sent_at = now;
        state.blocked_until = now;
    }

    async fn record_failure(&self, scope_key: &str, retry_after_secs: Option<u64>) {
        let now = now_unix_ms();
        let mut inner = self.inner.lock().await;
        let state = inner.entry(scope_key.to_string()).or_default();
        state.last_sent_at = now;
        state.blocked_until = retry_after_secs
            .map(|secs| now.saturating_add((secs as u128) * 1000))
            .unwrap_or_else(|| now.saturating_add(TELEGRAM_TYPING_ACTION_INTERVAL_MS));
    }
}

impl Default for TypingActionStore {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl ActiveRunStore {
    async fn insert(
        &self,
        scope_key: String,
        run_id: String,
        handle: tokio::task::AbortHandle,
        approval_tx: mpsc::UnboundedSender<LlmApprovalDecision>,
    ) {
        self.inner
            .lock()
            .await
            .entry(scope_key)
            .or_default()
            .push_back(ActiveRun {
                id: run_id,
                abort_handle: handle,
                approval_tx,
                pending_approval_request_id: None,
                pending_approval_command: None,
            });
    }

    async fn remove(&self, scope_key: &str, run_id: &str) {
        let mut inner = self.inner.lock().await;
        let Some(queue) = inner.get_mut(scope_key) else {
            return;
        };
        if let Some(index) = queue.iter().position(|run| run.id == run_id) {
            queue.remove(index);
        }
        if queue.is_empty() {
            inner.remove(scope_key);
        }
    }

    async fn abort(&self, scope_key: &str) -> bool {
        let mut inner = self.inner.lock().await;
        let Some(queue) = inner.get_mut(scope_key) else {
            return false;
        };
        let run = queue.pop_front();
        if queue.is_empty() {
            inner.remove(scope_key);
        }
        if let Some(run) = run {
            run.abort_handle.abort();
            true
        } else {
            false
        }
    }

    async fn submit_approval(&self, scope_key: &str, approval: LlmApprovalDecision) -> (bool, Option<String>) {
        let inner = self.inner.lock().await;
        let front_run = inner.get(scope_key).and_then(|queue| queue.front());
        let front_pending = front_run.and_then(|run| run.pending_approval_request_id.as_deref());
        tracing::debug!(
            scope_key,
            request_id = %approval.request_id,
            front_run_id = front_run.map(|r| r.id.as_str()).unwrap_or("none"),
            front_pending = front_pending.unwrap_or("none"),
            total_scopes = inner.len(),
            "submit_approval called"
        );
        if let Some(run) = inner
            .get(scope_key)
            .and_then(|queue| queue.front())
            .filter(|run| run.pending_approval_request_id.as_deref() == Some(&approval.request_id))
        {
            let command = run.pending_approval_command.clone();
            let sent = run.approval_tx.send(approval.clone()).is_ok();
            return (sent, command);
        }

        if let Some(run) = inner
            .values()
            .flat_map(|queue| queue.iter())
            .find(|run| run.pending_approval_request_id.as_deref() == Some(&approval.request_id))
        {
            let command = run.pending_approval_command.clone();
            let sent = run.approval_tx.send(approval).is_ok();
            return (sent, command);
        }

        (false, None)
    }

    async fn set_pending_approval(&self, scope_key: &str, approval: LlmApprovalRequest) {
        let mut inner = self.inner.lock().await;
        let Some(queue) = inner.get_mut(scope_key) else {
            tracing::warn!(scope_key, request_id = %approval.request_id, "set_pending_approval: scope not found in active runs");
            return;
        };
        let Some(run) = queue.front_mut() else {
            tracing::warn!(scope_key, request_id = %approval.request_id, "set_pending_approval: queue is empty");
            return;
        };
        tracing::debug!(scope_key, run_id = %run.id, request_id = %approval.request_id, "set_pending_approval: setting pending");
        run.pending_approval_command = approval.command.clone();
        run.pending_approval_request_id = Some(approval.request_id);
    }

    async fn clear_pending_approval(&self, scope_key: &str, request_id: &str) {
        let mut inner = self.inner.lock().await;
        let Some(queue) = inner.get_mut(scope_key) else {
            return;
        };
        let Some(run) = queue.front_mut() else {
            return;
        };
        if run.pending_approval_request_id.as_deref() == Some(request_id) {
            run.pending_approval_request_id = None;
            run.pending_approval_command = None;
        }
    }
}

impl Default for ActiveRunStore {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Debug, Clone)]
enum WalletWizardStep {
    NewWalletAwaitingPassword,
    ImportWalletAwaitingName,
    ImportWalletAwaitingKey { account_name: String },
    ImportWalletAwaitingPassword { account_name: String, private_key: String },
    SelectWalletAwaitingPassword { account_name: String },
    ThreadRenameAwaitingTitle { topic_thread_id: i32 },
}

#[derive(Default)]
struct WalletWizardStore {
    inner: Mutex<HashMap<String, WalletWizardStep>>,
}

impl WalletWizardStore {
    async fn get(&self, scope_key: &str) -> Option<WalletWizardStep> {
        self.inner.lock().await.get(scope_key).cloned()
    }

    async fn set(&self, scope_key: &str, step: WalletWizardStep) {
        self.inner.lock().await.insert(scope_key.to_string(), step);
    }

    async fn clear(&self, scope_key: &str) {
        self.inner.lock().await.remove(scope_key);
    }
}

#[derive(Clone)]
struct ActiveWallet {
    account_name: String,
    address: String,
}

#[derive(Serialize, Deserialize)]
struct PersistedWallet {
    account_name: String,
    address: String,
}

struct ActiveWalletStore {
    inner: Mutex<Option<ActiveWallet>>,
    file: PathBuf,
}

impl ActiveWalletStore {
    async fn load(file: PathBuf) -> Self {
        let inner = tokio::fs::read(&file)
            .await
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PersistedWallet>(&bytes).ok())
            .map(|p| ActiveWallet { account_name: p.account_name, address: p.address });
        Self { inner: Mutex::new(inner), file }
    }

    async fn get(&self) -> Option<ActiveWallet> {
        self.inner.lock().await.clone()
    }

    async fn set(&self, wallet: ActiveWallet) {
        *self.inner.lock().await = Some(wallet.clone());
        if let Ok(json) = serde_json::to_vec(&PersistedWallet {
            account_name: wallet.account_name,
            address: wallet.address,
        }) {
            let _ = tokio::fs::write(&self.file, json).await;
        }
    }

    async fn clear(&self) {
        *self.inner.lock().await = None;
        let _ = tokio::fs::remove_file(&self.file).await;
    }
}

impl TopicStateStore {
    async fn mark_manual_named(&self, chat_id: i64, message_thread_id: i32) {
        let mut inner = self.inner.lock().await;
        let state = inner
            .entry(scope_key(chat_id, Some(message_thread_id)))
            .or_default();
        state.manual_named = true;
    }

    async fn mark_auto_renamed(&self, chat_id: i64, message_thread_id: i32) {
        let mut inner = self.inner.lock().await;
        let state = inner
            .entry(scope_key(chat_id, Some(message_thread_id)))
            .or_default();
        state.auto_renamed = true;
    }

    async fn should_auto_rename(&self, chat_id: i64, message_thread_id: i32) -> bool {
        let inner = self.inner.lock().await;
        inner
            .get(&scope_key(chat_id, Some(message_thread_id)))
            .map(|state| !state.manual_named && !state.auto_renamed)
            .unwrap_or(true)
    }
}

impl Default for TopicStateStore {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

fn shell_escape(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

fn foundry_keystore_dir() -> Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join(".foundry/keystores"))
}

struct WalletCommandResult {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

fn sanitize_wallet_output(output: &str, secrets: &[&str]) -> String {
    let mut sanitized = output.replace('\r', "");
    for secret in secrets {
        if !secret.is_empty() {
            sanitized = sanitized.replace(secret, "[REDACTED]");
        }
    }
    sanitized.trim().to_string()
}

fn extract_eth_address(text: &str) -> Option<String> {
    text.split_whitespace()
        .find(|token| {
            let t = token.trim_end_matches([',', '.', ')']);
            t.starts_with("0x") && t.len() == 42 && t[2..].chars().all(|c| c.is_ascii_hexdigit())
        })
        .map(|t| t.trim_end_matches([',', '.', ')']).to_string())
}

fn build_transport_session_id(scope_key: &str) -> String {
    format!("{scope_key}:{}", Uuid::new_v4().simple())
}

impl From<LlmApprovalRequest> for TelegramApprovalPrompt {
    fn from(value: LlmApprovalRequest) -> Self {
        let summary = render_approval_summary_text(&value);
        Self {
            request_id: value.request_id,
            summary,
            command: value.command,
            allow_accept_for_session: value.allow_accept_for_session,
            allow_cancel: value.allow_cancel,
        }
    }
}

impl TelegramApprovalPrompt {
    fn markup_key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.request_id,
            self.allow_accept_for_session,
            self.allow_cancel,
            self.command.is_some(),
        )
    }
}

fn approval_reply_markup(prompt: &TelegramApprovalPrompt) -> InlineKeyboardMarkup {
    let mut rows = vec![vec![
        InlineKeyboardButton::builder()
            .text("Approve".to_string())
            .callback_data(approval_callback_data(
                &prompt.request_id,
                LlmApprovalChoice::Accept,
            ))
            .build(),
        InlineKeyboardButton::builder()
            .text("Deny".to_string())
            .callback_data(approval_callback_data(
                &prompt.request_id,
                LlmApprovalChoice::Decline,
            ))
            .build(),
    ]];
    if prompt.command.is_some() {
        rows.push(vec![
            InlineKeyboardButton::builder()
                .text("Always Allow".to_string())
                .callback_data(approval_callback_data(
                    &prompt.request_id,
                    LlmApprovalChoice::AlwaysAllow,
                ))
                .build(),
        ]);
    }
    if prompt.allow_accept_for_session || prompt.allow_cancel {
        let mut row = Vec::new();
        if prompt.allow_accept_for_session {
            row.push(
                InlineKeyboardButton::builder()
                    .text("Approve Session".to_string())
                    .callback_data(approval_callback_data(
                        &prompt.request_id,
                        LlmApprovalChoice::AcceptForSession,
                    ))
                    .build(),
            );
        }
        if prompt.allow_cancel {
            row.push(
                InlineKeyboardButton::builder()
                    .text("Stop".to_string())
                    .callback_data(approval_callback_data(
                        &prompt.request_id,
                        LlmApprovalChoice::Cancel,
                    ))
                    .build(),
            );
        }
        rows.push(row);
    }
    InlineKeyboardMarkup::builder()
        .inline_keyboard(rows)
        .build()
}

fn render_approval_summary_text(request: &LlmApprovalRequest) -> String {
    let command = request
        .command
        .as_deref()
        .map(|command| truncate_approval_text(command, 160));
    let reason = request
        .reason
        .as_deref()
        .map(|reason| truncate_approval_text(reason, 160));

    match (command, reason) {
        (Some(command), Some(reason)) => {
            format!("Claude requests to run command: `{command}`\n\nReason: {reason}")
        }
        (Some(command), None) => format!("Claude requests to run command: `{command}`"),
        (None, Some(reason)) => format!("Claude requests authorization: {reason}"),
        (None, None) => "Claude is requesting your authorization.".to_string(),
    }
}

fn truncate_approval_text(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn approval_callback_data(request_id: &str, choice: LlmApprovalChoice) -> String {
    let code = match choice {
        LlmApprovalChoice::Accept => "a",
        LlmApprovalChoice::AcceptForSession => "s",
        LlmApprovalChoice::AlwaysAllow => "p",
        LlmApprovalChoice::Decline => "d",
        LlmApprovalChoice::Cancel => "c",
    };
    format!("codex-approval:{code}:{request_id}")
}

fn parse_approval_callback_data(data: &str) -> Option<(LlmApprovalChoice, String)> {
    let mut parts = data.splitn(3, ':');
    if parts.next()? != "codex-approval" {
        return None;
    }
    let choice = match parts.next()? {
        "a" => LlmApprovalChoice::Accept,
        "s" => LlmApprovalChoice::AcceptForSession,
        "p" => LlmApprovalChoice::AlwaysAllow,
        "d" => LlmApprovalChoice::Decline,
        "c" => LlmApprovalChoice::Cancel,
        _ => return None,
    };
    Some((choice, parts.next()?.to_string()))
}

fn callback_message_location(message: &MaybeInaccessibleMessage) -> (i64, Option<i32>) {
    match message {
        MaybeInaccessibleMessage::Message(message) => (message.chat.id, message.message_thread_id),
        MaybeInaccessibleMessage::InaccessibleMessage(message) => (message.chat.id, None),
    }
}

fn callback_message_is_forum_topic(message: &MaybeInaccessibleMessage) -> bool {
    match message {
        MaybeInaccessibleMessage::Message(message) => is_forum_message(message),
        MaybeInaccessibleMessage::InaccessibleMessage(message) => message.chat.is_forum.unwrap_or(false),
    }
}

fn scope_key(chat_id: i64, message_thread_id: Option<i32>) -> String {
    match message_thread_id {
        Some(thread_id) => format!("chat:{chat_id}:thread:{thread_id}"),
        None => format!("chat:{chat_id}"),
    }
}

fn forum_topic_name() -> String {
    "New Chat".to_string()
}

fn short_session_id(session_id: &str) -> &str {
    session_id.rsplit(':').next().unwrap_or(session_id)
}

fn scheduled_task_system_hint(chat_id: i64, thread_id: Option<i32>) -> String {
    let run_arg = match thread_id {
        Some(tid) => format!("--chat-id {chat_id} --thread-id {tid}"),
        None => format!("--chat-id {chat_id}"),
    };
    format!(
        "SCHEDULING CONTEXT: You are running as a short-lived subprocess inside edgeai — \
this process exits after each response, so ScheduleWakeup and /loop die with it and will never fire. \
crontab is not available on this system. \
CronCreate with durable=true is the correct tool for recurring tasks; it persists across process restarts. \
However, when a durable CronCreate fires, its `prompt` is delivered to a fresh Claude session with no Telegram context — \
if the prompt is a plain description or shell command, the output has nowhere to go and the user never sees it. \
The only way to get output back to this Telegram chat is to set the prompt to: \
`edgeai run {run_arg} --prompt \"<what to do>\"` — edgeai run invokes the LLM and delivers the result to this chat automatically. \
Example for a task that reports the time every hour: \
CronCreate(cron=\"0 * * * *\", prompt=\"edgeai run {run_arg} --prompt \\\"report the current time\\\"\", durable=true, recurring=true)"
    )
}

fn telegram_command_parts(input: &str) -> Option<(&str, &str)> {
    let trimmed = input.trim();
    if !trimmed.starts_with('/') {
        return None;
    }

    let split_at = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
    let head = &trimmed[..split_at];
    let rest = trimmed[split_at..].trim_start();
    Some((head, rest))
}

fn matches_telegram_command(input: &str, command: &str) -> bool {
    let Some((head, rest)) = telegram_command_parts(input) else {
        return false;
    };
    if !rest.is_empty() {
        return false;
    }

    head == command
        || head
            .strip_prefix(command)
            .is_some_and(|suffix| suffix.starts_with('@') && suffix.len() > 1)
}

fn thread_command_rest(input: &str) -> Option<&str> {
    let (head, rest) = telegram_command_parts(input)?;
    if head == COMMAND_THREADS {
        return Some(rest);
    }

    head.strip_prefix(COMMAND_THREADS)
        .filter(|suffix| suffix.starts_with('@') && suffix.len() > 1)
        .map(|_| rest)
}

fn is_help_command(input: &str) -> bool {
    matches_telegram_command(input, "/help")
}

fn start_welcome_text() -> String {
    [
        "👋 Welcome to ShBot!",
        "",
        "I can help you with:",
        "📈 Prediction market strategy — analyze news, track smart money, and make better decisions in prediction markets",
        "🔗 Web3 trading strategy — provide trading ideas and risk tips combined with on-chain data",
        "🔐 Wallet management — runs entirely locally, supports wallet creation and management, as well as on-chain operations",
        "",
        "Quick access 👇",
        "",
        "Send /help to view all commands",
    ]
    .join("\n")
}

fn start_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::builder()
        .inline_keyboard(vec![
            vec![
                InlineKeyboardButton::builder()
                    .text("🔐 Wallet Management".to_string())
                    .callback_data(CB_WALLET_MENU.to_string())
                    .build(),
                InlineKeyboardButton::builder()
                    .text("⛓ On-chain Tools".to_string())
                    .callback_data(CB_ONCHAIN_TOOLS.to_string())
                    .build(),
            ],
            vec![
                InlineKeyboardButton::builder()
                    .text("📈 Prediction Markets".to_string())
                    .callback_data(CB_PREDICT_MARKET.to_string())
                    .build(),
                InlineKeyboardButton::builder()
                    .text("💬 Session Management".to_string())
                    .callback_data(CB_SESSION_MGMT.to_string())
                    .build(),
            ],
        ])
        .build()
}

fn is_stop_text(input: &str) -> bool {
    matches_telegram_command(input, COMMAND_STOP)
}

fn is_stop_message(message: &Message) -> bool {
    message.text.as_deref().map(is_stop_text).unwrap_or(false)
}

fn is_forum_message(message: &Message) -> bool {
    message.chat.is_forum.unwrap_or(false)
        || message.is_topic_message.unwrap_or(false)
        || message.message_thread_id.is_some()
}

fn shortcut_prompt(input: &str) -> Option<&'static str> {
    if matches_telegram_command(input, SHORTCUT_NEWS_WATCH) {
        Some(PROMPT_NEWS_WATCH)
    } else if matches_telegram_command(input, SHORTCUT_SMART_MONEY) {
        Some(PROMPT_SMART_MONEY)
    } else {
        None
    }
}

fn wallet_menu_text() -> String {
    [
        "🔐 Wallet Management",
        "",
        "All private key and password operations are completed locally, do not pass through LLM context, and are not recorded in bash command history — fully local management.",
        "",
        "Please select an action:",
    ]
    .join("\n")
}

fn wallet_menu_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::builder()
        .inline_keyboard(vec![
            vec![
                InlineKeyboardButton::builder()
                    .text("➕ Create Wallet".to_string())
                    .callback_data(CB_WALLET_NEW.to_string())
                    .build(),
                InlineKeyboardButton::builder()
                    .text("📥 Import Wallet".to_string())
                    .callback_data(CB_WALLET_IMPORT.to_string())
                    .build(),
            ],
            vec![
                InlineKeyboardButton::builder()
                    .text("✅ Select Wallet".to_string())
                    .callback_data(CB_WALLET_SELECT_LIST.to_string())
                    .build(),
                InlineKeyboardButton::builder()
                    .text("🗑 Delete Wallet".to_string())
                    .callback_data(CB_WALLET_DELETE_LIST.to_string())
                    .build(),
            ],
        ])
        .build()
}

fn predict_market_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::builder()
        .inline_keyboard(vec![vec![
            InlineKeyboardButton::builder()
                .text("💰 Smart Money".to_string())
                .callback_data(SHORTCUT_SMART_MONEY.to_string())
                .build(),
            InlineKeyboardButton::builder()
                .text("📰 News Tracker".to_string())
                .callback_data(SHORTCUT_NEWS_WATCH.to_string())
                .build(),
        ]])
        .build()
}

fn wallet_list_keyboard(
    wallets: &[(String, String)],
    prefix: &str,
    active_name: Option<&str>,
) -> InlineKeyboardMarkup {
    let rows: Vec<Vec<InlineKeyboardButton>> = wallets
        .iter()
        .map(|(name, addr)| {
            let is_active = active_name.is_some_and(|a| a == name);
            let addr_part = if addr.is_empty() {
                String::new()
            } else {
                format!(" ({})", short_eth_address(addr))
            };
            let label = if is_active {
                format!("✅ {name}{addr_part}")
            } else {
                format!("{name}{addr_part}")
            };
            vec![InlineKeyboardButton::builder()
                .text(label)
                .callback_data(format!("{prefix}{name}"))
                .build()]
        })
        .collect();
    InlineKeyboardMarkup::builder().inline_keyboard(rows).build()
}

fn short_eth_address(addr: &str) -> String {
    let without_prefix = addr.strip_prefix("0x").unwrap_or(addr);
    if without_prefix.len() >= 8 {
        format!(
            "0x{}...{}",
            &without_prefix[..4],
            &without_prefix[without_prefix.len() - 4..]
        )
    } else {
        addr.to_string()
    }
}

fn is_nav_callback(data: &str) -> bool {
    matches!(
        data,
        CB_WALLET_MENU
            | CB_ONCHAIN_TOOLS
            | CB_PREDICT_MARKET
            | CB_SESSION_MGMT
            | CB_WALLET_NEW
            | CB_WALLET_IMPORT
            | CB_WALLET_SELECT_LIST
            | CB_WALLET_DELETE_LIST
            | CB_THREADS_NEW
            | CB_THREADS_RESET
            | CB_THREADS_RENAME
            | CB_THREADS_DELETE
    ) || data.starts_with(CB_WALLET_SELECT_PREFIX)
        || data.starts_with(CB_WALLET_DELETE_PREFIX)
        || data.starts_with(CB_USE_THREAD_PREFIX)
}

fn list_keystore_wallets() -> Result<Vec<(String, String)>> {
    let keystore_dir = foundry_keystore_dir()?;
    if !keystore_dir.exists() {
        return Ok(Vec::new());
    }
    let mut wallets = Vec::new();
    for entry in std::fs::read_dir(&keystore_dir).context("failed to read keystore dir")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let address = read_keystore_address(&path).unwrap_or_default();
            wallets.push((name, address));
        }
    }
    wallets.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(wallets)
}

fn read_keystore_address(path: &Path) -> Result<String> {
    let content = std::fs::read_to_string(path)?;
    let json: serde_json::Value = serde_json::from_str(&content)?;
    let address = json["address"]
        .as_str()
        .context("keystore has no address field")?;
    Ok(format!("0x{address}"))
}

fn help_text() -> String {
    [
        "edgeai commands:",
        "/news_watch - News tracker: send a preset question asking about today's major news affecting prediction markets",
        "/smart_money - Smart money: send a preset question asking for the top 5 smart money traders",
        "/help or /start - show this help",
        "/stop - stop the current in-progress LLM reply for this thread",
        "/reset - clear only the current thread context; does not create a new thread",
        "",
        "Forum topics:",
        "/threads - show the current topic status and quick actions",
        "/threads new [title] - create a new Telegram topic",
        "/threads rename <title> - rename the current Telegram topic",
        "/threads delete - delete the current Telegram topic",
        "",
        "Private chats or non-forum groups:",
        "/threads - list local threads in this chat and available subcommands",
        "/threads new [title] - create a new local thread",
        "/threads use <id> - switch the active local thread",
        "",
        "Any other text message is forwarded to the configured LLM backend.",
    ]
    .join("\n")
}

fn truncate_reply(input: &str, max_len: usize) -> String {
    let mut truncated = String::new();
    for ch in input.chars() {
        if truncated.len() + ch.len_utf8() > max_len {
            truncated.push_str("\n[output truncated]");
            return truncated;
        }
        truncated.push(ch);
    }
    truncated
}

fn is_truncated(input: &str, max_len: usize) -> bool {
    input.chars().map(char::len_utf8).sum::<usize>() > max_len
}

fn render_streamed_body(body: &str, force: bool) -> String {
    let trimmed = body.trim();
    if force {
        trimmed.to_string()
    } else if trimmed.is_empty() {
        TELEGRAM_STREAM_THINKING_SUFFIX.to_string()
    } else {
        format!("{trimmed}\n\n{TELEGRAM_STREAM_THINKING_SUFFIX}")
    }
}

fn should_render_stream_update(previous: &str, current: &str) -> bool {
    let previous = previous.trim();
    let current = current.trim();
    if previous.is_empty() {
        return !current.is_empty();
    }
    if current.is_empty() || current == previous {
        return false;
    }

    let previous_chars = previous.chars().count();
    let current_chars = current.chars().count();
    if current_chars <= previous_chars {
        return true;
    }

    let delta = current_chars.saturating_sub(previous_chars);
    if delta >= TELEGRAM_STREAM_MIN_DELTA_CHARS {
        return true;
    }

    let added = current.chars().skip(previous_chars).collect::<String>();
    added.contains('\n')
        || current.ends_with("。")
        || current.ends_with("！")
        || current.ends_with("？")
        || current.ends_with('.')
        || current.ends_with('!')
        || current.ends_with('?')
        || current.ends_with(':')
        || current.ends_with('：')
}

fn should_attempt_stream_sync(state: &StreamedTelegramReply) -> bool {
    let markup_key = state
        .approval_request
        .as_ref()
        .map(|approval| approval.markup_key());
    let markup_changed = state.last_markup_key != markup_key;
    markup_changed || should_render_stream_update(&state.last_rendered_body, &state.last_body)
}

fn cron_field_matches(field: &str, value: u32) -> bool {
    if field == "*" {
        return true;
    }
    if let Some(step) = field.strip_prefix("*/") {
        return step.parse::<u32>().is_ok_and(|n| n > 0 && value % n == 0);
    }
    field.parse::<u32>().is_ok_and(|n| n == value)
}

fn cron_matches(expr: &str, now: &chrono::DateTime<chrono::Local>) -> bool {
    use chrono::Timelike as _;
    use chrono::Datelike as _;
    let f: Vec<&str> = expr.split_whitespace().collect();
    if f.len() != 5 {
        return false;
    }
    cron_field_matches(f[0], now.minute())
        && cron_field_matches(f[1], now.hour())
        && cron_field_matches(f[2], now.day())
        && cron_field_matches(f[3], now.month())
        && cron_field_matches(f[4], now.weekday().num_days_from_sunday())
}

fn parse_telegram_retry_after_secs(error_text: &str) -> Option<u64> {
    let marker = "retry after ";
    let start = error_text.find(marker)? + marker.len();
    let digits: String = error_text[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse::<u64>().ok()
    }
}

fn split_message(input: &str, max_len: usize) -> Vec<String> {
    if input.is_empty() {
        return vec!["<empty>".to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();

    for ch in input.chars() {
        if current.len() + ch.len_utf8() > max_len {
            chunks.push(current);
            current = String::new();
        }
        current.push(ch);
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn load_offset(path: &Path) -> Result<Option<i64>> {
    match std::fs::read_to_string(path) {
        Ok(content) => {
            let trimmed = content.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.parse()?))
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn save_offset(path: &Path, offset: Option<i64>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = offset.map(|value| value.to_string()).unwrap_or_default();
    std::fs::write(path, body)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::LlmApprovalKind;
    use tempfile::tempdir;

    #[test]
    fn message_splitting_preserves_content() {
        let chunks = split_message("abcdef", 2);
        assert_eq!(chunks, vec!["ab", "cd", "ef"]);
    }

    #[test]
    fn help_command_returns_usage_text() {
        let help = help_text();
        assert!(help.contains("/help"));
        assert!(help.contains("/stop"));
        assert!(help.contains("/threads use"));
        assert!(help.contains("/threads"));
        assert!(help.contains("LLM"));
        assert!(help.contains("/news_watch"));
        assert!(help.contains("/smart_money"));
    }

    #[test]
    fn truncation_adds_notice() {
        let truncated = truncate_reply("abcdefgh", 5);
        assert!(truncated.contains("[output truncated]"));
    }

    #[tokio::test]
    async fn chat_session_store_supports_multiple_threads() {
        let dir = tempdir().unwrap();
        let store = ChatSessionStore::load(dir.path().join("telegram-chat-sessions.json"))
            .await
            .unwrap();
        let scope = "chat:1";

        let first = store.current_or_create(scope).await.unwrap();
        let second = store.create_new(scope).await.unwrap();
        let view = store.list(scope).await.unwrap();

        assert_ne!(first, second);
        assert_eq!(view.active_session_id.as_deref(), Some(second.as_str()));
        assert_eq!(view.sessions.len(), 2);

        let switched = store
            .switch_to(scope, short_session_id(&first))
            .await
            .unwrap();
        assert_eq!(switched, first);
    }

    #[tokio::test]
    async fn reset_current_replaces_active_thread() {
        let dir = tempdir().unwrap();
        let store = ChatSessionStore::load(dir.path().join("telegram-chat-sessions.json"))
            .await
            .unwrap();
        let scope = "chat:1";

        let first = store.current_or_create(scope).await.unwrap();
        let replacement = store.reset_current(scope).await.unwrap();
        let view = store.list(scope).await.unwrap();

        assert_ne!(first, replacement);
        assert_eq!(
            view.active_session_id.as_deref(),
            Some(replacement.as_str())
        );
        assert_eq!(view.sessions.len(), 1);
        assert_eq!(view.sessions[0].id, replacement);
    }

    #[test]
    fn shortcut_prompts_are_mapped() {
        assert_eq!(shortcut_prompt("/news_watch"), Some(PROMPT_NEWS_WATCH));
        assert_eq!(
            shortcut_prompt("/news_watch@edgeai"),
            Some(PROMPT_NEWS_WATCH)
        );
        assert_eq!(shortcut_prompt("/smart_money"), Some(PROMPT_SMART_MONEY));
        assert_eq!(shortcut_prompt("/unknown"), None);
    }

    #[test]
    fn threads_subcommands_are_parsed() {
        assert!(matches!(
            ThreadsCommand::parse("/threads").unwrap(),
            Some(ThreadsCommand::Show)
        ));
        assert!(matches!(
            ThreadsCommand::parse("/threads new").unwrap(),
            Some(ThreadsCommand::New { title: None })
        ));
        assert!(matches!(
            ThreadsCommand::parse("/threads new Macro Radar").unwrap(),
            Some(ThreadsCommand::New {
                title: Some("Macro Radar")
            })
        ));
        assert!(matches!(
            ThreadsCommand::parse("/threads rename Smart Money").unwrap(),
            Some(ThreadsCommand::Rename {
                title: "Smart Money"
            })
        ));
        assert!(matches!(
            ThreadsCommand::parse("/threads delete").unwrap(),
            Some(ThreadsCommand::Delete)
        ));
        assert!(matches!(
            ThreadsCommand::parse("/threads use abc123").unwrap(),
            Some(ThreadsCommand::Use { id: "abc123" })
        ));
    }

    #[test]
    fn forum_detection_accepts_topic_messages() {
        let mut message: Message = serde_json::from_str(
            r#"{
                "message_id": 1,
                "date": 1,
                "chat": {"id": -1001, "type": "supergroup", "title": "Test"},
                "text": "/new test",
                "is_topic_message": true
            }"#,
        )
        .unwrap();
        assert!(is_forum_message(&message));

        message.is_topic_message = Some(false);
        message.message_thread_id = Some(42);
        assert!(is_forum_message(&message));
    }

    #[test]
    fn threads_command_aliases_are_supported() {
        assert!(ThreadsCommand::parse("/threads").unwrap().is_some());
        assert!(ThreadsCommand::parse("/threads@edgeai").unwrap().is_some());
        assert!(ThreadsCommand::parse("/threads new x").unwrap().is_some());
        assert!(ThreadsCommand::parse("/threads@edgeai new x").unwrap().is_some());
        assert!(ThreadsCommand::parse("/sessions").unwrap().is_none());
        assert!(ThreadsCommand::parse("/thread").unwrap().is_none());
    }

    #[test]
    fn stop_command_is_detected() {
        assert!(is_stop_text("/stop"));
        assert!(is_stop_text("/stop@edgeai"));
        assert!(is_stop_text(" /stop "));
        assert!(!is_stop_text("/stop now"));
    }

    #[test]
    fn help_command_accepts_bot_mentions() {
        assert!(is_help_command("/help"));
        assert!(is_help_command("/help@edgeai"));
        assert!(!is_help_command("/help now"));
    }

    #[test]
    fn streamed_body_keeps_thinking_until_finish() {
        assert_eq!(render_streamed_body("", false), "🧠thinking...");
        assert_eq!(
            render_streamed_body("partial reply", false),
            "partial reply\n\n🧠thinking..."
        );
        assert_eq!(render_streamed_body("final reply", true), "final reply");
    }

    #[tokio::test]
    async fn topic_state_store_tracks_manual_and_auto_names() {
        let store = TopicStateStore::default();

        assert!(store.should_auto_rename(1, 10).await);
        store.mark_manual_named(1, 10).await;
        assert!(!store.should_auto_rename(1, 10).await);

        assert!(store.should_auto_rename(1, 11).await);
        store.mark_auto_renamed(1, 11).await;
        assert!(!store.should_auto_rename(1, 11).await);
    }

    #[test]
    fn forum_topics_default_to_new_chat_name() {
        assert_eq!(forum_topic_name(), "New Chat");
    }

    #[tokio::test]
    async fn active_run_store_keeps_queued_run_after_current_finishes() {
        let store = ActiveRunStore::default();
        let (tx1, _rx1) = mpsc::unbounded_channel();
        let (tx2, _rx2) = mpsc::unbounded_channel();
        let handle1 = tokio::spawn(async {}).abort_handle();
        let handle2 = tokio::spawn(async {}).abort_handle();

        store
            .insert("chat:1".to_string(), "run-1".to_string(), handle1, tx1)
            .await;
        store
            .insert("chat:1".to_string(), "run-2".to_string(), handle2, tx2)
            .await;
        store
            .remove("chat:1", "run-1")
            .await;
        store
            .set_pending_approval(
                "chat:1",
                LlmApprovalRequest {
                    request_id: "req-1".to_string(),
                    kind: LlmApprovalKind::Permissions,
                    command: None,
                    reason: None,
                    allow_accept_for_session: false,
                    allow_cancel: false,
                },
            )
            .await;
        let (sent, _) = store
            .submit_approval(
                "chat:1",
                LlmApprovalDecision {
                    request_id: "req-1".to_string(),
                    choice: LlmApprovalChoice::Accept,
                },
            )
            .await;

        assert!(sent);
    }

    #[test]
    fn callback_forum_detection_uses_message_shape() {
        let message: MaybeInaccessibleMessage = serde_json::from_str(
            r#"{
                "message_id": 1,
                "date": 1,
                "chat": {"id": -1001, "type": "supergroup", "title": "Test", "is_forum": true},
                "message_thread_id": 42
            }"#,
        )
        .unwrap();

        assert!(callback_message_is_forum_topic(&message));
    }
}
