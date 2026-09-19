//! Instagram 新規投稿の Discord 転送（§3.15）。
//!
//! Instagram API with Instagram Login（`graph.instagram.com`）でオーナー自身の投稿を定期取得し、
//! 新規投稿を転送先チャンネルへ流す。オーナー個人の単一アカウント運用（`instagram_account` は
//! id = 1 の 1 行のみ・V21）で、設定は `config.yaml` / `.env` から [`InstagramSettings`] として
//! 注入される（`INSTAGRAM_CHANNEL_ID` 未設定なら本サービスは登録されない）。
//!
//! # 新規投稿の判定
//! `last_post_timestamp`（転送済みの最新投稿時刻）をカーソルとし、**これより新しい投稿**を未送信と
//! みなす。投稿 ID は順序を持たないため「前回の最新 ID の位置まで遡る」方式は採らない。その方式は
//! カーソルの投稿が削除された場合や取得件数を超える投稿があった場合に位置を見失い、過去投稿の
//! 一斉再送や取りこぼしを起こす。カーソルは **1 件転送するごとに**前進させるので、途中で配信に
//! 失敗しても重複送信・取りこぼしが発生しない。
//!
//! # トークン
//! 長期トークンは 60 日で失効し、リフレッシュすると値自体が変わるため `.env` では運用できない。
//! システム鍵で暗号化して DB に保存し（V21・§6.2）、失効前に自動リフレッシュする。
//! アクセストークンは単体で Graph API を呼べる資格情報のため、ログ・エラーメッセージへ本体を
//! 出力してはならない（URL 全体のログ出力も禁止＝クエリにトークンが乗る）。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Utc};
use rusqlite::OptionalExtension;
use yuuka_core::secrets::{ExposeSecret, SecretString};
use yuuka_core::{BotId, DbError, UserId};
use yuuka_crypto::SystemCrypto;
use yuuka_db::map_sqlite;

use crate::context::ServiceContext;
use crate::notifier::{Notification, NotifyTarget};
use crate::schedule::{CronService, Schedule};

const GRAPH_BASE: &str = "https://graph.instagram.com";

/// 取得するメディアフィールド（カルーセルは children で子メディアまで取る）。
const MEDIA_FIELDS: &str = "id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,\
                            username,children{id,media_type,media_url,thumbnail_url}";

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// 1 回のポーリングで転送する最大件数（まとめ投稿でのチャンネル氾濫を防ぐ・残りは次 tick）。
const MAX_POSTS_PER_TICK: usize = 5;

/// 失効予定までこの日数を切ったらリフレッシュする。
const REFRESH_THRESHOLD_DAYS: i64 = 10;

/// リフレッシュ失敗時の再試行間隔（失効予定が不明なトークンでの試行過多を防ぐ）。
const REFRESH_RETRY_INTERVAL: chrono::Duration = chrono::Duration::hours(1);

/// キャプションの転送上限（Discord の 1 メッセージ 2000 文字に対し URL 分の余裕を残す）。
const MAX_CAPTION_CHARS: usize = 1500;

/// Instagram 連携の起動時設定（main が `Config` + `SystemCrypto` から組み立てる）。
pub struct InstagramSettings {
    /// 転送先 Discord チャンネル ID（`INSTAGRAM_CHANNEL_ID`）。
    pub channel_id: String,
    /// ポーリング間隔（5-field cron 式・`INSTAGRAM_POLL_CRON`）。
    pub poll_cron: String,
    /// チャンネル閲覧資格の検証対象＝オーナーの Discord ユーザー ID。
    pub owner_user_id: String,
    /// 配信元 Bot。
    pub bot_id: String,
    /// 初回連携用アクセストークン（`INSTAGRAM_ACCESS_TOKEN`・短期可）。DB 未連携時のみ使う。
    pub seed_token: Option<SecretString>,
    /// アプリシークレット（`INSTAGRAM_APP_SECRET`）。短期→長期トークンの交換に必要。
    pub app_secret: Option<SecretString>,
    /// 保存時暗号化（トークンの at-rest 暗号化）。
    pub crypto: Arc<SystemCrypto>,
}

impl std::fmt::Debug for InstagramSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // トークン・シークレットは絶対に出さない。
        f.debug_struct("InstagramSettings")
            .field("channel_id", &self.channel_id)
            .field("poll_cron", &self.poll_cron)
            .finish_non_exhaustive()
    }
}

/// DB に保存された連携状態（`instagram_account` の 1 行）。
#[derive(Debug, Clone, Default)]
struct AccountRow {
    token_encrypted: String,
    token_iv: String,
    token_tag: String,
    token_expires_at: Option<String>,
    last_refreshed_at: Option<String>,
    last_post_id: Option<String>,
    last_post_timestamp: Option<String>,
}

/// Graph API から取り出した 1 件のメディア。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Media {
    /// メディア ID。
    pub id: String,
    /// 投稿時刻（RFC3339・`+0000` 形式）。
    pub timestamp: String,
    /// `IMAGE` / `VIDEO` / `CAROUSEL_ALBUM` 等。
    pub media_type: String,
    /// 本体 URL（VIDEO は mp4）。
    pub media_url: Option<String>,
    /// サムネイル URL（VIDEO で使う）。
    pub thumbnail_url: Option<String>,
    /// 投稿ページの URL。
    pub permalink: Option<String>,
    /// キャプション。
    pub caption: Option<String>,
    /// 投稿者名。
    pub username: Option<String>,
    /// カルーセルの子メディア。
    pub children: Vec<MediaChild>,
}

/// カルーセルの子メディア。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MediaChild {
    /// 種別。
    pub media_type: String,
    /// 本体 URL。
    pub media_url: Option<String>,
    /// サムネイル URL。
    pub thumbnail_url: Option<String>,
}

/// Instagram 新規投稿の転送サービス。
pub struct InstagramFeedService {
    settings: Arc<InstagramSettings>,
}

impl InstagramFeedService {
    /// 設定からサービスを作る。
    #[must_use]
    pub fn new(settings: Arc<InstagramSettings>) -> Self {
        Self { settings }
    }
}

#[async_trait]
impl CronService for InstagramFeedService {
    fn name(&self) -> &'static str {
        "instagram"
    }

    fn schedule(&self) -> Schedule {
        Schedule::CronExpr(self.settings.poll_cron.clone())
    }

    async fn tick(&self, ctx: &ServiceContext) {
        if let Err(e) = run(ctx, &self.settings).await {
            tracing::error!(error = %e, "[Instagram] 新規投稿の確認に失敗しました");
        }
    }
}

// ─── ポーリング本体 ──────────────────────────────────────────────────────────

async fn run(ctx: &ServiceContext, cfg: &InstagramSettings) -> Result<(), DbError> {
    if !ensure_account(ctx, cfg).await? {
        return Ok(());
    }
    let Some(account) = load_account(ctx).await? else {
        return Ok(());
    };
    refresh_if_needed(ctx, cfg, &account).await?;

    // リフレッシュでトークンが差し替わっている可能性があるため読み直す。
    let Some(account) = load_account(ctx).await? else {
        return Ok(());
    };
    let Some(token) = decrypt_token(cfg, &account) else {
        return Ok(());
    };

    let media = match fetch_recent_media(&token).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "[Instagram] メディア一覧の取得に失敗しました");
            return Ok(());
        }
    };
    mark_checked(ctx).await?;
    if media.is_empty() {
        return Ok(());
    }

    // 初回はカーソルを立てるだけで転送しない（既存投稿が一斉に流れるのを防ぐ）。
    if account.last_post_timestamp.is_none() {
        if let Some(newest) = newest_media(&media) {
            save_cursor(ctx, &newest.id, &newest.timestamp).await?;
            tracing::info!(
                "📸 [Instagram] 初回同期のため既存投稿は転送せず、以降の新規投稿から転送します"
            );
        }
        return Ok(());
    }

    let new_posts = select_new_posts(
        &media,
        account.last_post_timestamp.as_deref(),
        account.last_post_id.as_deref(),
    );
    if new_posts.is_empty() {
        return Ok(());
    }

    let batch_len = new_posts.len().min(MAX_POSTS_PER_TICK);
    if new_posts.len() > batch_len {
        tracing::info!(
            total = new_posts.len(),
            sending = batch_len,
            "📸 [Instagram] 新規投稿のうち一部を転送します（残りは次回）"
        );
    }

    for post in new_posts.iter().take(batch_len) {
        let notification = Notification::text(
            UserId::new(cfg.owner_user_id.clone()),
            BotId::new(cfg.bot_id.clone()),
            render_post(post),
        )
        .with_target(NotifyTarget::Channel(cfg.channel_id.clone()));

        if !ctx.notifier.send(notification).await {
            // 配信できなければカーソルを進めず、次 tick で再試行する。
            tracing::warn!(id = %post.id, "[Instagram] 転送に失敗したため次回再試行します");
            break;
        }
        save_cursor(ctx, &post.id, &post.timestamp).await?;
        tracing::info!(id = %post.id, "📸 [Instagram] 新しい投稿を転送しました");
    }
    Ok(())
}

// ─── 新規投稿の抽出（純ロジック） ────────────────────────────────────────────

/// RFC3339（Graph API は `2026-09-19T12:00:00+0000`）を解釈する。
fn parse_ts(ts: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .or_else(|| DateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S%z").ok())
}

/// 未転送の投稿を古い順で返す。カーソル未設定なら空（初回は転送しない）。
///
/// 時刻で判定するため、カーソルの投稿が削除されていても取りこぼし・再送が起きない。
#[must_use]
pub fn select_new_posts(media: &[Media], cursor_ts: Option<&str>, cursor_id: Option<&str>) -> Vec<Media> {
    let Some(cursor) = cursor_ts.and_then(parse_ts) else {
        return Vec::new();
    };
    let mut out: Vec<Media> = media
        .iter()
        .filter(|m| !m.id.is_empty())
        .filter(|m| parse_ts(&m.timestamp).is_some_and(|t| t > cursor))
        .filter(|m| Some(m.id.as_str()) != cursor_id)
        .cloned()
        .collect();
    out.sort_by_key(|m| parse_ts(&m.timestamp).map(|t| t.timestamp()).unwrap_or(0));
    out
}

/// 取得結果のうち最も新しい投稿（初回のカーソル設定用）。
#[must_use]
pub fn newest_media(media: &[Media]) -> Option<Media> {
    media
        .iter()
        .filter(|m| !m.id.is_empty() && parse_ts(&m.timestamp).is_some())
        .max_by_key(|m| parse_ts(&m.timestamp).map(|t| t.timestamp()).unwrap_or(0))
        .cloned()
}

/// 表示に使う画像 URL。
///
/// VIDEO の `media_url` は mp4 で Discord が画像展開しないため、サムネイルを使う。
/// カルーセルは先頭の子メディアを代表画像とする。
#[must_use]
pub fn primary_image_url(post: &Media) -> Option<String> {
    match post.media_type.as_str() {
        "VIDEO" => post.thumbnail_url.clone(),
        "CAROUSEL_ALBUM" => post.children.first().map_or_else(
            || post.media_url.clone().or_else(|| post.thumbnail_url.clone()),
            child_image_url,
        ),
        _ => post.media_url.clone().or_else(|| post.thumbnail_url.clone()),
    }
}

fn child_image_url(child: &MediaChild) -> Option<String> {
    if child.media_type == "VIDEO" {
        child.thumbnail_url.clone()
    } else {
        child
            .media_url
            .clone()
            .or_else(|| child.thumbnail_url.clone())
    }
}

/// Discord へ送る本文を組み立てる。
///
/// 画像 URL を本文に置くと Discord 側がプレビュー展開するため、`RichEmbed`（画像フィールドを
/// 持たない）を使わずに写真を見せられる。パーマリンクは投稿ページへの導線として併記する。
///
/// 文面は秘書（早瀬ユウカ）の業務報告調。固定文言なので LLM 呼び出しは行わない
/// （cron 経路での API コスト・遅延・失敗を持ち込まない）。
#[must_use]
pub fn render_post(post: &Media) -> String {
    let mut lines: Vec<String> = Vec::new();

    lines.push("📸 新規投稿を1件確認しました。".to_owned());

    if let Some(caption) = post.caption.as_deref().map(str::trim).filter(|c| !c.is_empty()) {
        lines.push(String::new());
        lines.push(truncate_chars(caption, MAX_CAPTION_CHARS));
    }

    let note = match post.media_type.as_str() {
        "VIDEO" => "🎬 動画です。内容はリンク先でご確認ください。".to_owned(),
        "CAROUSEL_ALBUM" if post.children.len() > 1 => {
            format!(
                "🖼 写真{}枚の投稿です。先頭の1枚のみ載せておきますね。",
                post.children.len()
            )
        }
        _ => "内容は以上です。共有しておきますね。".to_owned(),
    };
    lines.push(String::new());
    lines.push(note);

    if let Some(permalink) = post.permalink.as_deref() {
        lines.push(String::new());
        lines.push(permalink.to_owned());
    }
    if let Some(image) = primary_image_url(post) {
        lines.push(image);
    }

    lines.join("\n")
}

/// 文字数（バイトではなく char）で切り詰める。
fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_owned();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push('…');
    out
}

// ─── Graph API ───────────────────────────────────────────────────────────────

/// Graph API へ GET する。
///
/// セキュリティ: アクセストークンはクエリに乗るため、URL 全体をログ・エラーへ出してはならない。
async fn graph_get(path: &str, params: &[(&str, &str)]) -> Result<serde_json::Value, String> {
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|e| format!("HTTP クライアントの構築に失敗: {e}"))?;
    let resp = client
        .get(format!("{GRAPH_BASE}{path}"))
        .query(params)
        .send()
        .await
        .map_err(|e| format!("{path} の呼び出しに失敗: {e}"))?;

    let status = resp.status();
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("{path} の応答を解析できません: {e}"))?;
    if !status.is_success() {
        let msg = body
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("詳細不明");
        return Err(format!("Instagram API エラー ({path}): {msg}"));
    }
    Ok(body)
}

/// JSON からメディア一覧を取り出す（欠損要素は落とす）。
#[must_use]
pub fn parse_media_list(body: &serde_json::Value) -> Vec<Media> {
    let Some(items) = body.get("data").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    items.iter().filter_map(parse_media).collect()
}

fn str_field(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn parse_media(v: &serde_json::Value) -> Option<Media> {
    let id = str_field(v, "id")?;
    let timestamp = str_field(v, "timestamp")?;
    parse_ts(&timestamp)?; // 時刻を解釈できない要素はカーソル判定に使えないため落とす。
    let children = v
        .get("children")
        .and_then(|c| c.get("data"))
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|c| MediaChild {
                    media_type: str_field(c, "media_type").unwrap_or_default(),
                    media_url: str_field(c, "media_url"),
                    thumbnail_url: str_field(c, "thumbnail_url"),
                })
                .collect()
        })
        .unwrap_or_default();

    Some(Media {
        id,
        timestamp,
        media_type: str_field(v, "media_type").unwrap_or_default(),
        media_url: str_field(v, "media_url"),
        thumbnail_url: str_field(v, "thumbnail_url"),
        permalink: str_field(v, "permalink"),
        caption: str_field(v, "caption"),
        username: str_field(v, "username"),
        children,
    })
}

async fn fetch_recent_media(token: &str) -> Result<Vec<Media>, String> {
    let body = graph_get(
        "/me/media",
        &[
            ("fields", MEDIA_FIELDS),
            ("limit", "25"),
            ("access_token", token),
        ],
    )
    .await?;
    Ok(parse_media_list(&body))
}

/// 短期トークンを長期トークン（60 日）へ交換する。
async fn exchange_long_lived(token: &str, app_secret: &str) -> Result<(String, Option<i64>), String> {
    let body = graph_get(
        "/access_token",
        &[
            ("grant_type", "ig_exchange_token"),
            ("client_secret", app_secret),
            ("access_token", token),
        ],
    )
    .await?;
    let new_token = body
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .ok_or("交換応答にアクセストークンが含まれません")?;
    let expires_in = body.get("expires_in").and_then(serde_json::Value::as_i64);
    Ok((new_token.to_owned(), expires_in))
}

/// 長期トークンを再発行して有効期限を延長する（発行から 24 時間以上経過したトークンのみ）。
async fn refresh_long_lived(token: &str) -> Result<(String, Option<i64>), String> {
    let body = graph_get(
        "/refresh_access_token",
        &[("grant_type", "ig_refresh_token"), ("access_token", token)],
    )
    .await?;
    let new_token = body
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .ok_or("リフレッシュ応答にアクセストークンが含まれません")?;
    let expires_in = body.get("expires_in").and_then(serde_json::Value::as_i64);
    Ok((new_token.to_owned(), expires_in))
}

// ─── トークン管理 ────────────────────────────────────────────────────────────

fn expiry_from(expires_in: Option<i64>) -> Option<String> {
    let secs = expires_in.filter(|s| *s > 0)?;
    Some((Utc::now() + chrono::Duration::seconds(secs)).to_rfc3339())
}

fn decrypt_token(cfg: &InstagramSettings, account: &AccountRow) -> Option<String> {
    match cfg.crypto.decrypt_text(
        &account.token_encrypted,
        &account.token_iv,
        &account.token_tag,
    ) {
        Ok(t) => Some(t),
        Err(e) => {
            tracing::error!(error = %e, "[Instagram] アクセストークンの復号に失敗しました（再連携が必要です）");
            None
        }
    }
}

/// DB に未連携なら設定のトークンで初回連携する。連携済み（またはできた）なら `true`。
async fn ensure_account(ctx: &ServiceContext, cfg: &InstagramSettings) -> Result<bool, DbError> {
    if load_account(ctx).await?.is_some() {
        return Ok(true);
    }
    let Some(seed) = cfg.seed_token.as_ref() else {
        tracing::warn!(
            "[Instagram] アクセストークンが未設定のため連携できません（INSTAGRAM_ACCESS_TOKEN）"
        );
        return Ok(false);
    };
    let seed = seed.expose_secret();

    if let Some(secret) = cfg.app_secret.as_ref() {
        match exchange_long_lived(seed, secret.expose_secret()).await {
            Ok((token, expires_in)) => {
                save_token(ctx, cfg, &token, expiry_from(expires_in)).await?;
                tracing::info!("📸 [Instagram] 長期アクセストークンを取得しました");
                return Ok(true);
            }
            Err(e) => {
                // 既に長期トークンが設定されている場合も交換は失敗する。そのまま保存して継続する。
                tracing::warn!(error = %e, "[Instagram] 長期トークンへの交換に失敗。設定値をそのまま使用します");
            }
        }
    } else {
        tracing::warn!(
            "[Instagram] INSTAGRAM_APP_SECRET 未設定のため長期トークンへ交換しません（短期トークンは早期に失効します）"
        );
    }
    save_token(ctx, cfg, seed, None).await?;
    Ok(true)
}

/// 失効が近ければトークンをリフレッシュする。失敗しても既存トークンで継続する。
async fn refresh_if_needed(
    ctx: &ServiceContext,
    cfg: &InstagramSettings,
    account: &AccountRow,
) -> Result<(), DbError> {
    let now = Utc::now();
    let expires_at = account
        .token_expires_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
    let last_refreshed = account
        .last_refreshed_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok());

    let needs = match expires_at {
        Some(exp) => exp.with_timezone(&Utc) - now < chrono::Duration::days(REFRESH_THRESHOLD_DAYS),
        // 失効予定が不明（外部発行トークンを投入した場合）。発行直後はリフレッシュできないため、
        // 保存から 1 日以上経過していれば試みる。
        None => last_refreshed
            .is_none_or(|r| now - r.with_timezone(&Utc) > chrono::Duration::days(1)),
    };
    if !needs {
        return Ok(());
    }
    // 失敗が続くときに毎 tick 叩かないようバックオフする。
    if let Some(r) = last_refreshed {
        if expires_at.is_none() && now - r.with_timezone(&Utc) < REFRESH_RETRY_INTERVAL {
            return Ok(());
        }
    }

    let Some(token) = decrypt_token(cfg, account) else {
        return Ok(());
    };
    match refresh_long_lived(&token).await {
        Ok((new_token, expires_in)) => {
            save_token(ctx, cfg, &new_token, expiry_from(expires_in)).await?;
            tracing::info!("📸 [Instagram] アクセストークンをリフレッシュしました");
        }
        Err(e) => {
            tracing::error!(error = %e, "[Instagram] トークンのリフレッシュに失敗しました（失効前に再連携が必要な可能性があります）");
        }
    }
    Ok(())
}

// ─── instagram_account の読み書き ───────────────────────────────────────────

async fn load_account(ctx: &ServiceContext) -> Result<Option<AccountRow>, DbError> {
    ctx.db
        .read
        .read(|conn| {
            conn.query_row(
                "SELECT access_token_encrypted, access_token_iv, access_token_tag, \
                        token_expires_at, last_refreshed_at, last_post_id, last_post_timestamp \
                   FROM instagram_account WHERE id = 1",
                [],
                |r| {
                    Ok(AccountRow {
                        token_encrypted: r.get(0)?,
                        token_iv: r.get(1)?,
                        token_tag: r.get(2)?,
                        token_expires_at: r.get(3)?,
                        last_refreshed_at: r.get(4)?,
                        last_post_id: r.get(5)?,
                        last_post_timestamp: r.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(map_sqlite)
        })
        .await
}

/// トークンを保存する（初回連携・交換・リフレッシュで共用）。投稿カーソルは維持する。
async fn save_token(
    ctx: &ServiceContext,
    cfg: &InstagramSettings,
    token: &str,
    expires_at: Option<String>,
) -> Result<(), DbError> {
    let enc = match cfg.crypto.encrypt_text(token) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "[Instagram] アクセストークンの暗号化に失敗しました");
            return Ok(());
        }
    };
    ctx.db
        .writer
        .execute(move |conn| {
            conn.execute(
                "INSERT INTO instagram_account \
                   (id, access_token_encrypted, access_token_iv, access_token_tag, \
                    token_expires_at, last_refreshed_at) \
                 VALUES (1, ?1, ?2, ?3, ?4, datetime('now','localtime')) \
                 ON CONFLICT(id) DO UPDATE SET \
                   access_token_encrypted = excluded.access_token_encrypted, \
                   access_token_iv = excluded.access_token_iv, \
                   access_token_tag = excluded.access_token_tag, \
                   token_expires_at = excluded.token_expires_at, \
                   last_refreshed_at = excluded.last_refreshed_at, \
                   updated_at = datetime('now','localtime')",
                rusqlite::params![enc.encrypted, enc.iv, enc.auth_tag, expires_at],
            )
            .map_err(map_sqlite)?;
            Ok(())
        })
        .await
}

/// 転送済みカーソルを前進させる（配信に成功した投稿でのみ呼ぶ）。
async fn save_cursor(ctx: &ServiceContext, post_id: &str, timestamp: &str) -> Result<(), DbError> {
    let post_id = post_id.to_owned();
    let timestamp = timestamp.to_owned();
    ctx.db
        .writer
        .execute(move |conn| {
            conn.execute(
                "UPDATE instagram_account \
                    SET last_post_id = ?1, last_post_timestamp = ?2, \
                        updated_at = datetime('now','localtime') \
                  WHERE id = 1",
                rusqlite::params![post_id, timestamp],
            )
            .map_err(map_sqlite)?;
            Ok(())
        })
        .await
}

async fn mark_checked(ctx: &ServiceContext) -> Result<(), DbError> {
    ctx.db
        .writer
        .execute(|conn| {
            conn.execute(
                "UPDATE instagram_account SET last_checked_at = datetime('now','localtime') WHERE id = 1",
                [],
            )
            .map_err(map_sqlite)?;
            Ok(())
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ctx_null, seeded_db};

    const DDL: &str = "CREATE TABLE instagram_account (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        access_token_encrypted TEXT NOT NULL, access_token_iv TEXT NOT NULL,
        access_token_tag TEXT NOT NULL, token_expires_at TEXT, last_refreshed_at TEXT,
        last_post_id TEXT, last_post_timestamp TEXT, last_checked_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    );";

    fn post(id: &str, ts: &str) -> Media {
        Media {
            id: id.to_owned(),
            timestamp: ts.to_owned(),
            media_type: "IMAGE".to_owned(),
            media_url: Some(format!("https://cdn.example/{id}.jpg")),
            permalink: Some(format!("https://instagram.com/p/{id}")),
            caption: Some(format!("投稿 {id}")),
            username: Some("yanas".to_owned()),
            ..Media::default()
        }
    }

    fn sample() -> Vec<Media> {
        vec![
            post("P3", "2026-09-18T12:00:00+0000"),
            post("P2", "2026-09-17T12:00:00+0000"),
            post("P1", "2026-09-16T12:00:00+0000"),
        ]
    }

    fn ids(v: &[Media]) -> Vec<&str> {
        v.iter().map(|m| m.id.as_str()).collect()
    }

    // ─── 新規投稿の抽出 ─────────────────────────────────────────────────────

    #[test]
    fn cursor_unset_selects_nothing() {
        // 初回は既存投稿を一斉転送せず、カーソル設定のみ行う。
        assert!(select_new_posts(&sample(), None, None).is_empty());
    }

    #[test]
    fn selects_newer_than_cursor_oldest_first() {
        let got = select_new_posts(&sample(), Some("2026-09-16T12:00:00+0000"), Some("P1"));
        assert_eq!(ids(&got), ["P2", "P3"]);
    }

    #[test]
    fn no_new_posts_selects_nothing() {
        let got = select_new_posts(&sample(), Some("2026-09-18T12:00:00+0000"), Some("P3"));
        assert!(got.is_empty());
    }

    #[test]
    fn deleted_cursor_post_still_selects_only_new() {
        // ID を辿る方式では位置を見失い過去投稿を再送してしまうケース。
        let mut media = vec![post("P4", "2026-09-19T12:00:00+0000")];
        media.extend(sample());
        let got = select_new_posts(&media, Some("2026-09-18T18:00:00+0000"), Some("DELETED"));
        assert_eq!(ids(&got), ["P4"]);
    }

    #[test]
    fn cursor_id_is_excluded_at_boundary() {
        let got = select_new_posts(&sample(), Some("2026-09-17T12:00:00+0000"), Some("P2"));
        assert_eq!(ids(&got), ["P3"]);
    }

    #[test]
    fn unparsable_timestamp_is_ignored() {
        let media = vec![
            post("OK", "2026-09-19T12:00:00+0000"),
            post("BAD", "not-a-date"),
        ];
        let got = select_new_posts(&media, Some("2026-09-18T00:00:00+0000"), None);
        assert_eq!(ids(&got), ["OK"]);
    }

    #[test]
    fn newest_media_ignores_fetch_order() {
        let media = vec![
            post("B", "2026-09-17T12:00:00+0000"),
            post("C", "2026-09-19T12:00:00+0000"),
            post("A", "2026-09-16T12:00:00+0000"),
        ];
        assert_eq!(newest_media(&media).unwrap().id, "C");
        assert!(newest_media(&[]).is_none());
    }

    // ─── メディア種別ごとの画像 ─────────────────────────────────────────────

    #[test]
    fn image_uses_media_url() {
        let p = post("P", "2026-09-19T12:00:00+0000");
        assert_eq!(
            primary_image_url(&p).as_deref(),
            Some("https://cdn.example/P.jpg")
        );
    }

    #[test]
    fn video_uses_thumbnail_not_mp4() {
        // mp4 は Discord がプレビュー展開しないためサムネイルを使う。
        let mut p = post("V", "2026-09-19T12:00:00+0000");
        p.media_type = "VIDEO".to_owned();
        p.media_url = Some("https://cdn.example/V.mp4".to_owned());
        p.thumbnail_url = Some("https://cdn.example/V.jpg".to_owned());
        assert_eq!(
            primary_image_url(&p).as_deref(),
            Some("https://cdn.example/V.jpg")
        );
    }

    #[test]
    fn carousel_uses_first_child() {
        let mut p = post("C", "2026-09-19T12:00:00+0000");
        p.media_type = "CAROUSEL_ALBUM".to_owned();
        p.media_url = None;
        p.children = vec![
            MediaChild {
                media_type: "IMAGE".to_owned(),
                media_url: Some("https://cdn.example/c1.jpg".to_owned()),
                thumbnail_url: None,
            },
            MediaChild {
                media_type: "IMAGE".to_owned(),
                media_url: Some("https://cdn.example/c2.jpg".to_owned()),
                thumbnail_url: None,
            },
        ];
        assert_eq!(
            primary_image_url(&p).as_deref(),
            Some("https://cdn.example/c1.jpg")
        );
    }

    #[test]
    fn carousel_first_video_uses_its_thumbnail() {
        let mut p = post("C", "2026-09-19T12:00:00+0000");
        p.media_type = "CAROUSEL_ALBUM".to_owned();
        p.media_url = None;
        p.children = vec![MediaChild {
            media_type: "VIDEO".to_owned(),
            media_url: Some("https://cdn.example/c1.mp4".to_owned()),
            thumbnail_url: Some("https://cdn.example/c1.jpg".to_owned()),
        }];
        assert_eq!(
            primary_image_url(&p).as_deref(),
            Some("https://cdn.example/c1.jpg")
        );
    }

    #[test]
    fn video_without_thumbnail_has_no_image() {
        let mut p = post("N", "2026-09-19T12:00:00+0000");
        p.media_type = "VIDEO".to_owned();
        p.media_url = Some("https://cdn.example/N.mp4".to_owned());
        p.thumbnail_url = None;
        assert!(primary_image_url(&p).is_none());
    }

    // ─── 本文の組み立て ─────────────────────────────────────────────────────

    #[test]
    fn rendered_post_carries_caption_permalink_and_image() {
        let body = render_post(&post("P", "2026-09-19T12:00:00+0000"));
        assert!(body.starts_with("📸 新規投稿を1件確認しました。"));
        assert!(body.contains("投稿 P"));
        assert!(body.contains("内容は以上です。共有しておきますね。"));
        assert!(body.contains("https://instagram.com/p/P"));
        // 画像 URL を本文に置くことで Discord がプレビュー展開する。
        assert!(body.contains("https://cdn.example/P.jpg"));
    }

    #[test]
    fn rendered_video_notes_media_type() {
        let mut p = post("V", "2026-09-19T12:00:00+0000");
        p.media_type = "VIDEO".to_owned();
        p.thumbnail_url = Some("https://cdn.example/V.jpg".to_owned());
        let body = render_post(&p);
        assert!(body.contains("🎬 動画です。"));
        assert!(body.contains("https://cdn.example/V.jpg"));
    }

    #[test]
    fn rendered_carousel_notes_image_count() {
        let mut p = post("C", "2026-09-19T12:00:00+0000");
        p.media_type = "CAROUSEL_ALBUM".to_owned();
        p.children = vec![
            MediaChild {
                media_type: "IMAGE".to_owned(),
                media_url: Some("https://cdn.example/c1.jpg".to_owned()),
                thumbnail_url: None,
            },
            MediaChild {
                media_type: "IMAGE".to_owned(),
                media_url: Some("https://cdn.example/c2.jpg".to_owned()),
                thumbnail_url: None,
            },
        ];
        assert!(render_post(&p).contains("🖼 写真2枚の投稿です。"));
    }

    #[test]
    fn long_caption_is_truncated() {
        let mut p = post("L", "2026-09-19T12:00:00+0000");
        p.caption = Some("あ".repeat(MAX_CAPTION_CHARS + 500));
        let body = render_post(&p);
        assert!(body.contains('…'));
        // 切り詰め後もパーマリンクは残る（Discord の 2000 文字上限内に収める）。
        assert!(body.contains("https://instagram.com/p/L"));
    }

    // ─── Graph API 応答の解釈 ───────────────────────────────────────────────

    #[test]
    fn parses_media_list_and_drops_broken_entries() {
        let body = serde_json::json!({
            "data": [
                {
                    "id": "A", "timestamp": "2026-09-19T12:00:00+0000",
                    "media_type": "CAROUSEL_ALBUM", "permalink": "https://instagram.com/p/A",
                    "children": { "data": [
                        { "id": "c1", "media_type": "IMAGE", "media_url": "https://cdn.example/c1.jpg" }
                    ]}
                },
                { "id": "NO_TS", "media_type": "IMAGE" },
                { "timestamp": "2026-09-19T13:00:00+0000", "media_type": "IMAGE" }
            ]
        });
        let media = parse_media_list(&body);
        assert_eq!(ids(&media), ["A"]);
        assert_eq!(media[0].children.len(), 1);
    }

    #[test]
    fn parses_empty_payload() {
        assert!(parse_media_list(&serde_json::json!({})).is_empty());
    }

    // ─── DB ラウンドトリップ ────────────────────────────────────────────────

    fn settings(crypto: Arc<SystemCrypto>) -> InstagramSettings {
        InstagramSettings {
            channel_id: "123".to_owned(),
            poll_cron: "*/20 * * * *".to_owned(),
            owner_user_id: "owner".to_owned(),
            bot_id: "system_default".to_owned(),
            seed_token: None,
            app_secret: None,
            crypto,
        }
    }

    #[tokio::test]
    async fn token_round_trips_encrypted_and_cursor_survives_refresh() {
        let (db, _dir) = seeded_db(DDL);
        let ctx = ctx_null(db);
        let crypto = Arc::new(SystemCrypto::new(SecretString::from("master-secret-for-test")).unwrap());
        let cfg = settings(crypto);

        assert!(load_account(&ctx).await.unwrap().is_none());

        save_token(&ctx, &cfg, "IG_TOKEN_1", Some("2026-11-18T00:00:00+00:00".to_owned()))
            .await
            .unwrap();
        let row = load_account(&ctx).await.unwrap().unwrap();
        // 平文で保存されていない。
        assert!(!row.token_encrypted.contains("IG_TOKEN_1"));
        assert_eq!(decrypt_token(&cfg, &row).as_deref(), Some("IG_TOKEN_1"));

        save_cursor(&ctx, "POST_A", "2026-09-18T10:00:00+0000")
            .await
            .unwrap();

        // リフレッシュ（トークン差し替え）でカーソルが消えない。
        save_token(&ctx, &cfg, "IG_TOKEN_2", None).await.unwrap();
        let row = load_account(&ctx).await.unwrap().unwrap();
        assert_eq!(decrypt_token(&cfg, &row).as_deref(), Some("IG_TOKEN_2"));
        assert_eq!(row.last_post_id.as_deref(), Some("POST_A"));
        assert_eq!(
            row.last_post_timestamp.as_deref(),
            Some("2026-09-18T10:00:00+0000")
        );

        mark_checked(&ctx).await.unwrap();
    }

    #[tokio::test]
    async fn service_is_registered_only_when_configured() {
        let (db, _dir) = seeded_db(DDL);
        let ctx = ctx_null(db);
        let before = crate::build_services(&ctx).len();

        let crypto = Arc::new(SystemCrypto::new(SecretString::from("master-secret-for-test")).unwrap());
        let ctx = ctx.with_instagram(Arc::new(settings(crypto)));
        let svcs = crate::build_services(&ctx);
        assert_eq!(svcs.len(), before + 1);
        assert!(svcs.iter().any(|s| s.name() == "instagram"));
    }
}
