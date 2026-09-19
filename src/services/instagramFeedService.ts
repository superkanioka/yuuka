import { EmbedBuilder } from "discord.js";
import cron from "node-cron";
import { client } from "../bot.js";
import { config } from "../config.js";
import {
	getInstagramAccessToken,
	getInstagramAccount,
	hasInstagramAccount,
	type InstagramAccount,
	markInstagramChecked,
	saveInstagramToken,
	updateInstagramCursor,
	updateInstagramProfile,
} from "../db/instagramRepo.js";

// ─── Instagram新規投稿のDiscord転送（§3.15） ─────────────────────────────────
// Instagram API with Instagram Login（graph.instagram.com）で自分の投稿を定期取得し、
// 新規投稿をDiscordチャンネルへ転送する。自分専用の単一アカウント運用。
//
// 新規判定は last_post_timestamp（送信済みの最新投稿時刻）より新しい投稿、という基準で行う。
// 「前回の最新投稿IDの位置まで遡る」方式は、そのIDが取得範囲から外れた場合（削除・多数投稿）に
// 位置を見失い、取りこぼしや過去投稿の再送を起こすため採用しない。
//
// トークン: 長期トークンは60日で失効し、リフレッシュすると値自体が変わる。
// このため .env ではなくDBへ暗号化保存し（instagramRepo）、失効前に自動リフレッシュする。

const GRAPH_BASE = "https://graph.instagram.com";

/** 取得するメディアフィールド（カルーセルは children で子メディアまで取得する） */
const MEDIA_FIELDS =
	"id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,username," +
	"children{id,media_type,media_url,thumbnail_url}";

const REQUEST_TIMEOUT_MS = 15_000;
const DAY_MS = 24 * 60 * 60 * 1000;

/** 1回のポーリングで送信する最大件数（まとめ投稿によるチャンネル氾濫を防ぐ。残りは次回送信） */
const MAX_POSTS_PER_TICK = 5;

/** 失効予定までこの日数を切ったらリフレッシュする */
const REFRESH_THRESHOLD_DAYS = 10;

/** リフレッシュ失敗時の再試行間隔（失効予定が不明なトークンでの試行過多を防ぐ） */
const REFRESH_RETRY_INTERVAL_MS = 60 * 60 * 1000;

/** Discordの1メッセージあたりのembed上限 */
const MAX_EMBEDS_PER_MESSAGE = 10;

/** embed description の実用上限（Discordの上限4096に対し余裕を持たせる） */
const MAX_DESCRIPTION = 4000;

/** Instagramブランドカラー */
const INSTAGRAM_COLOR = 0xe1306c;

let task: cron.ScheduledTask | null = null;

/** 直近のリフレッシュ試行時刻（失敗時のバックオフ用。プロセス内のみ保持） */
let lastRefreshAttempt = 0;

/** チャンネル解決失敗ログの抑制（20分毎に同じエラーを出し続けない） */
let channelErrorLogged = false;

export interface InstagramChild {
	id: string;
	media_type?: string;
	media_url?: string;
	thumbnail_url?: string;
}

export interface InstagramMedia {
	id: string;
	caption?: string;
	media_type?: string;
	media_url?: string;
	thumbnail_url?: string;
	permalink?: string;
	timestamp: string;
	username?: string;
	children?: { data?: InstagramChild[] };
}

// ─── Graph API 呼び出し ──────────────────────────────────────────────────────

/**
 * graph.instagram.com へGETする。
 * セキュリティ: アクセストークンはクエリに含まれるため、URL全体をログへ出力してはならない。
 */
async function graphRequest<T>(
	path: string,
	params: Record<string, string>,
): Promise<T> {
	const url = new URL(`${GRAPH_BASE}${path}`);
	for (const [key, value] of Object.entries(params)) {
		url.searchParams.set(key, value);
	}

	const res = await fetch(url, {
		signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
	});
	const text = await res.text();

	let body: unknown;
	try {
		body = JSON.parse(text);
	} catch {
		body = undefined;
	}

	if (!res.ok) {
		const message =
			(body as { error?: { message?: string } } | undefined)?.error?.message ??
			`HTTP ${res.status}`;
		throw new Error(`Instagram APIエラー (${path}): ${message}`);
	}
	if (body === undefined) {
		throw new Error(`Instagram APIの応答を解析できませんでした (${path})`);
	}
	return body as T;
}

interface TokenResponse {
	access_token?: string;
	expires_in?: number;
}

/** expires_in（秒）から失効予定時刻を求める。不正値の場合は null。 */
function toExpiryDate(expiresIn: number | undefined): Date | null {
	if (typeof expiresIn !== "number" || !Number.isFinite(expiresIn)) return null;
	if (expiresIn <= 0) return null;
	return new Date(Date.now() + expiresIn * 1000);
}

/** 短期トークンを長期トークン（60日）へ交換する。 */
async function exchangeForLongLivedToken(
	shortLivedToken: string,
): Promise<{ token: string; expiresAt: Date | null }> {
	const body = await graphRequest<TokenResponse>("/access_token", {
		grant_type: "ig_exchange_token",
		client_secret: config.instagramAppSecret,
		access_token: shortLivedToken,
	});
	if (!body.access_token) {
		throw new Error("長期トークンへの交換応答にアクセストークンが含まれません");
	}
	return { token: body.access_token, expiresAt: toExpiryDate(body.expires_in) };
}

/** 長期トークンを再発行して有効期限を60日延長する（発行から24時間以上経過したトークンのみ可）。 */
async function refreshLongLivedToken(
	token: string,
): Promise<{ token: string; expiresAt: Date | null }> {
	const body = await graphRequest<TokenResponse>("/refresh_access_token", {
		grant_type: "ig_refresh_token",
		access_token: token,
	});
	if (!body.access_token) {
		throw new Error("リフレッシュ応答にアクセストークンが含まれません");
	}
	return { token: body.access_token, expiresAt: toExpiryDate(body.expires_in) };
}

/** 自分のメディア一覧を新しい順で取得する。 */
async function fetchRecentMedia(token: string): Promise<InstagramMedia[]> {
	const body = await graphRequest<{ data?: InstagramMedia[] }>("/me/media", {
		fields: MEDIA_FIELDS,
		limit: "25",
		access_token: token,
	});
	return Array.isArray(body.data) ? body.data : [];
}

/** プロフィール（IGユーザーID・ユーザー名）を取得して記録する。失敗しても転送処理は継続する。 */
async function syncProfile(token: string): Promise<void> {
	try {
		const me = await graphRequest<{ id?: string; username?: string }>("/me", {
			fields: "id,username",
			access_token: token,
		});
		updateInstagramProfile(me.id ?? null, me.username ?? null);
	} catch (err) {
		console.warn(
			"[Instagram] プロフィールの取得に失敗しました（転送は継続します）:",
			err instanceof Error ? err.message : err,
		);
	}
}

// ─── トークン管理 ────────────────────────────────────────────────────────────

/**
 * DBにアカウントが無い場合、設定のアクセストークンで初回連携を行う。
 * @returns 連携済み（またはこの呼び出しで連携できた）なら true
 */
async function ensureAccount(): Promise<boolean> {
	if (hasInstagramAccount()) return true;

	const seedToken = config.instagramAccessToken;
	if (!seedToken) return false;

	if (config.instagramAppSecret) {
		try {
			const exchanged = await exchangeForLongLivedToken(seedToken);
			saveInstagramToken(exchanged.token, exchanged.expiresAt);
			console.log(
				`📸 [Instagram] 長期アクセストークンを取得しました（失効予定: ${
					exchanged.expiresAt?.toLocaleString("ja-JP") ?? "不明"
				}）`,
			);
			await syncProfile(exchanged.token);
			return true;
		} catch (err) {
			// 既に長期トークンが設定されている場合も交換は失敗する。そのまま保存して継続する。
			console.warn(
				"[Instagram] 長期トークンへの交換に失敗しました。設定されたトークンをそのまま使用します:",
				err instanceof Error ? err.message : err,
			);
		}
	} else {
		console.warn(
			"[Instagram] INSTAGRAM_APP_SECRET が未設定のため長期トークンへの交換を行いません。" +
				"短期トークンの場合は1時間程度で失効します。",
		);
	}

	// 失効予定が不明なトークンとして保存する（以降の tick でリフレッシュを試みる）。
	saveInstagramToken(seedToken, null);
	await syncProfile(seedToken);
	return true;
}

/** 失効が近い場合にトークンをリフレッシュする。失敗しても既存トークンで処理を継続する。 */
async function ensureFreshToken(account: InstagramAccount): Promise<void> {
	const token = getInstagramAccessToken();
	if (!token) return;

	const now = Date.now();
	const expiresAt = account.token_expires_at
		? Date.parse(account.token_expires_at)
		: Number.NaN;
	const lastRefreshed = account.last_refreshed_at
		? Date.parse(account.last_refreshed_at)
		: Number.NaN;

	let needsRefresh: boolean;
	if (!Number.isNaN(expiresAt)) {
		needsRefresh = expiresAt - now < REFRESH_THRESHOLD_DAYS * DAY_MS;
	} else {
		// 失効予定が不明（外部で発行したトークンを投入した場合）。
		// 発行直後のトークンはリフレッシュできないため、保存から1日以上経過していれば試みる。
		needsRefresh = Number.isNaN(lastRefreshed) || now - lastRefreshed > DAY_MS;
	}
	if (!needsRefresh) return;

	// 失敗が続く場合に毎回APIを叩かないようバックオフする
	if (now - lastRefreshAttempt < REFRESH_RETRY_INTERVAL_MS) return;
	lastRefreshAttempt = now;

	try {
		const refreshed = await refreshLongLivedToken(token);
		saveInstagramToken(refreshed.token, refreshed.expiresAt);
		console.log(
			`📸 [Instagram] アクセストークンをリフレッシュしました（失効予定: ${
				refreshed.expiresAt?.toLocaleString("ja-JP") ?? "不明"
			}）`,
		);
	} catch (err) {
		console.error(
			"[Instagram] アクセストークンのリフレッシュに失敗しました" +
				"（失効前に再連携が必要な可能性があります）:",
			err instanceof Error ? err.message : err,
		);
	}
}

// ─── 新規投稿の抽出 ──────────────────────────────────────────────────────────

/** id と timestamp が揃っている投稿のみを対象とする。 */
function isUsableMedia(media: InstagramMedia): boolean {
	return (
		typeof media?.id === "string" &&
		typeof media?.timestamp === "string" &&
		!Number.isNaN(Date.parse(media.timestamp))
	);
}

/**
 * 未送信の投稿を古い順で返す。
 * カーソル（last_post_timestamp）より新しい投稿のみが対象で、カーソル未設定なら空配列。
 * 時刻で判定するため、カーソルの投稿が削除されていても取りこぼし・再送が起きない。
 * @param cursor 送信済みの最新投稿（last_post_timestamp / last_post_id）
 */
export function selectNewPosts(
	media: InstagramMedia[],
	cursor: Pick<InstagramAccount, "last_post_timestamp" | "last_post_id">,
): InstagramMedia[] {
	const cursorAt = cursor.last_post_timestamp
		? Date.parse(cursor.last_post_timestamp)
		: Number.NaN;
	if (Number.isNaN(cursorAt)) return [];

	return media
		.filter(isUsableMedia)
		.filter(
			(m) => Date.parse(m.timestamp) > cursorAt && m.id !== cursor.last_post_id,
		)
		.sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp));
}

/** 取得結果のうち最も新しい投稿（初回のカーソル設定用）。 */
export function newestMedia(
	media: InstagramMedia[],
): InstagramMedia | undefined {
	return media
		.filter(isUsableMedia)
		.sort((a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp))[0];
}

// ─── Discordへの送信 ─────────────────────────────────────────────────────────

/** 子メディアの表示用画像URL（動画はサムネイルを使う）。 */
function childImageUrl(child: InstagramChild): string | undefined {
	if (child.media_type === "VIDEO") return child.thumbnail_url;
	return child.media_url ?? child.thumbnail_url;
}

/**
 * 投稿のメイン画像URL。
 * VIDEO の media_url は mp4 でありembedの画像として描画されないため、サムネイルを使う。
 */
export function primaryImageUrl(post: InstagramMedia): string | undefined {
	const children = post.children?.data ?? [];
	switch (post.media_type) {
		case "VIDEO":
			return post.thumbnail_url ?? undefined;
		case "CAROUSEL_ALBUM":
			return children.length > 0
				? childImageUrl(children[0])
				: (post.media_url ?? post.thumbnail_url);
		default:
			return post.media_url ?? post.thumbnail_url;
	}
}

function footerLabel(post: InstagramMedia): string {
	const count = post.children?.data?.length ?? 0;
	switch (post.media_type) {
		case "VIDEO":
			return "Instagram · 動画";
		case "CAROUSEL_ALBUM":
			return count > 0
				? `Instagram · 画像${count}枚`
				: "Instagram · 複数メディア";
		default:
			return "Instagram";
	}
}

/**
 * 投稿からembedを組み立てる。
 * カルーセルは2枚目以降も同じ permalink を持つembedとして並べる
 * （Discordは同一URLのembedを1つのギャラリーとしてまとめて表示する）。
 */
function buildEmbeds(post: InstagramMedia): EmbedBuilder[] {
	const title = post.username
		? `@${post.username} の新しい投稿`
		: "Instagram の新しい投稿";

	const main = new EmbedBuilder()
		.setColor(INSTAGRAM_COLOR)
		.setTitle(title)
		.setTimestamp(new Date(post.timestamp))
		.setFooter({ text: footerLabel(post) });

	if (post.permalink) main.setURL(post.permalink);

	const caption = post.caption?.trim();
	if (caption) main.setDescription(caption.slice(0, MAX_DESCRIPTION));

	const primary = primaryImageUrl(post);
	if (primary) main.setImage(primary);

	const embeds = [main];

	if (post.media_type === "CAROUSEL_ALBUM" && post.permalink) {
		const children = post.children?.data ?? [];
		for (const child of children.slice(1, MAX_EMBEDS_PER_MESSAGE)) {
			const imageUrl = childImageUrl(child);
			if (!imageUrl) continue;
			embeds.push(
				new EmbedBuilder()
					.setColor(INSTAGRAM_COLOR)
					.setURL(post.permalink)
					.setImage(imageUrl),
			);
		}
	}

	return embeds;
}

/** 転送先チャンネルを解決する。取得できない場合は null。 */
async function resolveChannel() {
	if (!client.readyAt) {
		console.warn(
			"[Instagram] Botクライアントが未接続のため送信をスキップします",
		);
		return null;
	}

	const channel = await client.channels
		.fetch(config.instagramChannelId)
		.catch(() => null);

	if (channel && channel.isTextBased() && "send" in channel) {
		channelErrorLogged = false;
		return channel;
	}

	// 毎回のポーリングで同じエラーを出し続けないよう、復旧するまで1回だけ記録する
	if (!channelErrorLogged) {
		console.error(
			`[Instagram] 転送先チャンネル ${config.instagramChannelId} が見つからないか、` +
				"テキストチャンネルではありません",
		);
		channelErrorLogged = true;
	}
	return null;
}

/** 投稿1件をDiscordへ送信する。 */
async function postToDiscord(post: InstagramMedia): Promise<boolean> {
	const channel = await resolveChannel();
	if (!channel) return false;

	try {
		await (channel as { send: (options: unknown) => Promise<unknown> }).send({
			embeds: buildEmbeds(post),
		});
		return true;
	} catch (err) {
		console.error(
			`[Instagram] 投稿 ${post.id} のDiscord送信に失敗しました:`,
			err,
		);
		return false;
	}
}

// ─── ポーリング本体 ──────────────────────────────────────────────────────────

async function tick(): Promise<void> {
	try {
		if (!(await ensureAccount())) return;

		const account = getInstagramAccount();
		if (!account) return;

		await ensureFreshToken(account);

		const token = getInstagramAccessToken();
		if (!token) return;

		const media = await fetchRecentMedia(token);
		markInstagramChecked();
		if (media.length === 0) return;

		// 初回はカーソルを設定するだけで送信しない（過去投稿が一斉に流れるのを防ぐ）
		if (!account.last_post_timestamp) {
			const newest = newestMedia(media);
			if (newest) {
				updateInstagramCursor(newest.id, newest.timestamp);
				console.log(
					"📸 [Instagram] 初回同期のため既存投稿は送信せず、以降の新規投稿から転送します",
				);
			}
			return;
		}

		const newPosts = selectNewPosts(media, account);
		if (newPosts.length === 0) return;

		const batch = newPosts.slice(0, MAX_POSTS_PER_TICK);
		if (newPosts.length > batch.length) {
			console.log(
				`📸 [Instagram] 新規投稿${newPosts.length}件のうち${batch.length}件を送信します（残りは次回）`,
			);
		}

		for (const post of batch) {
			const sent = await postToDiscord(post);
			// 送信できなかった場合はカーソルを進めず、次回のポーリングで再試行する
			if (!sent) break;
			updateInstagramCursor(post.id, post.timestamp);
			console.log(`📸 [Instagram] 新しい投稿を転送しました (${post.id})`);
		}
	} catch (err) {
		console.error("[Instagram] 新規投稿の確認に失敗しました:", err);
	}
}

// ─── サービス制御 ────────────────────────────────────────────────────────────

export function startInstagramFeedService(): void {
	if (task) return;

	if (!config.instagramChannelId) {
		console.log(
			"📸 Instagram連携は未設定のため開始しません（INSTAGRAM_CHANNEL_ID）",
		);
		return;
	}
	if (!hasInstagramAccount() && !config.instagramAccessToken) {
		console.warn(
			"📸 Instagram連携: アクセストークンが未設定のため開始しません（INSTAGRAM_ACCESS_TOKEN）",
		);
		return;
	}
	if (!cron.validate(config.instagramPollCron)) {
		console.error(
			`📸 Instagram連携: cron式が不正なため開始しません (${config.instagramPollCron})`,
		);
		return;
	}

	task = cron.schedule(config.instagramPollCron, () => {
		void tick();
	});
	console.log(
		`📸 Instagram連携サービスを開始しました（${config.instagramPollCron}）`,
	);

	// 起動直後に1回実行する（初回連携・カーソル設定をここで済ませる）
	void tick();
}

export function stopInstagramFeedService(): void {
	if (task) {
		task.stop();
		task = null;
	}
}
