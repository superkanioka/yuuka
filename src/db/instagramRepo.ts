import { decryptText, encryptText } from "../utils/crypto.js";
import { getDb } from "./database.js";

// ─── Instagram 連携（v10 / §3.15） ───────────────────────────────────────────
// 自分専用の単一アカウント運用のため instagram_account は id = 1 の1行のみを保持する。
// 長期アクセストークンは60日で失効し、定期リフレッシュで値そのものが書き換わる。
// このため .env / config.yaml ではなくDBへ保存し、システム鍵で暗号化する
// （user_google_accounts のリフレッシュトークンと同じ方式）。
//
// セキュリティ: アクセストークンはそれ単体でInstagram APIを叩ける資格情報のため、
// ログ・例外メッセージ・監査ログに本体を出力することは禁止。

export interface InstagramAccount {
	id: number;
	ig_user_id: string | null;
	username: string | null;
	access_token_encrypted: string;
	access_token_iv: string;
	access_token_tag: string;
	token_expires_at: string | null;
	last_refreshed_at: string | null;
	last_post_id: string | null;
	last_post_timestamp: string | null;
	last_checked_at: string | null;
	created_at: string;
	updated_at: string;
}

/** UIやログに出せる安全なビュー（トークン列を除く）。 */
export interface InstagramAccountSafe {
	ig_user_id: string | null;
	username: string | null;
	token_expires_at: string | null;
	last_refreshed_at: string | null;
	last_post_id: string | null;
	last_post_timestamp: string | null;
	last_checked_at: string | null;
}

/** 単一アカウント運用のため行IDは常に1。 */
const ROW_ID = 1;

export function getInstagramAccount(): InstagramAccount | undefined {
	return getDb()
		.prepare("SELECT * FROM instagram_account WHERE id = ?")
		.get(ROW_ID) as InstagramAccount | undefined;
}

export function getInstagramAccountSafe(): InstagramAccountSafe | undefined {
	const a = getInstagramAccount();
	if (!a) return undefined;
	return {
		ig_user_id: a.ig_user_id,
		username: a.username,
		token_expires_at: a.token_expires_at,
		last_refreshed_at: a.last_refreshed_at,
		last_post_id: a.last_post_id,
		last_post_timestamp: a.last_post_timestamp,
		last_checked_at: a.last_checked_at,
	};
}

export function hasInstagramAccount(): boolean {
	return !!getDb()
		.prepare("SELECT 1 FROM instagram_account WHERE id = ? LIMIT 1")
		.get(ROW_ID);
}

/**
 * 復号済みアクセストークンを取得する。
 * 復号に失敗した場合（暗号化シークレットの変更等）は null を返し、呼び出し側で再連携を促す。
 */
export function getInstagramAccessToken(): string | null {
	const a = getInstagramAccount();
	if (!a) return null;
	try {
		return decryptText(
			a.access_token_encrypted,
			a.access_token_iv,
			a.access_token_tag,
		);
	} catch (err) {
		console.error(
			"[Instagram] アクセストークンの復号に失敗しました（再連携が必要です）:",
			err instanceof Error ? err.message : err,
		);
		return null;
	}
}

/**
 * アクセストークンを保存する（初回連携・長期トークンへの交換・リフレッシュで共用）。
 * 既存行がある場合はトークンと失効予定のみを差し替え、投稿カーソルは維持する。
 */
export function saveInstagramToken(
	token: string,
	expiresAt: Date | null,
): void {
	const enc = encryptText(token);
	getDb()
		.prepare(
			`INSERT INTO instagram_account
         (id, access_token_encrypted, access_token_iv, access_token_tag,
          token_expires_at, last_refreshed_at)
       VALUES (?, ?, ?, ?, ?, datetime('now', 'localtime'))
       ON CONFLICT(id) DO UPDATE SET
         access_token_encrypted = excluded.access_token_encrypted,
         access_token_iv = excluded.access_token_iv,
         access_token_tag = excluded.access_token_tag,
         token_expires_at = excluded.token_expires_at,
         last_refreshed_at = excluded.last_refreshed_at,
         updated_at = datetime('now', 'localtime')`,
		)
		.run(
			ROW_ID,
			enc.encrypted,
			enc.iv,
			enc.authTag,
			expiresAt ? expiresAt.toISOString() : null,
		);
}

/** プロフィール情報（IGユーザーID・ユーザー名）を記録する。 */
export function updateInstagramProfile(
	igUserId: string | null,
	username: string | null,
): void {
	getDb()
		.prepare(
			`UPDATE instagram_account
          SET ig_user_id = COALESCE(?, ig_user_id),
              username = COALESCE(?, username),
              updated_at = datetime('now', 'localtime')
        WHERE id = ?`,
		)
		.run(igUserId, username, ROW_ID);
}

/**
 * 送信済みカーソルを進める。
 * last_post_timestamp が新規投稿判定の基準となるため、Discordへの送信に成功した投稿でのみ呼ぶこと。
 */
export function updateInstagramCursor(
	postId: string,
	postTimestamp: string,
): void {
	getDb()
		.prepare(
			`UPDATE instagram_account
          SET last_post_id = ?, last_post_timestamp = ?,
              updated_at = datetime('now', 'localtime')
        WHERE id = ?`,
		)
		.run(postId, postTimestamp, ROW_ID);
}

/** ポーリング実行時刻を記録する（稼働確認用。新規投稿の有無に関わらず更新する）。 */
export function markInstagramChecked(): void {
	getDb()
		.prepare(
			`UPDATE instagram_account
          SET last_checked_at = datetime('now', 'localtime')
        WHERE id = ?`,
		)
		.run(ROW_ID);
}

/** 連携を解除する（トークンを含む行ごと削除）。 */
export function deleteInstagramAccount(): void {
	getDb().prepare("DELETE FROM instagram_account WHERE id = ?").run(ROW_ID);
}
