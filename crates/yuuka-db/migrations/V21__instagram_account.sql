-- V21: Instagram 連携（§3.15）。自分の新規投稿を Discord チャンネルへ転送するための連携状態。
--
-- Instagram API with Instagram Login（graph.instagram.com）でオーナー自身の投稿を定期取得する。
-- オーナー個人の単一アカウント運用のため id = 1 の 1 行のみを保持する（CHECK 制約で担保）。
-- ユーザー単位の連携（§5.5 のデータ分離）は対象外で、設定は config.yaml / .env で与える。
-- Node 版 (`src/services/*`) には無い Rust 新機能のため baseline(V17) 後の前方専用マイグレーション
-- として追加する。
--
-- 長期アクセストークンは有効期間 60 日で、リフレッシュすると**トークンの値自体が変わる**ため
-- .env では運用できない。システム鍵（YUUKA_ENCRYPTION_SECRET）由来の AES-256-GCM で暗号化して
-- ここに保存する（user_google_accounts のリフレッシュトークンと同方式・§6.2）。
--
-- 新規投稿の判定は last_post_id ではなく last_post_timestamp（この時刻より新しい投稿が未送信）で
-- 行う。投稿 ID は順序を持たないため「前回の最新 ID まで遡る」方式は、カーソルの投稿が削除された
-- 場合や取得件数を超える投稿があった場合に位置を見失い、過去投稿の一斉再送や取りこぼしを起こす。
CREATE TABLE IF NOT EXISTS instagram_account (
  id                     INTEGER PRIMARY KEY CHECK (id = 1), -- 単一アカウント運用の番人
  access_token_encrypted TEXT NOT NULL,
  access_token_iv        TEXT NOT NULL,
  access_token_tag       TEXT NOT NULL,
  token_expires_at       TEXT,                     -- 長期トークンの失効予定時刻（RFC3339）
  last_refreshed_at      TEXT,                     -- 最後にリフレッシュに成功した時刻
  last_post_id           TEXT,                     -- 最後に転送した投稿の ID（ログ・重複防止用）
  last_post_timestamp    TEXT,                     -- 転送済みの最新投稿時刻（新規判定のカーソル）
  last_checked_at        TEXT,
  created_at             TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
  updated_at             TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);
