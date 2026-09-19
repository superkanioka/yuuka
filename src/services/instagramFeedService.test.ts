import { describe, expect, it } from "vitest";
import {
	type InstagramMedia,
	newestMedia,
	primaryImageUrl,
	selectNewPosts,
} from "./instagramFeedService.js";

const post = (
	id: string,
	timestamp: string,
	extra: Partial<InstagramMedia> = {},
): InstagramMedia => ({
	id,
	timestamp,
	media_type: "IMAGE",
	media_url: `https://cdn.example/${id}.jpg`,
	permalink: `https://instagram.com/p/${id}`,
	...extra,
});

describe("selectNewPosts", () => {
	const media = [
		post("P3", "2026-09-18T12:00:00+0000"),
		post("P2", "2026-09-17T12:00:00+0000"),
		post("P1", "2026-09-16T12:00:00+0000"),
	];

	it("カーソル未設定（初回）では何も返さない", () => {
		// 既存投稿がまとめて転送されるのを防ぐため、初回はカーソル設定のみ行う
		expect(
			selectNewPosts(media, { last_post_timestamp: null, last_post_id: null }),
		).toEqual([]);
	});

	it("カーソルより新しい投稿だけを古い順で返す", () => {
		const result = selectNewPosts(media, {
			last_post_timestamp: "2026-09-16T12:00:00+0000",
			last_post_id: "P1",
		});
		expect(result.map((m) => m.id)).toEqual(["P2", "P3"]);
	});

	it("新規投稿が無ければ空", () => {
		expect(
			selectNewPosts(media, {
				last_post_timestamp: "2026-09-18T12:00:00+0000",
				last_post_id: "P3",
			}),
		).toEqual([]);
	});

	it("カーソルの投稿が削除されていても新規分だけを返す", () => {
		// ID を辿る方式では位置を見失い過去投稿を再送してしまうケース
		const withoutCursorPost = [
			post("P4", "2026-09-19T12:00:00+0000"),
			...media,
		];
		const result = selectNewPosts(withoutCursorPost, {
			last_post_timestamp: "2026-09-18T18:00:00+0000",
			last_post_id: "DELETED",
		});
		expect(result.map((m) => m.id)).toEqual(["P4"]);
	});

	it("カーソルと同一IDの投稿は境界時刻でも除外する", () => {
		const result = selectNewPosts(media, {
			last_post_timestamp: "2026-09-17T12:00:00+0000",
			last_post_id: "P2",
		});
		expect(result.map((m) => m.id)).toEqual(["P3"]);
	});

	it("id や timestamp が欠けた要素は無視する", () => {
		const broken = [
			post("OK", "2026-09-19T12:00:00+0000"),
			{ timestamp: "2026-09-19T13:00:00+0000" } as InstagramMedia,
			post("BAD", "not-a-date"),
		];
		const result = selectNewPosts(broken, {
			last_post_timestamp: "2026-09-18T00:00:00+0000",
			last_post_id: null,
		});
		expect(result.map((m) => m.id)).toEqual(["OK"]);
	});
});

describe("newestMedia", () => {
	it("取得順に関わらず最新の投稿を返す", () => {
		const shuffled = [
			post("B", "2026-09-17T12:00:00+0000"),
			post("C", "2026-09-19T12:00:00+0000"),
			post("A", "2026-09-16T12:00:00+0000"),
		];
		expect(newestMedia(shuffled)?.id).toBe("C");
	});

	it("空配列では undefined", () => {
		expect(newestMedia([])).toBeUndefined();
	});
});

describe("primaryImageUrl", () => {
	it("IMAGE は media_url を使う", () => {
		expect(primaryImageUrl(post("P", "2026-09-19T12:00:00+0000"))).toBe(
			"https://cdn.example/P.jpg",
		);
	});

	it("VIDEO は media_url(mp4) ではなくサムネイルを使う", () => {
		// mp4 は Discord の embed 画像として描画されないため
		const video = post("V", "2026-09-19T12:00:00+0000", {
			media_type: "VIDEO",
			media_url: "https://cdn.example/V.mp4",
			thumbnail_url: "https://cdn.example/V.jpg",
		});
		expect(primaryImageUrl(video)).toBe("https://cdn.example/V.jpg");
	});

	it("CAROUSEL_ALBUM は先頭の子メディアを使う", () => {
		const carousel = post("C", "2026-09-19T12:00:00+0000", {
			media_type: "CAROUSEL_ALBUM",
			media_url: undefined,
			children: {
				data: [
					{
						id: "c1",
						media_type: "IMAGE",
						media_url: "https://cdn.example/c1.jpg",
					},
					{
						id: "c2",
						media_type: "IMAGE",
						media_url: "https://cdn.example/c2.jpg",
					},
				],
			},
		});
		expect(primaryImageUrl(carousel)).toBe("https://cdn.example/c1.jpg");
	});

	it("カルーセル先頭が動画ならそのサムネイルを使う", () => {
		const carousel = post("C", "2026-09-19T12:00:00+0000", {
			media_type: "CAROUSEL_ALBUM",
			media_url: undefined,
			children: {
				data: [
					{
						id: "c1",
						media_type: "VIDEO",
						media_url: "https://cdn.example/c1.mp4",
						thumbnail_url: "https://cdn.example/c1.jpg",
					},
				],
			},
		});
		expect(primaryImageUrl(carousel)).toBe("https://cdn.example/c1.jpg");
	});

	it("画像URLが無い場合は undefined", () => {
		const bare = post("N", "2026-09-19T12:00:00+0000", {
			media_type: "VIDEO",
			media_url: "https://cdn.example/N.mp4",
			thumbnail_url: undefined,
		});
		expect(primaryImageUrl(bare)).toBeUndefined();
	});
});
