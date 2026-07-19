package cache

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// ─── Optional Redis invalidation ─────────────────────────────
// worker-transfer ใช้ Redis ทางเดียว: ลบ key แคชของ content-node/player-node
// (playlist_master, playlist_json, embed_resolve) หลังติดตั้ง media ใหม่
// มี REDIS_URL = เปิดใช้ / ไม่มี = ปิด (Del เป็น no-op)
// Redis ล่ม = fail-open: แคชจะหมดอายุเองใน 300 วิ ไม่ใช่เหตุให้งาน fail

var client *redis.Client

// Init connects to Redis from a URL (redis://[:pass@]host:port/db).
// Empty url = disabled.
func Init(url string) {
	if url == "" {
		log.Println("📦 Redis invalidation disabled (no REDIS_URL)")
		return
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		log.Printf("⚠️ REDIS_URL invalid — redis invalidation disabled: %v", err)
		return
	}
	c := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		log.Printf("⚠️ Redis unreachable — redis invalidation disabled: %v", err)
		return
	}
	client = c
	log.Printf("📦 Redis invalidation enabled: %s", opt.Addr)
}

// Del removes keys — no-op when disabled, fail-open on error.
func Del(ctx context.Context, keys ...string) {
	if client == nil || len(keys) == 0 {
		return
	}
	delCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := client.Del(delCtx, keys...).Err(); err != nil {
		log.Printf("⚠️ Redis DEL failed (ignored): %v", err)
		return
	}
	log.Printf("🧹 Redis DEL %d key(s)", len(keys))
}
