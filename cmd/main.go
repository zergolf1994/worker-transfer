package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"worker-transfer/internal/cache"
	"worker-transfer/internal/config"
	"worker-transfer/internal/core/logger"
	"worker-transfer/internal/core/utils"
	"worker-transfer/internal/db/database"
	"worker-transfer/internal/queue"
	"worker-transfer/internal/transfer"
)

// version ถูกฝังตอน build โดย GitHub Actions: -ldflags="-X main.version=v1.x.x"
var version = "dev"

func main() {
	config.Load()
	workerID := utils.GenerateWorkerID()
	log.Printf("🚀 Starting Worker Transfer %s [Worker: %s]", version, workerID)

	// transfer worker ไร้ความหมายถ้าไม่รู้ว่าตัวเองคือ storage ไหน —
	// Claim กรองด้วย targetStorageId = STORAGE_ID ของเครื่องนี้
	if config.AppConfig.StorageId == "" || config.AppConfig.StoragePath == "" {
		log.Println("❌ STORAGE_ID and STORAGE_PATH are required for a transfer worker")
		time.Sleep(5 * time.Second) // กัน systemd restart-loop รัวๆ
		os.Exit(1)
	}

	// ── Rotating file logger ──────────────────────────────────
	logCloser, err := logger.Init(config.AppConfig.LogPath)
	if err != nil {
		log.Printf("⚠️ File logging disabled: %v", err)
	} else {
		defer logCloser.Close()
		log.Printf("📝 Logging to: %s", config.AppConfig.LogPath)
	}

	// ── Redis (optional — ลบแคช content/player หลังติดตั้ง media) ──
	cache.Init(config.AppConfig.RedisURL)

	// ── MongoDB ───────────────────────────────────────────────
	if err := database.Connect(); err != nil {
		log.Printf("❌ Failed to connect to MongoDB: %v", err)
		time.Sleep(5 * time.Second) // ให้ log ถูก flush / กัน restart-loop รัวๆ
		os.Exit(1)
	}
	defer database.Disconnect()

	// ── Heartbeat ─────────────────────────────────────────────
	// ctx ยกเลิกเมื่อโดน SIGINT/SIGTERM → heartbeat mark ตัวเอง offline ก่อนจบ
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		queue.StartHeartbeat(ctx, workerID)
	}()

	// ── Job loop (blocking จนโดน SIGINT/SIGTERM) ──────────────
	// shutdown ระหว่างทำงาน → loop จะ Release งานคืนคิวให้เอง
	// gate: storage ของเครื่องนี้ถูกปิด/เต็ม/ออฟไลน์ใน DB → หยุดหยิบงาน
	queue.ClaimGate = transfer.LocalStorageBlockReason
	queue.RunLoop(ctx, workerID, transfer.Run)

	log.Println("🛑 Shutting down...")

	// รอ heartbeat ปิดตัว (mark offline) ให้เสร็จก่อน disconnect DB
	select {
	case <-hbDone:
	case <-time.After(10 * time.Second):
		log.Println("⚠️ Heartbeat shutdown timed out")
	}
}
