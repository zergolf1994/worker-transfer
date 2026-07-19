package config

import (
	"os"

	"github.com/joho/godotenv"
)

// AppConfig holds the application configuration loaded from environment variables.
var AppConfig Config

// Config represents the application configuration.
type Config struct {
	Port     string
	MongoURI string

	StorageId   string
	StoragePath string

	// Redis (optional) — ใช้ลบแคช content-node/player-node หลังติดตั้ง media
	// ไม่ตั้ง = ไม่ใช้ (env: REDIS_URL, รองรับ RADIS_URL)
	RedisURL string

	LogPath string // Path to rotating log file (env: LOG_PATH)
}

// Load reads configuration from environment variables (and .env file).
func Load() {
	// Load .env file if present (ignore error if not found)
	godotenv.Load()

	AppConfig = Config{
		MongoURI:    getEnv("DATABASE_URL", "mongodb://localhost:27017"),
		StorageId:   getEnv("STORAGE_ID", ""),
		StoragePath: getEnv("STORAGE_PATH", "./files"),
		RedisURL:    getEnv("REDIS_URL", getEnv("RADIS_URL", "")),
		LogPath:     getEnv("LOG_PATH", "logs/worker-transfer.log"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
