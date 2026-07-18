# Worker Transfer

Queue-based transfer worker สำหรับ [VdoHide](https://vdohide.xyz) — ดึงไฟล์ผลลัพธ์ (original + transcoded + sprite) จาก S3 temp ลง **local storage ของเครื่องตัวเอง** สร้าง media records แล้วเคลียร์ ingest ที่ใช้แล้ว

> แทนที่ `server-transfer` เดิมที่ scan หาไฟล์เอง — ตัวนี้รับงานจากคิวอย่างเดียว และรับเฉพาะงานที่ enqueuer assign มาให้ storage ของตัวเอง (`targetStorageId`)

## Features

- **Queue-based per storage** — claim จาก `video_process` เฉพาะงานที่ `targetStorageId` = `STORAGE_ID` ของเครื่อง (enqueuer เป็นคน balance ว่าไฟล์ไหนลง storage ไหน)
- **ingest.path เป็น source of truth** — ไม่ประกอบ S3 key เอง อ่านจาก ingest doc เท่านั้น (download worker เขียน key แบบมีวันที่ `{date}/{fileId}_file_original.mp4`)
- **Partial transfer** — asset ไหนมี ingest ค้างก็ย้ายอันนั้น (original มาก่อน, resolution อื่น/sprite ตามมาทีหลังได้)
- **Auto Retry + Backoff** — fail → กลับเป็น pending ใน doc เดิม (1m, 2m) ครบ 3 ครั้ง → failed ถาวร (ไฟล์ไม่ถูกแตะ — ยังเล่นจาก S3 temp ได้)
- **Instant Cancel** — admin เซ็ต `status: cancelled` → watcher (5s) จุดระเบิด context → S3 download/unzip หยุดทันที + เก็บกวาด temp
- **Storage gate** — storage ตัวเองถูกปิด/เต็ม/ออฟไลน์ใน DB → หยุดหยิบงาน (ไม่ claim แล้วคืนวนลูป)
- **Graceful Shutdown** — SIGTERM → คืนงานเข้าคิว (Release) + mark worker offline
- **Heartbeat** — รายงานเข้า `workers` ทุก 1 นาที พร้อม `storageId` (enqueuer ใช้จับคู่ slot ↔ storage)
- **Step-only DB writes** — progress % ออก log เท่านั้น (throttle 10%) DB เขียนขอบ step: download 25 → extract 50 → install 75 → media 100
- **Log per job** — จบงาน → อัพ `logs/process/<slug>.log` ขึ้น S3 ที่ `logs/transfer/` แล้วลบ local
- **Clone propagation** — media/status กระจายไปไฟล์ที่ `clonedFrom` อัตโนมัติ + purge Cloudflare playlist cache

## Requirements

- **MongoDB** (vdohide platform database — replica set)
- **vdohide-service** รันอยู่ (enqueuer `getTransferPending` เติมคิว + reaper)
- เครื่องต้องมี record ใน `storages` (type `local`) — `STORAGE_ID` ชี้ไปที่ record นั้น
- ไม่ต้องมี ffmpeg — worker นี้แค่ย้ายไฟล์ + แตก zip

---

## Installation (Linux Server)

### One-line install

```bash
curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-transfer/main/install.sh | sudo -E bash -s -- \
    --database-url "mongodb+srv://user:pass@cluster.mongodb.net/platform" \
    --storage-id "your-storage-uuid" \
    --storage-path "/home/files" \
    -n 1
```

### Options

| Option | Default | คำอธิบาย |
|---|---|---|
| `-n, -w, --count` | `1` | จำนวน worker instances |
| `--database-url` | `""` | MongoDB connection string (`DATABASE_URL`) |
| `--storage-id` | — | **REQUIRED** — local storage ที่เครื่องนี้ดูแล |
| `--storage-path` | `/home/files` | Local storage path |
| `--uninstall` | — | ถอนการติดตั้ง |

### After install

```bash
# ดู logs
journalctl -u "worker-transfer@*" -f

# Restart workers
for i in $(seq 1 2); do systemctl restart worker-transfer@$i; done

# Stop workers (SIGTERM → คืนงานเข้าคิวก่อนปิด)
for i in $(seq 1 2); do systemctl stop worker-transfer@$i; done
```

---

## Configuration (.env)

```env
# Required
DATABASE_URL=mongodb+srv://user:pass@cluster.mongodb.net/platform
STORAGE_ID=your-storage-uuid
STORAGE_PATH=/home/files

# Optional — Worker ID (default: transfer_hostname@1)
WORKER_ID=transfer_myhost@1

# Optional — log file (default: logs/worker-transfer.log)
LOG_PATH=logs/worker-transfer.log
```

> ⚠ `STORAGE_ID` + `STORAGE_PATH` บังคับ — ไม่ตั้ง binary จะ exit ทันที
> (Claim กรองงานด้วย `targetStorageId` = `STORAGE_ID`)

---

## Development

```bash
git clone https://github.com/zergolf1994/worker-transfer.git
cd worker-transfer

# สร้าง .env แล้วใส่ DATABASE_URL + STORAGE_ID + STORAGE_PATH

# Run
go run ./cmd

# Build (Windows exe + copy .env → .build/)
build.bat
```

## Release

```bash
git tag v1.0.0
git push origin v1.0.0
```

GitHub Actions build + release อัตโนมัติ: `linux` (amd64), `linux-arm64`

---

## Architecture

```
vdohide-service (Node)                     worker-transfer (Go, ตัวนี้)
├── enqueuer:transfer (ทุก 20s)            ├── heartbeat (ทุก 1m → workers + storageId)
│   ไฟล์ที่มี ingest processed ค้าง         ├── job loop
│   → balance เลือก storage ปลายทาง        │   gate: storage ตัวเองพร้อมไหม
│   → insert pending + targetStorageId     │   Claim (targetStorageId = ของเรา)
└── reaper                                 │   → download (จาก ingest.path)
    processing ค้าง (claimedAt เก่า)        │   → extract sprite → install → media
    → คืน pending                           │   → soft-delete ingests → Complete
                                           └── cancel watcher (ทุก 5s ระหว่างมีงาน)
```

## Job Lifecycle

```
pending ──claim──▶ processing ──สำเร็จ──▶ completed
   ▲                   │
   │◀── retry (backoff 1m/2m, ≤3) ── fail
   │◀── Release (shutdown / storage ไม่พร้อม / reaper)
   │
   └── admin เซ็ต cancelled ──▶ หยุดทุก I/O ใน ≤5s + cleanup
       fail ครั้งที่ 3 ──▶ failed ถาวร (ไฟล์ไม่ถูกแตะ — admin สั่ง retry เอง)
```

## Transfer Flow (1 job = 1 file)

1. **download (25%)** — ทุก asset ที่มี ingest `processed` ค้าง: อ่าน key จาก `ingest.path` → โหลดจาก S3 temp ลง `transfer/<slug>/`
2. **extract (50%)** — แตก `sprite.zip` (ถ้ามี)
3. **install (75%)** — ย้ายเข้า `{STORAGE_PATH}/{fileId}/` (+ `sprite/`)
4. **media (100%)** — สร้าง media records + clone propagation + purge CF cache → soft-delete ingests ที่ใช้แล้ว → ถ้า original ลงแล้ว file → `ready`

> soft-delete ingest ทำ **หลัง** media สำเร็จ (ต่างจากตัวเก่าที่ลบหลัง download) — install fail แล้ว retry ได้โดย ingest ยังอยู่ครบ

## Collections Used

| Collection | การใช้งาน |
|---|---|
| `video_process` | คิวงาน — claim (กรอง `targetStorageId`)/settle/timeline |
| `workers` | heartbeat + `storageId`, สถานะ, system info |
| `files` | อ่าน source, update status → `ready` |
| `ingests` | **driver ของงาน** — asset ไหนมี ingest `processed` ค้างก็ย้ายอันนั้น, key จาก `path` |
| `storages` | local storage ของตัวเอง (gate), S3 temp ต้นทาง (per-ingest) |
| `medias` | สร้าง media record + clone |
| `settings` | `transfer_config.enabled` (kill switch), `domain_playlist`, `cf_zone_id`, `cf_api_token` |

> ⚠ **Index ทั้งหมดเป็นของฝั่ง vdohide-service (mongoose)** — repo นี้ไม่สร้าง index เอง
> ⚠ ค่า enum ทุกตัวใน `internal/core/enums/` ต้อง match กับ `vdohide-service/src/core/enums/` ไฟล์ต่อไฟล์
