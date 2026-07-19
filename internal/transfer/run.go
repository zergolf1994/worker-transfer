package transfer

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"worker-transfer/internal/cache"
	"worker-transfer/internal/config"
	"worker-transfer/internal/core/enums"
	"worker-transfer/internal/core/utils"
	"worker-transfer/internal/db/models"
	"worker-transfer/internal/downloader"
	"worker-transfer/internal/queue"

	"go.mongodb.org/mongo-driver/bson"
)

// ─── Transfer pipeline ───────────────────────────────────────
//
// One job = one file: pull every asset that has an open "processed"
// ingest (original + transcoded mp4s + sprite.zip) from S3 temp down to
// this machine's local storage, create media records, then soft-delete
// the consumed ingests. Which assets move is driven ENTIRELY by ingest
// docs — no HeadObject probing, no reconstructed S3 keys (ingest.path
// is the source of truth; the download worker writes dated keys).
//
// Steps (DB writes at boundaries only): download 25 → extract 50 →
// install 75 → media 100.

// LocalStorageBlockReason is the pre-claim gate for queue.ClaimGate —
// empty when this worker's storage can accept a job right now.
func LocalStorageBlockReason(ctx context.Context) string {
	return localStorageBlockReason(ctx)
}

// consumedAsset — one downloaded (or stale) asset and the ingest doc it came from.
type consumedAsset struct {
	ingest     *models.Ingest
	resolution string // "" for sprite.zip
	fileName   string
	downloaded bool
}

// Run executes one claimed transfer job, then finalizes the per-process log.
func Run(jobCtx context.Context, job *models.VideoProcess) error {
	err := run(jobCtx, job)
	finalizeProcessLog(jobCtx, job, err)
	return err
}

func run(ctx context.Context, job *models.VideoProcess) error {
	fileID := derefStr(job.FileID)
	slug := derefStr(job.Slug)
	if fileID == "" {
		return fmt.Errorf("job has no fileId")
	}

	storagePath := config.AppConfig.StoragePath
	storageID := config.AppConfig.StorageId

	// storage ตัวเองใช้ไม่ได้ชั่วคราว (ปิด/เต็ม) — ไม่ใช่ความผิดของงาน คืนคิว
	if reason := localStorageBlockReason(ctx); reason != "" {
		return fmt.Errorf("%s: %w", reason, queue.ErrJobRequeue)
	}

	procLogger := utils.NewProcessLogger(slug)
	defer procLogger.Close()

	exePath, _ := os.Executable()
	baseDir := filepath.Dir(exePath)
	if strings.Contains(exePath, "go-build") {
		baseDir, _ = os.Getwd()
	}
	workDir := filepath.Join(baseDir, "transfer", slug)
	os.MkdirAll(workDir, 0755)

	var success bool
	defer func() {
		cancelled := goerrors.Is(context.Cause(ctx), queue.ErrJobCancelled)
		if success || cancelled {
			os.RemoveAll(workDir)
			utils.LogMain("🧹 [%s] Cleaned up temp dir", slug)
		} else {
			utils.LogMain("⚠️  [%s] Keeping temp dir for retry: %s", slug, workDir)
		}
	}()

	utils.LogMain("📦 [%s] START TRANSFER (S3 → %s)", slug, storageID)

	file, err := models.FileModel.FindByID(ctx, fileID)
	if err != nil {
		return fmt.Errorf("file not found: %w", err)
	}
	duration := fileDuration(file)

	// ─── STEP 1: DOWNLOAD assets with open ingests ────────────
	startStep(ctx, job.ID, "download")

	// cache S3 storages by id — assets may live on different temp buckets
	storageCache := map[string]*models.Storage{}
	getStorage := func(id string) (*models.Storage, error) {
		if s, ok := storageCache[id]; ok {
			return s, nil
		}
		s, err := models.StorageModel.FindByID(ctx, id)
		if err != nil || s.Type != enums.StorageTypeS3 || !s.IsOnline() {
			return nil, fmt.Errorf("S3 storage %s unavailable", id)
		}
		storageCache[id] = s
		return s, nil
	}

	assets := make([]consumedAsset, 0, len(enums.AllResolutions)+1)

	for _, res := range enums.AllResolutions {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fileName := enums.ResolutionToFileName[res]
		ingest := pendingIngestFor(ctx, fileID, fileName)
		if ingest == nil {
			continue // asset not produced yet (HLS still working) — partial transfer is fine
		}
		if hasVideoMedia(ctx, fileID, res) {
			// media already exists (installed elsewhere) — stale ingest, clean up later
			utils.LogMain("⏭️  [%s] %s media already exists — stale ingest", slug, res)
			assets = append(assets, consumedAsset{ingest: ingest, resolution: res, fileName: fileName})
			continue
		}

		s3Storage, err := getStorage(derefStr(ingest.StorageID))
		if err != nil {
			return fmt.Errorf("download %s: %w", fileName, err)
		}
		key := ingestObjectKey(ingest, fileID)
		dest := filepath.Join(workDir, fileName)
		utils.LogMain("📥 [%s] Downloading %s (key=%s)...", slug, fileName, key)
		if err := downloader.DownloadFromS3(ctx, s3Storage, key, dest, pctLogger64(slug, fileName)); err != nil {
			return fmt.Errorf("download %s: %w", fileName, err)
		}
		assets = append(assets, consumedAsset{ingest: ingest, resolution: res, fileName: fileName, downloaded: true})
	}

	// sprite.zip (thumbnail track)
	spriteZipPath := filepath.Join(workDir, enums.SpriteZipName)
	hasSpriteZip := false
	if spriteIngest := pendingIngestFor(ctx, fileID, enums.SpriteZipName); spriteIngest != nil {
		if hasThumbnailMedia(ctx, fileID) {
			utils.LogMain("⏭️  [%s] thumbnail media already exists — stale sprite ingest", slug)
			assets = append(assets, consumedAsset{ingest: spriteIngest, fileName: enums.SpriteZipName})
		} else {
			s3Storage, err := getStorage(derefStr(spriteIngest.StorageID))
			if err != nil {
				return fmt.Errorf("download sprite.zip: %w", err)
			}
			key := ingestObjectKey(spriteIngest, fileID)
			utils.LogMain("📥 [%s] Downloading %s (key=%s)...", slug, enums.SpriteZipName, key)
			if err := downloader.DownloadFromS3(ctx, s3Storage, key, spriteZipPath, nil); err != nil {
				return fmt.Errorf("download sprite.zip: %w", err)
			}
			hasSpriteZip = true
			assets = append(assets, consumedAsset{ingest: spriteIngest, fileName: enums.SpriteZipName, downloaded: true})
		}
	}

	if len(assets) == 0 {
		// enqueuer queued a file with nothing pending — treat as done, not failed
		utils.LogMain("⏭️  [%s] Nothing to transfer (no open ingests) — completing", slug)
		success = true
		return nil
	}
	completeStep(ctx, job.ID, "download")

	// ─── STEP 2: EXTRACT sprite.zip ───────────────────────────
	startStep(ctx, job.ID, "extract")
	spriteDir := filepath.Join(workDir, "sprite")
	if hasSpriteZip {
		utils.LogMain("📦 [%s] Extracting sprite.zip...", slug)
		if err := unzip(ctx, spriteZipPath, spriteDir); err != nil {
			return fmt.Errorf("extract sprite.zip: %w", err)
		}
	}
	completeStep(ctx, job.ID, "extract")

	// ─── STEP 3: INSTALL to local storage path ────────────────
	startStep(ctx, job.ID, "install")
	installedRes := make([]string, 0, len(assets))

	for _, a := range assets {
		if !a.downloaded || a.resolution == "" {
			continue
		}
		src := filepath.Join(workDir, a.fileName)
		if err := installFile(storagePath, fileID, a.fileName, src); err != nil {
			return fmt.Errorf("install %s: %w", a.fileName, err)
		}
		installedRes = append(installedRes, a.resolution)
		utils.LogMain("📂 [%s] Installed %s → %s/%s/", slug, a.fileName, storagePath, fileID)
	}

	if hasSpriteZip {
		if err := installDir(storagePath, fileID, "sprite", spriteDir); err != nil {
			return fmt.Errorf("install sprite: %w", err)
		}
		utils.LogMain("📂 [%s] Installed sprite/ → %s/%s/sprite/", slug, storagePath, fileID)
	}
	completeStep(ctx, job.ID, "install")

	// ─── STEP 4: MEDIA RECORDS + ingest cleanup ───────────────
	startStep(ctx, job.ID, "media")
	now := time.Now()
	mimeType := "video/mp4"
	needCfPurge := false // resolution ใหม่ลง → playlist.m3u8 เปลี่ยน

	for _, res := range installedRes {
		if hasVideoMedia(ctx, fileID, res) {
			continue
		}
		fileName := enums.ResolutionToFileName[res]
		fn := fileName
		resPtr := res
		sid := storageID
		media := models.Media{
			ID: newUUID(), Type: enums.MediaTypeVideo, FileName: &fn, MimeType: &mimeType,
			Resolution: &resPtr, StorageID: &sid, Slug: utils.RandomString(11, false),
			FileID: &fileID,
			Metadata: &models.MediaMetadata{
				Size:     fileSizeOf(filepath.Join(storagePath, fileID, fileName)),
				Duration: duration,
			},
			CreatedAt: now, UpdatedAt: now,
		}
		if _, err := models.MediaModel.Create(ctx, &media); err != nil {
			return fmt.Errorf("create media %s: %w", res, err)
		}
		cloneMediaToClonedFiles(ctx, fileID, media, slug)
		utils.LogMain("✅ [%s] Media record: %s", slug, res)
		if isPurgeResolution(res) {
			needCfPurge = true
		}
	}

	if hasSpriteZip && !hasThumbnailMedia(ctx, fileID) {
		var totalSpriteSize int64
		spriteDest := filepath.Join(storagePath, fileID, "sprite")
		if entries, err := os.ReadDir(spriteDest); err == nil {
			for _, e := range entries {
				if !e.IsDir() {
					if info, err := e.Info(); err == nil {
						totalSpriteSize += info.Size()
					}
				}
			}
		}
		thumbFn := enums.SpriteVTTName
		sid := storageID
		thumbMedia := models.Media{
			ID: newUUID(), Type: enums.MediaTypeThumbnail, FileName: &thumbFn,
			StorageID: &sid, Slug: utils.RandomString(11, false), FileID: &fileID,
			Metadata:  &models.MediaMetadata{Size: totalSpriteSize, Duration: duration},
			CreatedAt: now, UpdatedAt: now,
		}
		if _, err := models.MediaModel.Create(ctx, &thumbMedia); err != nil {
			return fmt.Errorf("create media thumbnail: %w", err)
		}
		cloneMediaToClonedFiles(ctx, fileID, thumbMedia, slug)
		utils.LogMain("✅ [%s] Media record: thumbnail", slug)
	}

	// media records exist → now it's safe to burn the ingests. (Doing this
	// LAST means a failed install retries with ingests intact — unlike the
	// old worker, which deleted them right after download.)
	for _, a := range assets {
		softDeleteIngest(ctx, a.ingest.ID, slug, a.fileName)
	}
	completeStep(ctx, job.ID, "media")

	// ─── Cache invalidation (ครั้งเดียวต่อ job — ไฟล์ + clones) ──
	// Redis: ลบ playlist_master/playlist_json/embed_resolve ทุกครั้งที่มี
	//   media ใหม่ (รวม sprite — embed/feed เปลี่ยน) | ไม่ตั้ง REDIS_URL = no-op
	// Cloudflare: purge playlist.m3u8 เฉพาะตอน resolution ใหม่ลง —
	//   ไม่ได้ผูก domain_bindings.playlist = ข้าม
	if len(installedRes) > 0 || hasSpriteZip {
		slugs := collectSlugs(ctx, fileID, slug)
		cache.Del(ctx, redisKeysFor(slugs)...)
		if needCfPurge {
			purgePlaylistCache(ctx, slug, slugs)
		}
	}

	// original playable from local storage → file is fully ready
	if hasVideoMedia(ctx, fileID, enums.ResolutionOriginal) && file.Status != enums.FileStatusReady {
		updateFields := bson.M{"status": enums.FileStatusReady}
		if duration > 0 {
			updateFields["metadata.duration"] = int64(duration)
		}
		if _, err := models.FileModel.FindByIDAndUpdate(ctx, fileID, bson.M{"$set": updateFields}); err != nil {
			return fmt.Errorf("update file ready: %w", err)
		}
		updateClonedFilesReady(ctx, fileID, slug)
	}

	success = true
	utils.LogMain("✅ [%s] TRANSFER COMPLETE (%d video(s), sprite=%v)", slug, len(installedRes), hasSpriteZip)
	return nil
}

func fileDuration(file *models.File) float64 {
	if file.Metadata != nil && file.Metadata.Duration != nil {
		return *file.Metadata.Duration
	}
	return 0
}

func fileSizeOf(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
