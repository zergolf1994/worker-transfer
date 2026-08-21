package transfer

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"worker-transfer/internal/cache"
	"worker-transfer/internal/config"
	"worker-transfer/internal/core/enums"
	"worker-transfer/internal/core/utils"
	"worker-transfer/internal/db/models"
	"worker-transfer/internal/downloader"
	"worker-transfer/internal/queue"
	"worker-transfer/internal/uploader"

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
	mediaType  string
	fileName   string
	downloaded bool
}

// Run executes one claimed transfer job, then finalizes the per-process log.
func Run(jobCtx context.Context, job *models.VideoProcess) error {
	var err error
	switch job.TransferMode {
	case enums.TransferModeEvacuate:
		err = runEvacuate(jobCtx, job)
	case enums.TransferModeRestore:
		err = runRestore(jobCtx, job)
	case enums.TransferModeCleanup:
		err = runCleanup(jobCtx, job)
	default:
		err = run(jobCtx, job)
	}
	finalizeProcessLog(jobCtx, job, err)
	return err
}

func run(ctx context.Context, job *models.VideoProcess) error {
	fileID := derefStr(job.FileID)
	slug := derefStr(job.Slug)
	migrationID := derefStr(job.MigrationID)
	isMigration := migrationID != ""
	sourceStorageID := derefStr(job.SourceStorageID)
	if fileID == "" {
		return fmt.Errorf("job has no fileId")
	}
	if isMigration && sourceStorageID == "" {
		return fmt.Errorf("migration job has no sourceStorageId")
	}

	storagePath := config.AppConfig.StoragePath
	storageID := derefStr(job.DestinationStorageID)
	forceLocalInstall := false
	// destinationStorageId ที่ enqueuer ระบุมามีสิทธิ์เหนือ marker local แบบเก่า
	// เพื่อให้ ingest ที่ fallback ลง Temp สามารถกู้ไป permanent S3 ได้
	if !isMigration && storageID == "" {
		forceLocal, _ := models.IngestModel.CountDocuments(ctx, bson.M{
			"fileId": fileID, "sourceType": enums.IngestSourceTypeProcessed,
			"installTarget": "local", "deletedAt": bson.M{"$exists": false},
		})
		if forceLocal > 0 {
			forceLocalInstall = true
			storageID = config.AppConfig.StorageId
		}
	}
	if storageID == "" {
		storageID = config.AppConfig.StorageId
	}
	targetStorage, err := models.StorageModel.FindByID(ctx, storageID)
	if err != nil {
		return fmt.Errorf("target storage %s not found: %w", storageID, err)
	}
	isS3Target := targetStorage.Type == enums.StorageTypeS3
	if forceLocalInstall && isS3Target {
		return fmt.Errorf("local fallback was assigned to non-local worker: %w", queue.ErrJobRequeue)
	}

	if isS3Target {
		if !targetStorage.IsOnline() || targetStorage.S3 == nil {
			return fmt.Errorf("S3 target storage %s unavailable: %w", storageID, queue.ErrJobRequeue)
		}
	} else {
		// local install ต้องลง storage ของ worker เครื่องนี้เท่านั้น
		if storageID != config.AppConfig.StorageId {
			return fmt.Errorf("local target storage does not match this worker")
		}
		if reason := installStorageBlockReason(ctx); reason != "" {
			return fmt.Errorf("%s: %w", reason, queue.ErrJobRequeue)
		}
	}

	procLogger := utils.NewProcessLogger(slug)
	defer procLogger.Close()

	workDir := transferWorkDir(slug, job.ID)
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

	utils.LogMain("📦 [%s] START TRANSFER (S3 temp → %s)", slug, storageID)

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
		var ingest *models.Ingest
		if isMigration {
			ingest = pendingMigrationIngestFor(ctx, fileID, fileName, migrationID)
		} else {
			ingest = pendingIngestFor(ctx, fileID, fileName)
		}
		if ingest == nil {
			continue // asset not produced yet (HLS still working) — partial transfer is fine
		}
		if !isMigration && hasVideoMedia(ctx, fileID, res) {
			// media already exists (installed elsewhere) — stale ingest, clean up later
			utils.LogMain("⏭️  [%s] %s media already exists — stale ingest", slug, res)
			assets = append(assets, consumedAsset{ingest: ingest, resolution: res, mediaType: enums.MediaTypeVideo, fileName: fileName})
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
		assets = append(assets, consumedAsset{ingest: ingest, resolution: res, mediaType: enums.MediaTypeVideo, fileName: fileName, downloaded: true})
	}

	{
		var trackIngests []*models.Ingest
		var err error
		if isMigration {
			trackIngests, err = pendingMigrationTrackIngests(ctx, fileID, migrationID)
		} else {
			trackIngests, err = pendingTrackIngests(ctx, fileID)
		}
		if err != nil {
			return fmt.Errorf("list track ingests: %w", err)
		}
		for _, ingest := range trackIngests {
			mediaType := derefStr(ingest.MediaType)
			fileName := filepath.Base(ingest.FileName)
			if fileName != ingest.FileName || fileName == "." || fileName == "" {
				return fmt.Errorf("invalid track fileName %q", ingest.FileName)
			}
			if !isMigration && hasTrackMedia(ctx, fileID, mediaType, fileName) {
				assets = append(assets, consumedAsset{ingest: ingest, mediaType: mediaType, fileName: fileName})
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
			assets = append(assets, consumedAsset{ingest: ingest, mediaType: mediaType, fileName: fileName, downloaded: true})
		}
	}

	// sprite.zip (thumbnail track)
	spriteZipPath := filepath.Join(workDir, enums.SpriteZipName)
	hasSpriteZip := false
	var spriteIngest *models.Ingest
	if isMigration {
		spriteIngest = pendingMigrationIngestFor(ctx, fileID, enums.SpriteZipName, migrationID)
	} else {
		spriteIngest = pendingIngestFor(ctx, fileID, enums.SpriteZipName)
	}
	if spriteIngest != nil {
		if !isMigration && hasThumbnailMedia(ctx, fileID) {
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
		// A migration retry may arrive after its DB cutover committed but before
		// the Temp cleanup handoff completed. Cache invalidation stays guarded so
		// it can be restored later without rebuilding this recovery path.
		if isMigration {
			if err := finalizeMigrationIngests(ctx, fileID, migrationID, sourceStorageID); err != nil {
				return fmt.Errorf("finalize migrated ingest: %w", err)
			}
			if cacheInvalidationEnabled {
				if err := invalidateMigrationCache(ctx, fileID, slug, migrationID); err != nil {
					return err
				}
			}
		}
		// enqueuer queued a file with nothing pending — treat as done, not failed
		utils.LogMain("⏭️  [%s] Nothing to transfer (no open ingests) — completing", slug)
		success = true
		return nil
	}
	completeStep(ctx, job.ID, "download")

	// ─── STEP 2: EXTRACT sprite.zip ───────────────────────────
	startStep(ctx, job.ID, "extract")
	spriteDir := filepath.Join(workDir, "sprite")
	var totalSpriteSize int64
	if hasSpriteZip {
		utils.LogMain("📦 [%s] Extracting sprite.zip...", slug)
		if err := unzip(ctx, spriteZipPath, spriteDir); err != nil {
			return fmt.Errorf("extract sprite.zip: %w", err)
		}
		var err error
		totalSpriteSize, err = directoryFilesSize(spriteDir)
		if err != nil {
			return fmt.Errorf("calculate sprite size: %w", err)
		}
	}
	completeStep(ctx, job.ID, "extract")

	// ─── STEP 3: INSTALL to local path or permanent S3 ────────
	startStep(ctx, job.ID, "install")
	installedRes := make([]string, 0, len(assets))

	for _, a := range assets {
		if !a.downloaded || a.mediaType == "" {
			continue
		}
		src := filepath.Join(workDir, a.fileName)
		if isS3Target {
			objectKey := path.Join(fileID, a.fileName)
			info, err := os.Stat(src)
			if err != nil {
				return fmt.Errorf("stat %s: %w", a.fileName, err)
			}
			if err := uploader.VerifyS3Object(ctx, targetStorage, objectKey, info.Size()); err != nil {
				utils.LogMain("📤 [%s] Uploading %s to permanent S3...", slug, a.fileName)
				if err := uploader.UploadToS3(ctx, targetStorage, src, objectKey, pctLogger64(slug, a.fileName)); err != nil {
					return fmt.Errorf("upload %s: %w", a.fileName, err)
				}
				if err := uploader.VerifyS3Object(ctx, targetStorage, objectKey, info.Size()); err != nil {
					return fmt.Errorf("verify %s: %w", a.fileName, err)
				}
			}
			utils.LogMain("☁️  [%s] Installed %s → %s", slug, a.fileName, objectKey)
		} else if err := installFile(storagePath, fileID, a.fileName, src); err != nil {
			return fmt.Errorf("install %s: %w", a.fileName, err)
		} else {
			utils.LogMain("📂 [%s] Installed %s → %s/%s/", slug, a.fileName, storagePath, fileID)
		}
		if a.resolution != "" {
			installedRes = append(installedRes, a.resolution)
		}
	}

	if hasSpriteZip {
		if isS3Target {
			if err := uploadDirectoryToS3(ctx, targetStorage, spriteDir, path.Join(fileID, "sprite"), slug); err != nil {
				return fmt.Errorf("upload sprite: %w", err)
			}
			utils.LogMain("☁️  [%s] Installed sprite/ → %s/sprite/", slug, fileID)
		} else {
			if err := installDir(storagePath, fileID, "sprite", spriteDir); err != nil {
				return fmt.Errorf("install sprite: %w", err)
			}
			utils.LogMain("📂 [%s] Installed sprite/ → %s/%s/sprite/", slug, storagePath, fileID)
		}
	}
	completeStep(ctx, job.ID, "install")

	// ─── STEP 4: MEDIA RECORDS + migration cutover ─────────────
	startStep(ctx, job.ID, "media")
	now := time.Now()
	mimeType := "video/mp4"
	needCfPurge := false

	for _, res := range installedRes {
		if res == "" {
			continue
		}
		if isMigration {
			var ingest *models.Ingest
			for _, asset := range assets {
				if asset.resolution == res {
					ingest = asset.ingest
					break
				}
			}
			if ingest == nil {
				return fmt.Errorf("migration ingest missing for %s", res)
			}
			if err := cutoverMigrationMedia(ctx, ingest, storageID, fileID, res, enums.MediaTypeVideo); err != nil {
				return fmt.Errorf("migrate media %s: %w", res, err)
			}
			utils.LogMain("✅ [%s] Media moved: %s", slug, res)
			continue
		}
		if hasVideoMedia(ctx, fileID, res) {
			continue
		}
		fileName := enums.ResolutionToFileName[res]
		fn := fileName
		resPtr := res
		sid := storageID
		mediaPath := filepath.Join(storagePath, fileID, fileName)
		if isS3Target {
			mediaPath = filepath.Join(workDir, fileName)
		}
		metadata := &models.MediaMetadata{Size: fileSizeOf(mediaPath), Duration: duration}
		for _, asset := range assets {
			if asset.resolution == res && asset.ingest != nil && asset.ingest.MediaMetadata != nil {
				metadata = asset.ingest.MediaMetadata
				break
			}
		}
		media := models.Media{
			ID: newUUID(), Type: enums.MediaTypeVideo, FileName: &fn, MimeType: &mimeType,
			Resolution: &resPtr, StorageID: &sid, Slug: utils.RandomString(11, false),
			FileID:    &fileID,
			Metadata:  metadata,
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

	for _, asset := range assets {
		if !asset.downloaded || (asset.mediaType != enums.MediaTypeAudio && asset.mediaType != enums.MediaTypeSubtitle) {
			continue
		}
		if hasTrackMedia(ctx, fileID, asset.mediaType, asset.fileName) {
			if !isMigration {
				continue
			}
		}
		if isMigration {
			if err := cutoverMigrationMedia(ctx, asset.ingest, storageID, fileID, "", asset.mediaType); err != nil {
				return fmt.Errorf("migrate %s media %s: %w", asset.mediaType, asset.fileName, err)
			}
			utils.LogMain("✅ [%s] Media moved: %s %s", slug, asset.mediaType, asset.fileName)
			continue
		}
		mimeType := "application/octet-stream"
		if asset.ingest.MimeType != nil {
			mimeType = *asset.ingest.MimeType
		}
		fn, sid := asset.fileName, storageID
		metadata := asset.ingest.MediaMetadata
		if metadata == nil {
			metadata = &models.MediaMetadata{Size: asset.ingest.Size, Duration: duration}
		}
		media := models.Media{ID: newUUID(), Type: asset.mediaType, FileName: &fn, MimeType: &mimeType,
			StorageID: &sid, Slug: utils.RandomString(11, false), FileID: &fileID,
			Metadata: metadata, CreatedAt: now, UpdatedAt: now}
		if isS3Target {
			objectPath := path.Join(fileID, asset.fileName)
			media.Path = &objectPath
		}
		if _, err := models.MediaModel.Create(ctx, &media); err != nil {
			return fmt.Errorf("create %s media %s: %w", asset.mediaType, asset.fileName, err)
		}
		cloneMediaToClonedFiles(ctx, fileID, media, slug)
		utils.LogMain("✅ [%s] Media record: %s %s", slug, asset.mediaType, asset.fileName)
	}

	audioCount, _ := models.MediaModel.CountDocuments(ctx, bson.M{"fileId": fileID, "type": enums.MediaTypeAudio, "deletedAt": bson.M{"$exists": false}})
	subtitleCount, _ := models.MediaModel.CountDocuments(ctx, bson.M{"fileId": fileID, "type": enums.MediaTypeSubtitle, "deletedAt": bson.M{"$exists": false}})
	if audioCount > 0 || subtitleCount > 0 {
		layout := "separated"
		audio, subtitles := int(audioCount), int(subtitleCount)
		update := bson.M{"$set": bson.M{"metadata.mediaLayout": layout, "metadata.audioTrackCount": audio, "metadata.subtitleTrackCount": subtitles}}
		_, _ = models.FileModel.UpdateByID(ctx, fileID, update)
		_, _ = models.FileModel.UpdateMany(ctx, bson.M{"clonedFrom": fileID, "metadata.deletedAt": bson.M{"$exists": false}}, update)
	}

	if hasSpriteZip && isMigration {
		if spriteIngest == nil {
			return fmt.Errorf("migration ingest missing for sprite")
		}
		if err := cutoverMigrationMedia(ctx, spriteIngest, storageID, fileID, "", enums.MediaTypeThumbnail); err != nil {
			return fmt.Errorf("migrate thumbnail media: %w", err)
		}
		utils.LogMain("✅ [%s] Media moved: thumbnail", slug)
	} else if hasSpriteZip && !hasThumbnailMedia(ctx, fileID) {
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

	// Regular transfer ingests can be closed now. Migration ingests move to
	// "installed" in the media cutover transaction, then this INSTALL job
	// soft-deletes them for service-owned Temp cleanup before cache invalidation.
	for _, a := range assets {
		if !isMigration {
			softDeleteIngest(ctx, a.ingest.ID, slug, a.fileName)
		}
	}
	if isMigration {
		if err := finalizeMigrationIngests(ctx, fileID, migrationID, sourceStorageID); err != nil {
			return fmt.Errorf("finalize migrated ingest: %w", err)
		}
	}
	completeStep(ctx, job.ID, "media")

	// Cache invalidation is disabled; content-node/CDN TTL controls freshness.
	// Flip cacheInvalidationEnabled to restore both Redis and Cloudflare calls.
	if cacheInvalidationEnabled {
		if isMigration {
			if err := invalidateMigrationCache(ctx, fileID, slug, migrationID); err != nil {
				return err
			}
		} else if len(installedRes) > 0 || hasSpriteZip {
			slugs := collectSlugs(ctx, fileID, slug)
			cache.Del(ctx, redisKeysFor(slugs)...)
			if needCfPurge {
				_ = purgePlaylistCache(ctx, slug, slugs, false)
			}
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

func invalidateMigrationCache(ctx context.Context, fileID, slug, migrationID string) error {
	slugs := collectSlugs(ctx, fileID, slug)
	if len(slugs) == 0 {
		return fmt.Errorf("migration %s has no file slug for cache invalidation", migrationID)
	}
	cache.Del(ctx, redisKeysFor(slugs)...)
	if err := purgePlaylistCache(ctx, slug, slugs, false); err != nil {
		return fmt.Errorf("purge migration %s playlist cache: %w", migrationID, err)
	}
	utils.LogMain("☁️  [%s] Migration cache invalidation completed (%d playlist(s))", slug, len(slugs))
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

func uploadDirectoryToS3(ctx context.Context, storage *models.Storage, root, objectPrefix, slug string) error {
	return filepath.WalkDir(root, func(localPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rel, err := filepath.Rel(root, localPath)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		objectKey := path.Join(objectPrefix, filepath.ToSlash(rel))
		if err := uploader.VerifyS3Object(ctx, storage, objectKey, info.Size()); err == nil {
			return nil
		}
		utils.LogMain("📤 [%s] Uploading %s...", slug, objectKey)
		if err := uploader.UploadToS3(ctx, storage, localPath, objectKey, nil); err != nil {
			return err
		}
		return uploader.VerifyS3Object(ctx, storage, objectKey, info.Size())
	})
}
