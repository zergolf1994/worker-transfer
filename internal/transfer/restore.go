package transfer

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"path/filepath"

	"worker-transfer/internal/config"
	"worker-transfer/internal/core/enums"
	"worker-transfer/internal/core/utils"
	"worker-transfer/internal/db/models"
	"worker-transfer/internal/downloader"
	"worker-transfer/internal/queue"

	"go.mongodb.org/mongo-driver/bson"
)

// runRestore moves exactly one permanent S3 media asset to this worker's
// Local storage. The database cutover clones references first and only
// soft-deletes the old S3 media, leaving version-aware object deletion to the
// service cleanup job.
func runRestore(ctx context.Context, job *models.VideoProcess) error {
	fileID := derefStr(job.FileID)
	sourceStorageID := derefStr(job.SourceStorageID)
	targetStorageID := derefStr(job.TargetStorageID)
	if fileID == "" || sourceStorageID == "" || targetStorageID == "" || len(job.SourceMediaIDs) != 1 {
		return fmt.Errorf("restore job must contain one media and complete S3-to-Local routing")
	}
	if targetStorageID != config.AppConfig.StorageId {
		return fmt.Errorf("restore target storage does not match this worker")
	}
	if reason := installStorageBlockReason(ctx); reason != "" {
		return fmt.Errorf("%s: %w", reason, queue.ErrJobRequeue)
	}
	sourceStorage, err := models.StorageModel.FindByID(ctx, sourceStorageID)
	if err != nil || sourceStorage.Type != enums.StorageTypeS3 || sourceStorage.S3 == nil || sourceStorage.Status != enums.StorageStatusOnline {
		return fmt.Errorf("S3 source storage %s unavailable: %w", sourceStorageID, queue.ErrJobRequeue)
	}

	mediaID := job.SourceMediaIDs[0]
	media, err := models.MediaModel.FindOne(ctx, bson.M{
		"_id": mediaID, "fileId": fileID, "storageId": sourceStorageID,
		"clonedFrom": bson.M{"$exists": false}, "deletedAt": bson.M{"$exists": false},
	})
	if err != nil {
		// A crash after the transaction committed but before the job completed is
		// safe to resume: an active replacement on the assigned target proves the
		// cutover already happened.
		oldMedia, oldErr := models.MediaModel.FindOne(ctx, bson.M{
			"_id": mediaID, "fileId": fileID, "storageId": sourceStorageID,
			"deletedAt": bson.M{"$exists": true, "$ne": nil},
		})
		if oldErr != nil {
			return fmt.Errorf("active source media %s not found", mediaID)
		}
		filter := bson.M{
			"fileId": fileID, "storageId": targetStorageID, "type": oldMedia.Type,
			"deletedAt": bson.M{"$exists": false},
		}
		if oldMedia.Resolution != nil {
			filter["resolution"] = *oldMedia.Resolution
		}
		if oldMedia.FileName != nil {
			filter["fileName"] = *oldMedia.FileName
		}
		if count, _ := models.MediaModel.CountDocuments(ctx, filter); count > 0 {
			return nil
		}
		return fmt.Errorf("active source media %s not found", mediaID)
	}

	slug := derefStr(job.Slug)
	if slug == "" {
		slug = fileID
	}
	utils.LogMain("📥 [%s] START RESTORE (S3 %s → Local %s, media=%s)", slug, sourceStorageID, targetStorageID, mediaID)
	workDir := transferWorkDir(slug, job.ID)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("create restore workdir: %w", err)
	}
	var success bool
	defer func() {
		if success || goerrors.Is(context.Cause(ctx), queue.ErrJobCancelled) {
			_ = os.RemoveAll(workDir)
		}
	}()

	startStep(ctx, job.ID, "download")
	fileName := derefStr(media.FileName)
	resolution := derefStr(media.Resolution)
	if media.Type == enums.MediaTypeThumbnail {
		prefix := derefStr(media.Path)
		if prefix == "" {
			prefix = filepath.ToSlash(filepath.Join(fileID, "sprite"))
		}
		spriteDir := filepath.Join(workDir, "sprite")
		count, err := downloader.DownloadPrefixFromS3(ctx, sourceStorage, prefix, spriteDir)
		if err != nil {
			return fmt.Errorf("download sprite prefix: %w", err)
		}
		utils.LogMain("📥 [%s] Downloaded %d sprite object(s)", slug, count)
		completeStep(ctx, job.ID, "download")
		startStep(ctx, job.ID, "install")
		if err := installDir(config.AppConfig.StoragePath, fileID, "sprite", spriteDir); err != nil {
			return fmt.Errorf("install sprite: %w", err)
		}
	} else {
		if fileName == "" || filepath.Base(fileName) != fileName {
			return fmt.Errorf("media %s has invalid fileName", mediaID)
		}
		objectKey := derefStr(media.Path)
		if objectKey == "" {
			objectKey = filepath.ToSlash(filepath.Join(fileID, fileName))
		}
		tempPath := filepath.Join(workDir, fileName)
		if err := downloader.DownloadFromS3(ctx, sourceStorage, objectKey, tempPath, pctLogger64(slug, fileName)); err != nil {
			return fmt.Errorf("download %s: %w", fileName, err)
		}
		completeStep(ctx, job.ID, "download")
		startStep(ctx, job.ID, "install")
		if err := installFile(config.AppConfig.StoragePath, fileID, fileName, tempPath); err != nil {
			return fmt.Errorf("install %s: %w", fileName, err)
		}
	}
	completeStep(ctx, job.ID, "install")

	startStep(ctx, job.ID, "media")
	if err := cutoverDirectMigrationMedia(ctx, mediaID, sourceStorageID, targetStorageID, fileID, resolution, media.Type); err != nil {
		return fmt.Errorf("cut over media: %w", err)
	}
	completeStep(ctx, job.ID, "media")
	utils.LogMain("✅ [%s] RESTORE COMPLETE (%s)", slug, mediaID)
	success = true
	return nil
}
