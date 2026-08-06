package transfer

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"worker-transfer/internal/config"
	"worker-transfer/internal/core/enums"
	"worker-transfer/internal/core/utils"
	"worker-transfer/internal/db/models"
	"worker-transfer/internal/downloader"
	"worker-transfer/internal/queue"
	"worker-transfer/internal/uploader"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type evacuationAsset struct {
	media      models.Media
	localPath  string
	fileName   string
	sourcePath string
	mimeType   string
	temporary  bool
}

func runEvacuate(ctx context.Context, job *models.VideoProcess) error {
	if reason := drainStorageBlockReason(ctx); reason != "" {
		return fmt.Errorf("%s: %w", reason, queue.ErrJobRequeue)
	}
	fileID := derefStr(job.FileID)
	migrationID := derefStr(job.MigrationID)
	tempStorageID := derefStr(job.TempStorageID)
	sourceStorageID := derefStr(job.SourceStorageID)
	if fileID == "" || migrationID == "" || tempStorageID == "" || sourceStorageID == "" {
		return fmt.Errorf("evacuate job is missing migration routing fields")
	}
	if sourceStorageID != config.AppConfig.StorageId {
		return fmt.Errorf("evacuate source storage does not match this worker")
	}

	tempStorage, err := models.StorageModel.FindByID(ctx, tempStorageID)
	if err != nil || tempStorage.Type != enums.StorageTypeS3 || !tempStorage.IsOnline() {
		return fmt.Errorf("temp storage %s unavailable", tempStorageID)
	}

	slug := derefStr(job.Slug)
	if slug == "" {
		slug = fileID
	}
	utils.LogMain("📤 [%s] START EVACUATE (%s → S3 temp)", slug, sourceStorageID)
	startStep(ctx, job.ID, "scan")

	workDir := transferWorkDir(slug)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("create evacuation workdir: %w", err)
	}
	var success bool
	defer func() {
		if success || goerrors.Is(context.Cause(ctx), queue.ErrJobCancelled) {
			_ = os.RemoveAll(workDir)
		}
	}()

	cursor, err := models.MediaModel.FindRaw(ctx, bson.M{
		"_id":       bson.M{"$in": job.SourceMediaIDs},
		"fileId":    fileID,
		"storageId": sourceStorageID,
		"deletedAt": bson.M{"$exists": false},
	})
	if err != nil {
		return fmt.Errorf("load source media: %w", err)
	}
	defer cursor.Close(ctx)

	assets := make([]evacuationAsset, 0, len(job.SourceMediaIDs))
	for cursor.Next(ctx) {
		var media models.Media
		if err := cursor.Decode(&media); err != nil {
			return fmt.Errorf("decode source media: %w", err)
		}
		switch media.Type {
		case enums.MediaTypeVideo:
			fileName := derefStr(media.FileName)
			if fileName == "" || filepath.Base(fileName) != fileName {
				return fmt.Errorf("media %s has invalid fileName", media.ID)
			}
			relPath := filepath.Join(fileID, fileName)
			assets = append(assets, evacuationAsset{
				media: media, localPath: filepath.Join(config.AppConfig.StoragePath, relPath),
				fileName: fileName, sourcePath: relPath, mimeType: "video/mp4",
			})
		case enums.MediaTypeThumbnail:
			sourceDir := filepath.Join(config.AppConfig.StoragePath, fileID, "sprite")
			zipPath := filepath.Join(workDir, media.ID+"-sprite.zip")
			if err := zipDir(ctx, sourceDir, zipPath); err != nil {
				return fmt.Errorf("archive sprite for media %s: %w", media.ID, err)
			}
			assets = append(assets, evacuationAsset{
				media: media, localPath: zipPath, fileName: enums.SpriteZipName,
				sourcePath: filepath.Join(fileID, "sprite"), mimeType: "application/zip", temporary: true,
			})
		default:
			return fmt.Errorf("media %s type %s is not supported by transfer migration", media.ID, media.Type)
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("scan source media: %w", err)
	}
	if len(assets) == 0 {
		return fmt.Errorf("no active source media found")
	}
	completeStep(ctx, job.ID, "scan")

	startStep(ctx, job.ID, "upload")
	for _, asset := range assets {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		info, err := os.Stat(asset.localPath)
		if err != nil || info.IsDir() {
			return fmt.Errorf("source asset missing %s: %w", asset.sourcePath, err)
		}
		objectKey := fmt.Sprintf("migration/%s/%s/%s", migrationID, asset.media.ID, asset.fileName)
		if err := uploader.VerifyS3Object(ctx, tempStorage, objectKey, info.Size()); err != nil {
			utils.LogMain("📤 [%s] Uploading %s...", slug, asset.fileName)
			if err := uploader.UploadToS3(ctx, tempStorage, asset.localPath, objectKey, pctLogger64(slug, asset.fileName)); err != nil {
				return fmt.Errorf("upload %s: %w", asset.fileName, err)
			}
			if err := uploader.VerifyS3Object(ctx, tempStorage, objectKey, info.Size()); err != nil {
				return fmt.Errorf("verify %s: %w", asset.fileName, err)
			}
		}

		now := time.Now()
		_, err = models.IngestModel.FindOneAndUpdate(ctx,
			bson.M{
				"migrationId":   migrationID,
				"sourceMediaId": asset.media.ID,
				"deletedAt":     bson.M{"$exists": false},
			},
			bson.M{
				"$set": bson.M{
					"fileId": fileID, "storageId": tempStorageID, "fileName": asset.fileName,
					"status": enums.IngestStatusCompleted, "size": info.Size(), "mimeType": asset.mimeType,
					"path": objectKey, "sourceType": enums.IngestSourceTypeMigration,
					"migrationState": enums.IngestMigrationStateStaged,
					"sourceMediaId":  asset.media.ID, "sourceStorageId": sourceStorageID,
					"sourcePath": asset.sourcePath, "updatedAt": now,
				},
				"$setOnInsert": bson.M{"_id": newUUID(), "createdAt": now},
			},
			options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
		)
		if err != nil {
			return fmt.Errorf("upsert migration ingest %s: %w", asset.media.ID, err)
		}
	}
	completeStep(ctx, job.ID, "upload")
	startStep(ctx, job.ID, "ingest")
	completeStep(ctx, job.ID, "ingest")
	utils.LogMain("✅ [%s] EVACUATE STAGED (%d asset(s))", slug, len(assets))
	success = true
	return nil
}

func runCleanup(ctx context.Context, job *models.VideoProcess) error {
	if reason := drainStorageBlockReason(ctx); reason != "" {
		return fmt.Errorf("%s: %w", reason, queue.ErrJobRequeue)
	}
	fileID := derefStr(job.FileID)
	migrationID := derefStr(job.MigrationID)
	sourceStorageID := derefStr(job.SourceStorageID)
	if fileID == "" || migrationID == "" || sourceStorageID != config.AppConfig.StorageId {
		return fmt.Errorf("cleanup job is missing migration routing fields")
	}
	startStep(ctx, job.ID, "verify")
	refs, err := models.MediaModel.CountDocuments(ctx, bson.M{
		"storageId": sourceStorageID,
		"deletedAt": bson.M{"$exists": false},
		"$or":       []bson.M{{"fileId": fileID}, {"clonedFrom": fileID}},
	})
	if err != nil {
		return fmt.Errorf("verify source references: %w", err)
	}
	if refs > 0 {
		return fmt.Errorf("source still has %d active media reference(s): %w", refs, queue.ErrJobRequeue)
	}
	completeStep(ctx, job.ID, "verify")

	startStep(ctx, job.ID, "cleanup")
	ingestCursor, err := models.IngestModel.FindRaw(ctx, bson.M{
		"migrationId":     migrationID,
		"sourceStorageId": sourceStorageID,
		"sourceType":      enums.IngestSourceTypeMigration,
		"migrationState":  enums.IngestMigrationStateInstalled,
		"deletedAt":       bson.M{"$exists": false},
	})
	if err != nil {
		return fmt.Errorf("load installed migration ingests: %w", err)
	}
	defer ingestCursor.Close(ctx)

	installedIngests := make([]models.Ingest, 0, len(job.SourceMediaIDs))
	for ingestCursor.Next(ctx) {
		var ingest models.Ingest
		if err := ingestCursor.Decode(&ingest); err != nil {
			return fmt.Errorf("decode installed migration ingest: %w", err)
		}
		installedIngests = append(installedIngests, ingest)
	}
	if err := ingestCursor.Err(); err != nil {
		return fmt.Errorf("scan installed migration ingests: %w", err)
	}

	tempStorages := map[string]*models.Storage{}
	for _, ingest := range installedIngests {
		tempStorageID := derefStr(ingest.StorageID)
		objectKey := derefStr(ingest.Path)
		if tempStorageID == "" || objectKey == "" {
			return fmt.Errorf("migration ingest %s has no temp storage or object path", ingest.ID)
		}
		tempStorage, ok := tempStorages[tempStorageID]
		if !ok {
			tempStorage, err = models.StorageModel.FindByID(ctx, tempStorageID)
			if err != nil || tempStorage.Type != enums.StorageTypeS3 {
				return fmt.Errorf("migration temp storage %s unavailable", tempStorageID)
			}
			tempStorages[tempStorageID] = tempStorage
		}
		if err := downloader.DeleteFromS3(ctx, tempStorage, objectKey); err != nil {
			return fmt.Errorf("delete migration temp object %s: %w", objectKey, err)
		}
	}

	now := time.Now()
	if _, err := models.IngestModel.UpdateMany(ctx, bson.M{
		"migrationId":     migrationID,
		"sourceStorageId": sourceStorageID,
		"sourceType":      enums.IngestSourceTypeMigration,
		"migrationState":  enums.IngestMigrationStateInstalled,
		"deletedAt":       bson.M{"$exists": false},
	}, bson.M{"$set": bson.M{
		"migrationState": enums.IngestMigrationStateCleaned,
		"deletedAt":      now,
		"updatedAt":      now,
	}}); err != nil {
		return fmt.Errorf("close migration ingests: %w", err)
	}

	completeStep(ctx, job.ID, "cleanup")
	return nil
}
