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
	destinationStorageID := derefStr(job.DestinationStorageID)
	sourceStorageID := derefStr(job.SourceStorageID)
	if fileID == "" || migrationID == "" || (tempStorageID == "" && destinationStorageID == "") || sourceStorageID == "" {
		return fmt.Errorf("evacuate job is missing migration routing fields")
	}
	if sourceStorageID != config.AppConfig.StorageId {
		return fmt.Errorf("evacuate source storage does not match this worker")
	}

	directToPermanentS3 := destinationStorageID != ""
	uploadStorageID := tempStorageID
	if directToPermanentS3 {
		uploadStorageID = destinationStorageID
	}
	uploadStorage, err := models.StorageModel.FindByID(ctx, uploadStorageID)
	if err != nil || uploadStorage.Type != enums.StorageTypeS3 || !uploadStorage.IsOnline() {
		return fmt.Errorf("S3 upload storage %s unavailable", uploadStorageID)
	}

	slug := derefStr(job.Slug)
	if slug == "" {
		slug = fileID
	}
	destinationLabel := "S3 temp"
	if directToPermanentS3 {
		destinationLabel = "permanent S3"
	}
	utils.LogMain("📤 [%s] START EVACUATE (%s → %s)", slug, sourceStorageID, destinationLabel)
	startStep(ctx, job.ID, "scan")

	workDir := transferWorkDir(slug, job.ID)
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
		case enums.MediaTypeVideo, enums.MediaTypeAudio, enums.MediaTypeSubtitle:
			fileName := derefStr(media.FileName)
			if fileName == "" || filepath.Base(fileName) != fileName {
				return fmt.Errorf("media %s has invalid fileName", media.ID)
			}
			relPath := filepath.Join(fileID, fileName)
			if media.Path != nil && *media.Path != "" {
				relPath = filepath.FromSlash(*media.Path)
			}
			mimeType := derefStr(media.MimeType)
			if mimeType == "" {
				mimeType = "application/octet-stream"
			}
			assets = append(assets, evacuationAsset{
				media: media, localPath: filepath.Join(config.AppConfig.StoragePath, relPath),
				fileName: fileName, sourcePath: relPath, mimeType: mimeType,
			})
		case enums.MediaTypeThumbnail:
			sourceDir := filepath.Join(config.AppConfig.StoragePath, fileID, "sprite")
			if directToPermanentS3 {
				assets = append(assets, evacuationAsset{
					media: media, localPath: sourceDir, fileName: enums.SpriteVTTName,
					sourcePath: filepath.Join(fileID, "sprite"), mimeType: "text/vtt",
				})
			} else {
				zipPath := filepath.Join(workDir, media.ID+"-sprite.zip")
				if err := zipDir(ctx, sourceDir, zipPath); err != nil {
					return fmt.Errorf("archive sprite for media %s: %w", media.ID, err)
				}
				assets = append(assets, evacuationAsset{
					media: media, localPath: zipPath, fileName: enums.SpriteZipName,
					sourcePath: filepath.Join(fileID, "sprite"), mimeType: "application/zip", temporary: true,
				})
			}
		default:
			return fmt.Errorf("media %s type %s is not supported by transfer migration", media.ID, media.Type)
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("scan source media: %w", err)
	}
	if len(assets) == 0 {
		if directToPermanentS3 {
			cutOverCount, _ := models.MediaModel.CountDocuments(ctx, bson.M{
				"_id": bson.M{"$in": job.SourceMediaIDs}, "storageId": sourceStorageID,
				"deletedAt": bson.M{"$exists": true, "$ne": nil},
			})
			if cutOverCount == int64(len(job.SourceMediaIDs)) {
				if cacheInvalidationEnabled {
					if err := invalidateMigrationCache(ctx, fileID, slug, migrationID); err != nil {
						return err
					}
				}
				utils.LogMain("✅ [%s] Direct migration already cut over", slug)
				success = true
				return nil
			}
		}
		return fmt.Errorf("no active source media found")
	}
	completeStep(ctx, job.ID, "scan")

	startStep(ctx, job.ID, "upload")
	for _, asset := range assets {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		info, err := os.Stat(asset.localPath)
		if err != nil {
			return fmt.Errorf("source asset missing %s: %w", asset.sourcePath, err)
		}
		if directToPermanentS3 && asset.media.Type == enums.MediaTypeThumbnail {
			if !info.IsDir() {
				return fmt.Errorf("sprite source is not a directory: %s", asset.sourcePath)
			}
			if err := uploadDirectoryToS3(ctx, uploadStorage, asset.localPath, filepath.ToSlash(filepath.Join(fileID, "sprite")), slug); err != nil {
				return fmt.Errorf("upload sprite: %w", err)
			}
		} else if info.IsDir() {
			return fmt.Errorf("source asset is a directory: %s", asset.sourcePath)
		}

		if directToPermanentS3 {
			if asset.media.Type != enums.MediaTypeThumbnail {
				objectKey := filepath.ToSlash(filepath.Join(fileID, asset.fileName))
				if err := uploader.VerifyS3Object(ctx, uploadStorage, objectKey, info.Size()); err != nil {
					utils.LogMain("📤 [%s] Uploading %s...", slug, asset.fileName)
					if err := uploader.UploadToS3(ctx, uploadStorage, asset.localPath, objectKey, pctLogger64(slug, asset.fileName)); err != nil {
						return fmt.Errorf("upload %s: %w", asset.fileName, err)
					}
					if err := uploader.VerifyS3Object(ctx, uploadStorage, objectKey, info.Size()); err != nil {
						return fmt.Errorf("verify %s: %w", asset.fileName, err)
					}
				}
			}
			resolution := derefStr(asset.media.Resolution)
			if err := cutoverDirectMigrationMedia(ctx, asset.media.ID, sourceStorageID, destinationStorageID, fileID, resolution, asset.media.Type); err != nil {
				return fmt.Errorf("cut over %s: %w", asset.media.ID, err)
			}
			utils.LogMain("✅ [%s] Media moved directly: %s", slug, asset.fileName)
			continue
		}

		objectKey := fmt.Sprintf("migration/%s/%s", fileID, asset.fileName)
		if err := uploader.VerifyS3Object(ctx, uploadStorage, objectKey, info.Size()); err != nil {
			utils.LogMain("📤 [%s] Uploading %s...", slug, asset.fileName)
			if err := uploader.UploadToS3(ctx, uploadStorage, asset.localPath, objectKey, pctLogger64(slug, asset.fileName)); err != nil {
				return fmt.Errorf("upload %s: %w", asset.fileName, err)
			}
			if err := uploader.VerifyS3Object(ctx, uploadStorage, objectKey, info.Size()); err != nil {
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
					"fileId": fileID, "storageId": uploadStorageID, "fileName": asset.fileName,
					"status": enums.IngestStatusCompleted, "size": info.Size(), "mimeType": asset.mimeType,
					"path": objectKey, "sourceType": enums.IngestSourceTypeMigration,
					"migrationState": enums.IngestMigrationStateStaged,
					"sourceMediaId":  asset.media.ID, "sourceStorageId": sourceStorageID,
					"sourcePath": asset.sourcePath, "updatedAt": now,
					"mediaType": asset.media.Type, "resolution": asset.media.Resolution,
					"mediaMetadata": asset.media.Metadata,
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
	if directToPermanentS3 && cacheInvalidationEnabled {
		if err := invalidateMigrationCache(ctx, fileID, slug, migrationID); err != nil {
			return err
		}
	}
	completeStep(ctx, job.ID, "ingest")
	if directToPermanentS3 {
		utils.LogMain("✅ [%s] EVACUATE COMPLETE (%d asset(s))", slug, len(assets))
	} else {
		utils.LogMain("✅ [%s] EVACUATE STAGED (%d asset(s))", slug, len(assets))
	}
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
	if len(job.SourceMediaIDs) == 0 {
		return fmt.Errorf("cleanup job has no source media reference")
	}
	startStep(ctx, job.ID, "verify")
	refs, err := models.MediaModel.CountDocuments(ctx, bson.M{
		"_id":       bson.M{"$in": job.SourceMediaIDs},
		"storageId": sourceStorageID,
		"deletedAt": bson.M{"$exists": false},
	})
	if err != nil {
		return fmt.Errorf("verify source references: %w", err)
	}
	if refs > 0 {
		return fmt.Errorf("source still has %d active media reference(s): %w", refs, queue.ErrJobRequeue)
	}
	completeStep(ctx, job.ID, "verify")

	startStep(ctx, job.ID, "cleanup")
	if err := finalizeMigrationIngests(ctx, fileID, migrationID, sourceStorageID); err != nil {
		return err
	}
	completeStep(ctx, job.ID, "cleanup")
	return nil
}

// finalizeMigrationIngests hands Temp cleanup to vdohide-service only after
// the media cutover transaction has committed. The service owns version-aware
// S3 deletion and keeps the soft-deleted ingest as its retry pointer.
func finalizeMigrationIngests(ctx context.Context, fileID, migrationID, sourceStorageID string) error {
	now := time.Now()
	if _, err := models.IngestModel.UpdateMany(ctx, bson.M{
		"fileId":          fileID,
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
	return nil
}
