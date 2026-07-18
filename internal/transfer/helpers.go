package transfer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"worker-transfer/internal/config"
	"worker-transfer/internal/core/enums"
	"worker-transfer/internal/core/utils"
	"worker-transfer/internal/db/models"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func newUUID() string { return uuid.New().String() }

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ─── Local storage gating ─────────────────────────────────────
// A transfer worker serves exactly one local storage (STORAGE_ID). The
// enqueuer decides WHICH storage a file belongs on (targetStorageId on
// the job) — this worker only checks that its own storage is usable.

const storageCapacityMaxPercent = 90.0

// localStorageBlockReason returns why this worker's STORAGE_ID cannot
// accept jobs right now (empty = ok).
func localStorageBlockReason(ctx context.Context) string {
	storageID := config.AppConfig.StorageId
	if storageID == "" {
		return "storage_not_configured"
	}
	storage, err := models.StorageModel.FindByID(ctx, storageID)
	if err != nil {
		return "local_storage_not_found"
	}
	if !storage.Enable {
		return "local_storage_disabled"
	}
	if storage.Status != enums.StorageStatusOnline {
		return "local_storage_not_online"
	}
	if storage.Capacity != nil && storage.Capacity.Percentage >= storageCapacityMaxPercent {
		return "local_storage_capacity_full"
	}
	return ""
}

// ─── Ingest / media lookups ──────────────────────────────────

// pendingIngestFor returns the open processed ingest for one asset fileName.
// nil = no pending ingest (asset not produced yet, or already transferred).
func pendingIngestFor(ctx context.Context, fileID, fileName string) *models.Ingest {
	ingest, err := models.IngestModel.FindOne(ctx, bson.M{
		"fileId":     fileID,
		"fileName":   fileName,
		"sourceType": enums.IngestSourceTypeProcessed,
		"deletedAt":  bson.M{"$exists": false},
	})
	if err != nil {
		return nil
	}
	return ingest
}

// ingestObjectKey returns the S3 key for an ingest — ALWAYS ingest.path
// when set (the download worker writes dated keys like
// "2026-07-18/{fileId}_file_original.mp4"); legacy fallback is
// "{fileId}/{fileName}".
func ingestObjectKey(ingest *models.Ingest, fileID string) string {
	if p := derefStr(ingest.Path); p != "" {
		return p
	}
	return fmt.Sprintf("%s/%s", fileID, ingest.FileName)
}

// hasVideoMedia checks medias collection globally (any storage) for this resolution.
func hasVideoMedia(ctx context.Context, fileID, resolution string) bool {
	count, _ := models.MediaModel.CountDocuments(ctx, bson.M{
		"fileId":     fileID,
		"type":       enums.MediaTypeVideo,
		"resolution": resolution,
		"deletedAt":  bson.M{"$exists": false},
	})
	return count > 0
}

// hasThumbnailMedia checks medias collection globally (any storage).
func hasThumbnailMedia(ctx context.Context, fileID string) bool {
	count, _ := models.MediaModel.CountDocuments(ctx, bson.M{
		"fileId":    fileID,
		"type":      enums.MediaTypeThumbnail,
		"deletedAt": bson.M{"$exists": false},
	})
	return count > 0
}

// softDeleteIngest marks one consumed ingest as deleted (by _id — exact doc).
func softDeleteIngest(ctx context.Context, ingestID, slug, label string) {
	now := time.Now()
	if _, err := models.IngestModel.FindByIDAndUpdate(ctx, ingestID, bson.M{
		"$set": bson.M{"deletedAt": now},
	}); err != nil {
		log.Printf("⚠️  [%s] soft-delete ingest %s: %v", slug, label, err)
		return
	}
	log.Printf("🗑️  [%s] Soft-deleted ingest: %s", slug, label)
}

// ─── Clone propagation ───────────────────────────────────────

func cloneMediaToClonedFiles(ctx context.Context, sourceFileID string, media models.Media, slug string) {
	cursor, err := models.FileModel.FindRaw(ctx, bson.M{
		"clonedFrom":         sourceFileID,
		"type":               enums.FileTypeVideo,
		"metadata.trashedAt": bson.M{"$exists": false},
		"metadata.deletedAt": bson.M{"$exists": false},
	})
	if err != nil {
		return
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var clonedFile models.File
		if err := cursor.Decode(&clonedFile); err != nil {
			continue
		}

		filter := bson.M{"fileId": clonedFile.ID, "type": media.Type}
		if media.Resolution != nil {
			filter["resolution"] = *media.Resolution
		}
		existCount, _ := models.MediaModel.CountDocuments(ctx, filter)
		if existCount > 0 {
			continue
		}

		now := time.Now()
		clonedMedia := models.Media{
			ID:         newUUID(),
			Type:       media.Type,
			FileName:   media.FileName,
			MimeType:   media.MimeType,
			Resolution: media.Resolution,
			StorageID:  media.StorageID,
			Slug:       utils.RandomString(11, true),
			FileID:     &clonedFile.ID,
			Metadata:   media.Metadata,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		clonedFrom := sourceFileID
		clonedMedia.ClonedFrom = &clonedFrom

		if _, err := models.MediaModel.Create(ctx, &clonedMedia); err != nil {
			log.Printf("⚠️  [%s] Failed to clone media to %s: %v", slug, clonedFile.ID, err)
			continue
		}
		log.Printf("📋 [%s] Cloned media → file %s", slug, clonedFile.ID)
	}
}

func updateClonedFilesReady(ctx context.Context, sourceFileID string, slug string) {
	result, _ := models.FileModel.UpdateMany(ctx, bson.M{
		"clonedFrom":         sourceFileID,
		"type":               enums.FileTypeVideo,
		"status":             enums.FileStatusReadyOriginal,
		"metadata.trashedAt": bson.M{"$exists": false},
		"metadata.deletedAt": bson.M{"$exists": false},
	}, bson.M{"$set": bson.M{
		"status": enums.FileStatusReady,
	}})
	if result != nil && result.ModifiedCount > 0 {
		log.Printf("📋 [%s] Updated %d cloned files → ready", slug, result.ModifiedCount)
	}
}

// ─── S3 temp storage (log upload) ────────────────────────────

// resolveS3TempStorage finds an S3 storage that accepts ["temp", "video"].
func resolveS3TempStorage(ctx context.Context) (*models.Storage, error) {
	filter := bson.M{
		"enable":  true,
		"status":  enums.StorageStatusOnline,
		"type":    enums.StorageTypeS3,
		"accepts": bson.M{"$all": []string{enums.StorageAcceptTemp, enums.StorageAcceptVideo}},
	}
	storage, err := models.StorageModel.FindOne(ctx, filter, options.FindOne().SetSort(bson.M{"capacity.percentage": 1}))
	if err != nil {
		return nil, fmt.Errorf("no S3 temp storage available")
	}
	return storage, nil
}

// ─── Cloudflare playlist purge ───────────────────────────────

func isPurgeResolution(res string) bool {
	switch res {
	case enums.Resolution360, enums.Resolution480, enums.Resolution720, enums.Resolution1080:
		return true
	default:
		return false
	}
}

// purgePlaylistCache purges playlist.m3u8 from Cloudflare for the file and its clones.
func purgePlaylistCache(ctx context.Context, slug, fileID string) {
	domainSetting, err := models.SettingModel.FindOne(ctx, bson.M{"name": enums.SettingDomainPlaylist})
	if err != nil {
		return
	}
	domain := domainSetting.GetString("")
	if domain == "" {
		return
	}
	if !strings.HasPrefix(domain, "http://") && !strings.HasPrefix(domain, "https://") {
		domain = "https://" + domain
	}
	domain = strings.TrimRight(domain, "/")

	zoneSetting, err := models.SettingModel.FindOne(ctx, bson.M{"name": enums.SettingCfZoneID})
	if err != nil {
		return
	}
	tokenSetting, err := models.SettingModel.FindOne(ctx, bson.M{"name": enums.SettingCfApiToken})
	if err != nil {
		return
	}

	cfConfig := utils.CloudflareConfig{
		ZoneID:   zoneSetting.GetString(""),
		APIToken: tokenSetting.GetString(""),
	}
	if cfConfig.ZoneID == "" || cfConfig.APIToken == "" {
		return
	}

	purgeURLs := []string{fmt.Sprintf("%s/%s/playlist.m3u8", domain, slug)}

	cursor, err := models.FileModel.FindRaw(ctx, bson.M{
		"clonedFrom":         fileID,
		"type":               enums.FileTypeVideo,
		"metadata.trashedAt": bson.M{"$exists": false},
		"metadata.deletedAt": bson.M{"$exists": false},
	}, options.Find().SetProjection(bson.M{"slug": 1}))
	if err == nil {
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			var clonedFile models.File
			if err := cursor.Decode(&clonedFile); err != nil {
				continue
			}
			if clonedFile.Slug != "" {
				purgeURLs = append(purgeURLs, fmt.Sprintf("%s/%s/playlist.m3u8", domain, clonedFile.Slug))
			}
		}
	}

	log.Printf("☁️  [%s] Purging %d playlist URL(s) from Cloudflare cache...", slug, len(purgeURLs))
	if err := utils.PurgeCloudflareCache(ctx, cfConfig, purgeURLs); err != nil {
		log.Printf("⚠️  [%s] Cloudflare purge failed: %v", slug, err)
	}
}
