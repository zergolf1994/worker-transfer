package enums

// ─── Setting Keys ────────────────────────────────────────────────────

const (
	// transfer_config = {enabled, slotRate} — shared with the
	// vdohide-service enqueuer; worker reads only .enabled as a kill switch
	SettingTransferConfig = "transfer_config"

	// Cloudflare cache purge (playlist.m3u8) after new resolutions install
	SettingDomainPlaylist = "domain_playlist"
	SettingCfZoneID       = "cf_zone_id"
	SettingCfApiToken     = "cf_api_token"
)
