package enums

// ─── Setting Keys ────────────────────────────────────────────────────

const (
	// transfer_config = {enabled, slotRate} — shared with the
	// vdohide-service enqueuer; worker reads only .enabled as a kill switch
	SettingTransferConfig = "transfer_config"

	SettingDomainPlaylist = "domain_playlist"
	// domain_profiles = [{name, zone_id, api_token}] — CF profile ต่อโดเมน
	SettingDomainProfiles = "domain_profiles"
	// domain_bindings = {playlist: profileId, content: ..., ...} — จับคู่ purpose → profile
	SettingDomainBindings = "domain_bindings"
)
