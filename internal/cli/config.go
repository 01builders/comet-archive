package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type serveConfig struct {
	Store                  string   `json:"store"`
	Prefix                 string   `json:"prefix"`
	ChainID                string   `json:"chain_id"`
	ManifestName           string   `json:"manifest_name"`
	ManifestKey            string   `json:"manifest_key"`
	DBDir                  string   `json:"db_dir"`
	DBBackend              string   `json:"db_backend"`
	P2PListen              string   `json:"p2p_listen"`
	Moniker                string   `json:"moniker"`
	NodeKeyFile            string   `json:"node_key_file"`
	PersistentPeers        []string `json:"persistent_peers"`
	RequestLimit           *int     `json:"request_limit"`
	ColdWorkers            *int     `json:"cold_workers"`
	ColdManifestCacheTTL   string   `json:"cold_manifest_cache_ttl"`
	RequestTimeout         string   `json:"request_timeout"`
	StatusRequestInterval  string   `json:"status_request_interval"`
	PEX                    *bool    `json:"pex"`
	AddrBookFile           string   `json:"addr_book_file"`
	AddrBookStrict         *bool    `json:"addr_book_strict"`
	Seeds                  []string `json:"seeds"`
	SeedMode               *bool    `json:"seed_mode"`
	PrivatePeerIDs         []string `json:"private_peer_ids"`
	MetricsListen          string   `json:"metrics_listen"`
	SafetyWindow           *int64   `json:"safety_window"`
	ArchiveInterval        string   `json:"archive_interval"`
	PruneInterval          string   `json:"prune_interval"`
	RetainBlocks           *int64   `json:"retain_blocks"`
	EvidenceMaxAgeBlocks   *int64   `json:"evidence_max_age_blocks"`
	EvidenceMaxAgeDuration string   `json:"evidence_max_age_duration"`
	SegmentBlocks          *int     `json:"segment_blocks"`
	Compression            string   `json:"compression"`
	Validation             string   `json:"validation"`
	Checkpoints            []string `json:"checkpoints"`
	ValidatorSets          []string `json:"validator_sets"`
	ValidatorSetRPC        string   `json:"validator_set_rpc"`
	ValidatorSetTimeout    string   `json:"validator_set_timeout"`
	DryRun                 *bool    `json:"dry_run"`
}

type serveSettings struct {
	common               *commonFlags
	dbDir                *string
	dbBackend            *string
	listenAddress        *string
	moniker              *string
	nodeKeyFile          *string
	persistentPeers      *string
	requestLimit         *int
	coldWorkers          *int
	coldManifestCacheTTL *time.Duration
	requestTimeout       *time.Duration
	statusInterval       *time.Duration
	pexEnabled           *bool
	addrBookFile         *string
	addrBookStrict       *bool
	seeds                *string
	seedMode             *bool
	privatePeerIDs       *string
	metricsListen        *string
	safetyWindow         *int64
	archiveInterval      *time.Duration
	pruneInterval        *time.Duration
	retainBlocks         *int64
	evidenceBlocks       *int64
	evidenceDuration     *time.Duration
	segmentBlocks        *int
	compression          *string
	validation           *string
	checkpoints          *[]string
	validatorSets        *[]string
	validatorSetRPC      *string
	validatorSetTimeout  *time.Duration
	dryRun               *bool
}

func applyServeConfig(cmd *cobra.Command, path string, settings serveSettings) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg serveConfig
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("decode config %q: %w", path, err)
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err != nil {
			return fmt.Errorf("decode config %q: %w", path, err)
		}
		return fmt.Errorf("decode config %q: trailing JSON after top-level object", path)
	}
	if cfg.Store != "" && !flagChanged(cmd, "store") {
		settings.common.storeURL = cfg.Store
	}
	if cfg.Prefix != "" && !flagChanged(cmd, "prefix") {
		settings.common.prefix = cfg.Prefix
	}
	if cfg.ChainID != "" && !flagChanged(cmd, "chain-id") {
		settings.common.chainID = cfg.ChainID
	}
	if cfg.ManifestName != "" && !flagChanged(cmd, "manifest-name") {
		settings.common.manifestName = cfg.ManifestName
	}
	if cfg.ManifestKey != "" && !flagChanged(cmd, "manifest-key") {
		settings.common.manifestKey = cfg.ManifestKey
	}
	if cfg.DBDir != "" && !flagChanged(cmd, "db-dir") {
		*settings.dbDir = cfg.DBDir
	}
	if cfg.DBBackend != "" && !flagChanged(cmd, "db-backend") {
		*settings.dbBackend = cfg.DBBackend
	}
	if cfg.P2PListen != "" && !flagChanged(cmd, "p2p-listen") {
		*settings.listenAddress = cfg.P2PListen
	}
	if cfg.Moniker != "" && !flagChanged(cmd, "moniker") {
		*settings.moniker = cfg.Moniker
	}
	if cfg.NodeKeyFile != "" && !flagChanged(cmd, "node-key-file") {
		*settings.nodeKeyFile = cfg.NodeKeyFile
	}
	if len(cfg.PersistentPeers) > 0 && !flagChanged(cmd, "persistent-peers") {
		value, err := configCSV("persistent_peers", cfg.PersistentPeers)
		if err != nil {
			return err
		}
		*settings.persistentPeers = value
	}
	if cfg.RequestLimit != nil && !flagChanged(cmd, "request-limit") {
		*settings.requestLimit = *cfg.RequestLimit
	}
	if cfg.ColdWorkers != nil && !flagChanged(cmd, "cold-workers") {
		*settings.coldWorkers = *cfg.ColdWorkers
	}
	if cfg.ColdManifestCacheTTL != "" && !flagChanged(cmd, "cold-manifest-cache-ttl") {
		duration, err := time.ParseDuration(cfg.ColdManifestCacheTTL)
		if err != nil {
			return fmt.Errorf("config cold_manifest_cache_ttl: %w", err)
		}
		*settings.coldManifestCacheTTL = duration
	}
	if cfg.RequestTimeout != "" && !flagChanged(cmd, "request-timeout") {
		duration, err := time.ParseDuration(cfg.RequestTimeout)
		if err != nil {
			return fmt.Errorf("config request_timeout: %w", err)
		}
		*settings.requestTimeout = duration
	}
	if cfg.StatusRequestInterval != "" && !flagChanged(cmd, "status-request-interval") {
		duration, err := time.ParseDuration(cfg.StatusRequestInterval)
		if err != nil {
			return fmt.Errorf("config status_request_interval: %w", err)
		}
		*settings.statusInterval = duration
	}
	if cfg.PEX != nil && !flagChanged(cmd, "pex") {
		*settings.pexEnabled = *cfg.PEX
	}
	if cfg.AddrBookFile != "" && !flagChanged(cmd, "addr-book-file") {
		*settings.addrBookFile = cfg.AddrBookFile
	}
	if cfg.AddrBookStrict != nil && !flagChanged(cmd, "addr-book-strict") {
		*settings.addrBookStrict = *cfg.AddrBookStrict
	}
	if len(cfg.Seeds) > 0 && !flagChanged(cmd, "seeds") {
		value, err := configCSV("seeds", cfg.Seeds)
		if err != nil {
			return err
		}
		*settings.seeds = value
	}
	if cfg.SeedMode != nil && !flagChanged(cmd, "seed-mode") {
		*settings.seedMode = *cfg.SeedMode
	}
	if len(cfg.PrivatePeerIDs) > 0 && !flagChanged(cmd, "private-peer-ids") {
		value, err := configCSV("private_peer_ids", cfg.PrivatePeerIDs)
		if err != nil {
			return err
		}
		*settings.privatePeerIDs = value
	}
	if cfg.MetricsListen != "" && !flagChanged(cmd, "metrics-listen") {
		*settings.metricsListen = cfg.MetricsListen
	}
	if cfg.SafetyWindow != nil && !flagChanged(cmd, "safety-window") {
		*settings.safetyWindow = *cfg.SafetyWindow
	}
	if cfg.ArchiveInterval != "" && !flagChanged(cmd, "archive-interval") {
		duration, err := time.ParseDuration(cfg.ArchiveInterval)
		if err != nil {
			return fmt.Errorf("config archive_interval: %w", err)
		}
		*settings.archiveInterval = duration
	}
	if cfg.PruneInterval != "" && !flagChanged(cmd, "prune-interval") {
		duration, err := time.ParseDuration(cfg.PruneInterval)
		if err != nil {
			return fmt.Errorf("config prune_interval: %w", err)
		}
		*settings.pruneInterval = duration
	}
	if cfg.RetainBlocks != nil && !flagChanged(cmd, "retain-blocks") {
		*settings.retainBlocks = *cfg.RetainBlocks
	}
	if cfg.EvidenceMaxAgeBlocks != nil && !flagChanged(cmd, "evidence-max-age-blocks") {
		*settings.evidenceBlocks = *cfg.EvidenceMaxAgeBlocks
	}
	if cfg.EvidenceMaxAgeDuration != "" && !flagChanged(cmd, "evidence-max-age-duration") {
		duration, err := time.ParseDuration(cfg.EvidenceMaxAgeDuration)
		if err != nil {
			return fmt.Errorf("config evidence_max_age_duration: %w", err)
		}
		*settings.evidenceDuration = duration
	}
	if cfg.SegmentBlocks != nil && !flagChanged(cmd, "segment-blocks") {
		*settings.segmentBlocks = *cfg.SegmentBlocks
	}
	if cfg.Compression != "" && !flagChanged(cmd, "compression") {
		*settings.compression = cfg.Compression
	}
	if cfg.Validation != "" && !flagChanged(cmd, "validation") {
		*settings.validation = cfg.Validation
	}
	if len(cfg.Checkpoints) > 0 && !flagChanged(cmd, "checkpoint") {
		*settings.checkpoints = cfg.Checkpoints
	}
	if len(cfg.ValidatorSets) > 0 && !flagChanged(cmd, "validator-set") {
		*settings.validatorSets = cfg.ValidatorSets
	}
	if cfg.ValidatorSetRPC != "" && !flagChanged(cmd, "validator-set-rpc") {
		*settings.validatorSetRPC = cfg.ValidatorSetRPC
	}
	if cfg.ValidatorSetTimeout != "" && !flagChanged(cmd, "validator-set-timeout") {
		duration, err := time.ParseDuration(cfg.ValidatorSetTimeout)
		if err != nil {
			return fmt.Errorf("config validator_set_timeout: %w", err)
		}
		*settings.validatorSetTimeout = duration
	}
	if cfg.DryRun != nil && !flagChanged(cmd, "dry-run") {
		*settings.dryRun = *cfg.DryRun
	}
	return nil
}

func configCSV(name string, values []string) (string, error) {
	normalized := make([]string, 0, len(values))
	for i, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return "", fmt.Errorf("config %s[%d] must not be empty", name, i)
		}
		if strings.Contains(value, ",") {
			return "", fmt.Errorf("config %s[%d] must not contain commas", name, i)
		}
		normalized = append(normalized, value)
	}
	return strings.Join(normalized, ","), nil
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flags().Lookup(name)
	return flag != nil && flag.Changed
}
