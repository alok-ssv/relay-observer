package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

const (
	builderBidsPath   = "/relay/v1/data/bidtraces/builder_blocks_received"
	deliveredBidsPath = "/relay/v1/data/bidtraces/proposer_payload_delivered"
)

var weiPerETH = new(big.Float).SetInt(big.NewInt(1_000_000_000_000_000_000))
var gweiPerETH = new(big.Float).SetInt(big.NewInt(1_000_000_000))
var waitForSecondsPattern = regexp.MustCompile(`(?i)wait\s+for\s+([0-9]+)\s*s`)

type Config struct {
	BeaconEndpoint   string
	RelayArg         string
	StartSlotArg     string
	EndSlotArg       string
	StartTimeArg     string
	EndTimeArg       string
	Concurrency      int
	RelayConcurrency int
	HTTPTimeout      time.Duration
	BucketMS         int64
	AllRelayBids     bool
	PrintAllBids     bool
	AllowInferred    bool
	EthGasPoolAddr   string
	MaxRetries       int
	RetryBackoff     time.Duration
}

type RetryPolicy struct {
	MaxRetries  int
	BaseBackoff time.Duration
}

type ChainSpec struct {
	GenesisTime    time.Time
	SecondsPerSlot uint64
}

type RelayClient struct {
	Name       string
	BaseURL    string
	IsETHGas   bool
	HTTPClient *http.Client
	Retry      RetryPolicy
}

type SlotAnalysis struct {
	Slot               uint64
	HasBeaconBlock     bool
	BeaconBlockHash    string
	BeaconFeeRecipient string
	ETHGasMode         string
	WinningRelay       string
	WinningRelays      []string
	WinningBlockHash   string
	WinningValueETH    float64
	WinningValueKnown  bool
	WinningMSInto      *int64
	MaxBidETH          float64
	MaxBidKnown        bool
	MaxBidMSInto       *int64
	TotalBids          int
	Bids               []BidSample
	Notes              []string
}

type BidSample struct {
	Slot         uint64
	Relay        string
	BlockHash    string
	ValueWei     string
	ValueETH     float64
	TimestampMS  uint64
	MSIntoSlot   int64
	HasTimestamp bool
	IsWinningBid bool
}

type winnerCandidate struct {
	Relay string
	Trace BidTrace
}

type relaySlotFetch struct {
	RelayName    string
	Bids         []BidTrace
	Delivered    []BidTrace
	BuilderErr   error
	DeliveredErr error
}

type latenessAgg struct {
	Count           int
	TotalETH        float64
	WinCount        int
	WinningTotalETH float64
	MaxBidETH       float64
	Slots           map[uint64]struct{}
	WinningSlots    map[uint64]struct{}
}

type FlexString string

func (f *FlexString) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "null" || trimmed == "" {
		*f = ""
		return nil
	}

	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		*f = FlexString(strings.TrimSpace(s))
		return nil
	}

	*f = FlexString(trimmed)
	return nil
}

func (f FlexString) String() string {
	return string(f)
}

type FlexUint64 uint64

func (f *FlexUint64) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "null" || trimmed == "" {
		*f = 0
		return nil
	}

	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		s = strings.TrimSpace(s)
		if s == "" {
			*f = 0
			return nil
		}

		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid uint64 value %q: %w", s, err)
		}
		*f = FlexUint64(n)
		return nil
	}

	n, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid uint64 value %q: %w", trimmed, err)
	}
	*f = FlexUint64(n)
	return nil
}

func (f FlexUint64) Uint64() uint64 {
	return uint64(f)
}

type BidTrace struct {
	Slot        FlexUint64 `json:"slot"`
	BlockHash   string     `json:"block_hash"`
	Value       FlexString `json:"value"`
	TimestampMS FlexUint64 `json:"timestamp_ms"`
}

type beaconGenesisResponse struct {
	Data struct {
		GenesisTime FlexString `json:"genesis_time"`
	} `json:"data"`
}

type beaconSpecResponse struct {
	Data map[string]FlexString `json:"data"`
}

type beaconBlockResponse struct {
	Data struct {
		Message struct {
			Body struct {
				ExecutionPayload struct {
					BlockHash    string `json:"block_hash"`
					FeeRecipient string `json:"fee_recipient"`
				} `json:"execution_payload"`
				ExecutionPayloadHeader struct {
					BlockHash    string `json:"block_hash"`
					FeeRecipient string `json:"fee_recipient"`
				} `json:"execution_payload_header"`
			} `json:"body"`
		} `json:"message"`
	} `json:"data"`
}

type beaconBlockRewardResponse struct {
	Data struct {
		Total FlexString `json:"total"`
	} `json:"data"`
}

func main() {
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.BeaconEndpoint, "beacon-endpoint", "", "Beacon REST API endpoint (required), e.g. http://127.0.0.1:5052")
	flag.StringVar(&cfg.RelayArg, "relays", "", "Comma-separated relays as URL or name=url (required)")
	flag.StringVar(&cfg.StartSlotArg, "start-slot", "", "Start slot (inclusive)")
	flag.StringVar(&cfg.EndSlotArg, "end-slot", "", "End slot (inclusive)")
	flag.StringVar(&cfg.StartTimeArg, "start-time", "", "Start time (RFC3339 or unix seconds/millis)")
	flag.StringVar(&cfg.EndTimeArg, "end-time", "", "End time (RFC3339 or unix seconds/millis)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 8, "Number of slots analyzed in parallel")
	flag.IntVar(&cfg.RelayConcurrency, "relay-concurrency", 4, "Number of per-slot relay requests in parallel")
	flag.DurationVar(&cfg.HTTPTimeout, "http-timeout", 10*time.Second, "HTTP timeout per request")
	flag.Int64Var(&cfg.BucketMS, "bucket-ms", 100, "Bucket size in milliseconds for lateness stats")
	flag.BoolVar(&cfg.AllRelayBids, "all-relay-bids", false, "Collect all relay bids (default: only winning bids)")
	flag.BoolVar(&cfg.PrintAllBids, "print-bids", false, "Print every bid row in addition to summary tables")
	flag.BoolVar(&cfg.AllowInferred, "allow-inferred-winner", false, "Allow inferred winner attribution when canonical matching is unavailable")
	flag.StringVar(&cfg.EthGasPoolAddr, "ethgas-pool-address", "", "EthGasPool fee recipient address (0x...) used to classify ETHGasExternal blocks")
	flag.IntVar(&cfg.MaxRetries, "max-retries", 3, "Max retries for timeout/rate-limit/server errors")
	flag.DurationVar(&cfg.RetryBackoff, "retry-backoff", 300*time.Millisecond, "Base exponential backoff for retries")
	flag.Parse()

	return cfg
}

func run(cfg Config) error {
	if strings.TrimSpace(cfg.BeaconEndpoint) == "" {
		return errors.New("missing -beacon-endpoint")
	}
	if strings.TrimSpace(cfg.RelayArg) == "" {
		return errors.New("missing -relays")
	}
	if cfg.Concurrency <= 0 {
		return errors.New("-concurrency must be >= 1")
	}
	if cfg.RelayConcurrency <= 0 {
		return errors.New("-relay-concurrency must be >= 1")
	}
	if cfg.BucketMS <= 0 {
		return errors.New("-bucket-ms must be >= 1")
	}
	if cfg.MaxRetries < 0 {
		return errors.New("-max-retries must be >= 0")
	}
	if cfg.RetryBackoff <= 0 {
		return errors.New("-retry-backoff must be > 0")
	}
	normalizedEthGasPoolAddr, err := normalizeAddress(cfg.EthGasPoolAddr)
	if err != nil {
		return fmt.Errorf("invalid -ethgas-pool-address: %w", err)
	}

	httpClient := &http.Client{Timeout: cfg.HTTPTimeout}
	retryPolicy := RetryPolicy{
		MaxRetries:  cfg.MaxRetries,
		BaseBackoff: cfg.RetryBackoff,
	}

	beaconEndpoint := strings.TrimRight(strings.TrimSpace(cfg.BeaconEndpoint), "/")
	relays, err := parseRelays(cfg.RelayArg, httpClient, retryPolicy)
	if err != nil {
		return err
	}
	if len(relays) == 0 {
		return errors.New("no relays parsed from -relays")
	}

	ctx := context.Background()
	spec, err := fetchChainSpec(ctx, httpClient, beaconEndpoint, retryPolicy)
	if err != nil {
		return fmt.Errorf("fetch beacon chain spec: %w", err)
	}

	startSlot, endSlot, err := resolveSlotRange(cfg, spec)
	if err != nil {
		return err
	}

	if endSlot < startSlot {
		return fmt.Errorf("end slot (%d) cannot be lower than start slot (%d)", endSlot, startSlot)
	}

	results := analyzeSlotRange(
		ctx,
		httpClient,
		beaconEndpoint,
		relays,
		spec,
		startSlot,
		endSlot,
		cfg.Concurrency,
		cfg.RelayConcurrency,
		cfg.AllRelayBids,
		cfg.AllowInferred,
		retryPolicy,
	)
	sort.Slice(results, func(i, j int) bool { return results[i].Slot < results[j].Slot })
	tagETHGasModes(results, buildETHGasRelaySet(relays), normalizedEthGasPoolAddr)

	printSlotSummary(results)
	fmt.Println()
	printLatenessStats(results, cfg.BucketMS, cfg.AllRelayBids)

	if cfg.PrintAllBids {
		fmt.Println()
		printAllBids(results)
	}

	fmt.Println()
	printETHGasModeStats(results, normalizedEthGasPoolAddr)

	fmt.Println()
	printPerRelayBidStats(results)

	return nil
}

func parseRelays(arg string, httpClient *http.Client, retryPolicy RetryPolicy) ([]RelayClient, error) {
	parts := strings.Split(arg, ",")
	relays := make([]RelayClient, 0, len(parts))
	seen := map[string]bool{}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		name := ""
		rawURL := part
		if strings.Contains(part, "=") {
			p := strings.SplitN(part, "=", 2)
			name = strings.TrimSpace(p[0])
			rawURL = strings.TrimSpace(p[1])
		}

		if rawURL == "" {
			return nil, fmt.Errorf("invalid relay entry %q", part)
		}

		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("invalid relay URL %q", rawURL)
		}

		if name == "" {
			name = parsed.Host
		}

		baseURL := strings.TrimRight(rawURL, "/")
		if seen[baseURL] {
			continue
		}
		seen[baseURL] = true

		relays = append(relays, RelayClient{
			Name:       name,
			BaseURL:    baseURL,
			IsETHGas:   isETHGasRelayName(name) || isETHGasRelayName(baseURL),
			HTTPClient: httpClient,
			Retry:      retryPolicy,
		})
	}

	return relays, nil
}

func fetchChainSpec(ctx context.Context, httpClient *http.Client, beaconEndpoint string, retryPolicy RetryPolicy) (ChainSpec, error) {
	specURL := beaconEndpoint + "/eth/v1/config/spec"
	genesisURL := beaconEndpoint + "/eth/v1/beacon/genesis"

	specResp := beaconSpecResponse{}
	if err := getJSON(ctx, httpClient, specURL, retryPolicy, &specResp); err != nil {
		return ChainSpec{}, err
	}

	genesisResp := beaconGenesisResponse{}
	if err := getJSON(ctx, httpClient, genesisURL, retryPolicy, &genesisResp); err != nil {
		return ChainSpec{}, err
	}

	secPerSlotRaw, ok := specResp.Data["SECONDS_PER_SLOT"]
	if !ok {
		return ChainSpec{}, errors.New("SECONDS_PER_SLOT missing from beacon spec")
	}

	secPerSlot, err := strconv.ParseUint(secPerSlotRaw.String(), 10, 64)
	if err != nil {
		return ChainSpec{}, fmt.Errorf("invalid SECONDS_PER_SLOT %q: %w", secPerSlotRaw, err)
	}

	genesisUnix, err := strconv.ParseInt(genesisResp.Data.GenesisTime.String(), 10, 64)
	if err != nil {
		return ChainSpec{}, fmt.Errorf("invalid genesis_time %q: %w", genesisResp.Data.GenesisTime, err)
	}

	return ChainSpec{
		GenesisTime:    time.Unix(genesisUnix, 0).UTC(),
		SecondsPerSlot: secPerSlot,
	}, nil
}

func resolveSlotRange(cfg Config, spec ChainSpec) (uint64, uint64, error) {
	hasSlotRange := strings.TrimSpace(cfg.StartSlotArg) != "" || strings.TrimSpace(cfg.EndSlotArg) != ""
	hasTimeRange := strings.TrimSpace(cfg.StartTimeArg) != "" || strings.TrimSpace(cfg.EndTimeArg) != ""

	if hasSlotRange && hasTimeRange {
		return 0, 0, errors.New("use either slot range flags or time range flags, not both")
	}

	if hasSlotRange {
		if strings.TrimSpace(cfg.StartSlotArg) == "" || strings.TrimSpace(cfg.EndSlotArg) == "" {
			return 0, 0, errors.New("both -start-slot and -end-slot are required")
		}

		startSlot, err := strconv.ParseUint(strings.TrimSpace(cfg.StartSlotArg), 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid -start-slot %q: %w", cfg.StartSlotArg, err)
		}

		endSlot, err := strconv.ParseUint(strings.TrimSpace(cfg.EndSlotArg), 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid -end-slot %q: %w", cfg.EndSlotArg, err)
		}

		return startSlot, endSlot, nil
	}

	if hasTimeRange {
		if strings.TrimSpace(cfg.StartTimeArg) == "" || strings.TrimSpace(cfg.EndTimeArg) == "" {
			return 0, 0, errors.New("both -start-time and -end-time are required")
		}

		startTime, err := parseFlexibleTime(cfg.StartTimeArg)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid -start-time: %w", err)
		}

		endTime, err := parseFlexibleTime(cfg.EndTimeArg)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid -end-time: %w", err)
		}

		startSlot, err := slotForTime(startTime, spec)
		if err != nil {
			return 0, 0, err
		}

		endSlot, err := slotForTime(endTime, spec)
		if err != nil {
			return 0, 0, err
		}

		return startSlot, endSlot, nil
	}

	return 0, 0, errors.New("provide either slot range flags or time range flags")
}

func parseFlexibleTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, errors.New("empty time")
	}

	if unix, err := strconv.ParseInt(raw, 10, 64); err == nil {
		// Treat >10-digit values as milliseconds.
		if len(raw) > 10 {
			return time.UnixMilli(unix).UTC(), nil
		}
		return time.Unix(unix, 0).UTC(), nil
	}

	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected RFC3339 or unix time, got %q", raw)
	}
	return t.UTC(), nil
}

func slotForTime(t time.Time, spec ChainSpec) (uint64, error) {
	if t.Before(spec.GenesisTime) {
		return 0, fmt.Errorf("time %s is before genesis %s", t.Format(time.RFC3339), spec.GenesisTime.Format(time.RFC3339))
	}
	delta := t.Unix() - spec.GenesisTime.Unix()
	return uint64(delta) / spec.SecondsPerSlot, nil
}

func analyzeSlotRange(
	ctx context.Context,
	httpClient *http.Client,
	beaconEndpoint string,
	relays []RelayClient,
	spec ChainSpec,
	startSlot uint64,
	endSlot uint64,
	concurrency int,
	relayConcurrency int,
	allRelayBids bool,
	allowInferred bool,
	retryPolicy RetryPolicy,
) []SlotAnalysis {
	totalSlots := int(endSlot - startSlot + 1)
	results := make([]SlotAnalysis, 0, totalSlots)
	resultsCh := make(chan SlotAnalysis, totalSlots)
	jobs := make(chan uint64)

	workerCount := concurrency
	if totalSlots < workerCount {
		workerCount = totalSlots
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for slot := range jobs {
				resultsCh <- analyzeSingleSlot(ctx, httpClient, beaconEndpoint, relays, spec, slot, relayConcurrency, allRelayBids, allowInferred, retryPolicy)
			}
		}()
	}

	go func() {
		for slot := startSlot; slot <= endSlot; slot++ {
			jobs <- slot
		}
		close(jobs)
		wg.Wait()
		close(resultsCh)
	}()

	for result := range resultsCh {
		results = append(results, result)
	}

	return results
}

func analyzeSingleSlot(
	ctx context.Context,
	httpClient *http.Client,
	beaconEndpoint string,
	relays []RelayClient,
	spec ChainSpec,
	slot uint64,
	relayConcurrency int,
	allRelayBids bool,
	allowInferred bool,
	retryPolicy RetryPolicy,
) SlotAnalysis {
	result := SlotAnalysis{Slot: slot}
	slotStartMS := spec.GenesisTime.UnixMilli() + int64(slot)*int64(spec.SecondsPerSlot)*1000

	beaconBlockHash, beaconFeeRecipient, hasBeaconBlock, err := fetchBeaconExecutionInfo(ctx, httpClient, beaconEndpoint, slot, retryPolicy)
	if err != nil {
		result.Notes = append(result.Notes, "beacon block lookup failed: "+err.Error())
	}
	if hasBeaconBlock {
		result.HasBeaconBlock = true
		result.BeaconBlockHash = beaconBlockHash
		result.BeaconFeeRecipient = beaconFeeRecipient
	}

	candidates := make([]winnerCandidate, 0, len(relays))
	relayByName := make(map[string]RelayClient, len(relays))
	for _, relay := range relays {
		relayByName[strings.ToLower(strings.TrimSpace(relay.Name))] = relay
	}
	fetchesCh := make(chan relaySlotFetch, len(relays))
	var relayWG sync.WaitGroup
	relaySem := make(chan struct{}, relayConcurrency)
	for _, relay := range relays {
		relay := relay
		relayWG.Add(1)
		go func() {
			defer relayWG.Done()
			relaySem <- struct{}{}
			defer func() { <-relaySem }()

			var bids []BidTrace
			var builderErr error
			if allRelayBids {
				bids, builderErr = relay.fetchBidTraces(ctx, builderBidsPath, slot)
			}
			delivered, deliveredErr := relay.fetchBidTraces(ctx, deliveredBidsPath, slot)
			fetchesCh <- relaySlotFetch{
				RelayName:    relay.Name,
				Bids:         bids,
				Delivered:    delivered,
				BuilderErr:   builderErr,
				DeliveredErr: deliveredErr,
			}
		}()
	}
	relayWG.Wait()
	close(fetchesCh)

	for fetch := range fetchesCh {
		if allRelayBids && fetch.BuilderErr != nil {
			result.Notes = append(result.Notes, fmt.Sprintf("builder bids failed for %s: %v", fetch.RelayName, fetch.BuilderErr))
		}
		if fetch.DeliveredErr != nil {
			result.Notes = append(result.Notes, fmt.Sprintf("delivered bids failed for %s: %v", fetch.RelayName, fetch.DeliveredErr))
		}

		for _, trace := range fetch.Delivered {
			candidates = append(candidates, winnerCandidate{Relay: fetch.RelayName, Trace: trace})
		}

		if allRelayBids {
			for _, trace := range fetch.Bids {
				valueETH, err := weiToETH(trace.Value.String())
				if err != nil {
					result.Notes = append(result.Notes, fmt.Sprintf("invalid value for relay %s block %s: %v", fetch.RelayName, trace.BlockHash, err))
					continue
				}

				ts := trace.TimestampMS.Uint64()
				msIntoSlot, hasTimestamp := normalizeMSIntoSlot(ts, slotStartMS)
				result.Bids = append(result.Bids, BidSample{
					Slot:         slot,
					Relay:        fetch.RelayName,
					BlockHash:    strings.TrimSpace(trace.BlockHash),
					ValueWei:     trace.Value.String(),
					ValueETH:     valueETH,
					TimestampMS:  ts,
					MSIntoSlot:   msIntoSlot,
					HasTimestamp: hasTimestamp,
				})
			}
		}
	}

	winningCandidates, rewardCandidate, note := chooseWinningCandidate(candidates, beaconBlockHash, allowInferred)
	if note != "" {
		result.Notes = append(result.Notes, note)
	}

	if len(winningCandidates) > 0 {
		result.WinningRelays = uniqueRelayNames(winningCandidates)
		result.WinningRelay = strings.Join(result.WinningRelays, ",")
	}

	if rewardCandidate != nil {
		result.WinningBlockHash = strings.TrimSpace(rewardCandidate.Trace.BlockHash)
		valueETH, err := weiToETH(rewardCandidate.Trace.Value.String())
		if err == nil {
			result.WinningValueETH = valueETH
			result.WinningValueKnown = true
		} else {
			result.Notes = append(result.Notes, "winning value parse error: "+err.Error())
		}

		if rewardCandidate.Trace.TimestampMS.Uint64() > 0 {
			ms := int64(rewardCandidate.Trace.TimestampMS.Uint64()) - slotStartMS
			result.WinningMSInto = &ms
		}
	}

	if len(winningCandidates) == 0 && hasBeaconBlock && result.BeaconBlockHash != "" {
		result.WinningRelay = "self-built"
		result.WinningRelays = []string{"self-built"}
		result.WinningBlockHash = result.BeaconBlockHash
		result.Notes = append(result.Notes, "canonical block not found in relay delivered traces; attributed as self-built")

		rewardETH, ok, rewardErr := fetchBeaconBlockRewardETH(ctx, httpClient, beaconEndpoint, slot, retryPolicy)
		if rewardErr != nil {
			result.Notes = append(result.Notes, "self-built reward lookup failed: "+rewardErr.Error())
		} else if ok {
			result.WinningValueETH = rewardETH
			result.WinningValueKnown = true
			result.Notes = append(result.Notes, "self-built reward sourced from beacon block rewards endpoint")
		}
	}

	if len(result.WinningRelays) > 0 && !(len(result.WinningRelays) == 1 && result.WinningRelays[0] == "self-built") {
		winningRelaySet := make(map[string]struct{}, len(result.WinningRelays))
		for _, relay := range result.WinningRelays {
			winningRelaySet[strings.ToLower(relay)] = struct{}{}
		}

		for i := range result.Bids {
			if _, ok := winningRelaySet[strings.ToLower(result.Bids[i].Relay)]; !ok {
				continue
			}
			if result.WinningBlockHash != "" && !strings.EqualFold(result.Bids[i].BlockHash, result.WinningBlockHash) {
				continue
			}

			result.Bids[i].IsWinningBid = true
			if result.WinningMSInto == nil && result.Bids[i].HasTimestamp {
				ms := result.Bids[i].MSIntoSlot
				result.WinningMSInto = &ms
			}
		}
	}

	if hasBeaconBlock && result.WinningRelay == "" {
		result.Notes = append(result.Notes, "no relay candidate matched this canonical beacon block")
	}

	if !allRelayBids {
		result.Bids = fetchWinningBidSamplesFromBuilder(
			ctx,
			slot,
			slotStartMS,
			result.WinningBlockHash,
			winningCandidates,
			relayByName,
			relayConcurrency,
			&result,
		)
		if len(result.Bids) == 0 {
			// Fallback to delivered traces if builder bid timestamps are unavailable.
			result.Bids = winningCandidatesToBidSamples(winningCandidates, slot, slotStartMS, &result)
		}
	}

	for _, bid := range result.Bids {
		if !result.MaxBidKnown || bid.ValueETH > result.MaxBidETH {
			result.MaxBidETH = bid.ValueETH
			result.MaxBidKnown = true
			result.MaxBidMSInto = nil
			if bid.HasTimestamp {
				ms := bid.MSIntoSlot
				result.MaxBidMSInto = &ms
			}
			continue
		}

		if !result.MaxBidKnown || bid.ValueETH != result.MaxBidETH {
			continue
		}

		if result.MaxBidMSInto == nil && bid.HasTimestamp {
			ms := bid.MSIntoSlot
			result.MaxBidMSInto = &ms
			continue
		}

		if result.MaxBidMSInto != nil && bid.HasTimestamp && bid.MSIntoSlot < *result.MaxBidMSInto {
			ms := bid.MSIntoSlot
			result.MaxBidMSInto = &ms
		}
	}

	result.TotalBids = len(result.Bids)

	return result
}

func chooseWinningCandidate(candidates []winnerCandidate, beaconBlockHash string, allowInferred bool) ([]winnerCandidate, *winnerCandidate, string) {
	if len(candidates) == 0 {
		return nil, nil, ""
	}

	beaconBlockHash = strings.TrimSpace(beaconBlockHash)
	if beaconBlockHash == "" {
		if allowInferred {
			selected := highestValueCandidate(candidates)
			return []winnerCandidate{selected}, &selected, "inferred winner: beacon block hash unavailable; selected highest value delivered trace"
		}
		return nil, nil, "beacon block hash unavailable; winner relay cannot be attributed safely"
	}

	matching := make([]winnerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.EqualFold(strings.TrimSpace(candidate.Trace.BlockHash), beaconBlockHash) {
			matching = append(matching, candidate)
		}
	}

	if len(matching) == 1 {
		selected := matching[0]
		return matching, &selected, ""
	}

	if len(matching) > 1 {
		selected := highestValueCandidate(matching)
		return matching, &selected, "multiple relays delivered canonical block hash; tracking all matching relays and using highest value trace for reward"
	}

	return nil, nil, "no relay delivered trace matched the canonical beacon block hash"
}

func uniqueRelayNames(candidates []winnerCandidate) []string {
	seen := map[string]bool{}
	names := make([]string, 0, len(candidates))

	for _, candidate := range candidates {
		name := strings.TrimSpace(candidate.Relay)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		seen[key] = true
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

func winningCandidatesToBidSamples(candidates []winnerCandidate, slot uint64, slotStartMS int64, result *SlotAnalysis) []BidSample {
	if len(candidates) == 0 {
		return nil
	}

	seen := map[string]bool{}
	out := make([]BidSample, 0, len(candidates))
	for _, candidate := range candidates {
		blockHash := strings.TrimSpace(candidate.Trace.BlockHash)
		valueWei := candidate.Trace.Value.String()
		ts := candidate.Trace.TimestampMS.Uint64()
		key := strings.ToLower(candidate.Relay) + "|" + strings.ToLower(blockHash) + "|" + strconv.FormatUint(ts, 10) + "|" + valueWei
		if seen[key] {
			continue
		}
		seen[key] = true

		valueETH, err := weiToETH(valueWei)
		if err != nil {
			result.Notes = append(result.Notes, fmt.Sprintf("invalid winning value for relay %s block %s: %v", candidate.Relay, blockHash, err))
			continue
		}
		msIntoSlot, hasTimestamp := normalizeMSIntoSlot(ts, slotStartMS)

		out = append(out, BidSample{
			Slot:         slot,
			Relay:        candidate.Relay,
			BlockHash:    blockHash,
			ValueWei:     valueWei,
			ValueETH:     valueETH,
			TimestampMS:  ts,
			MSIntoSlot:   msIntoSlot,
			HasTimestamp: hasTimestamp,
			IsWinningBid: true,
		})
	}

	return out
}

func fetchWinningBidSamplesFromBuilder(
	ctx context.Context,
	slot uint64,
	slotStartMS int64,
	winningBlockHash string,
	winningCandidates []winnerCandidate,
	relayByName map[string]RelayClient,
	relayConcurrency int,
	result *SlotAnalysis,
) []BidSample {
	type relayTarget struct {
		RelayName string
		BlockHash string
	}
	type fetchResult struct {
		Target relayTarget
		Traces []BidTrace
		Err    error
	}

	targetsByKey := map[string]relayTarget{}
	for _, candidate := range winningCandidates {
		relayName := strings.TrimSpace(candidate.Relay)
		if relayName == "" {
			continue
		}

		targetHash := strings.TrimSpace(winningBlockHash)
		if targetHash == "" {
			targetHash = strings.TrimSpace(candidate.Trace.BlockHash)
		}

		key := strings.ToLower(relayName)
		if existing, ok := targetsByKey[key]; ok {
			if existing.BlockHash == "" && targetHash != "" {
				existing.BlockHash = targetHash
				targetsByKey[key] = existing
			}
			continue
		}

		targetsByKey[key] = relayTarget{
			RelayName: relayName,
			BlockHash: targetHash,
		}
	}

	if len(targetsByKey) == 0 {
		return nil
	}

	targets := make([]relayTarget, 0, len(targetsByKey))
	for _, t := range targetsByKey {
		targets = append(targets, t)
	}

	resultsCh := make(chan fetchResult, len(targets))
	var wg sync.WaitGroup
	sem := make(chan struct{}, relayConcurrency)

	for _, target := range targets {
		target := target
		relayClient, ok := relayByName[strings.ToLower(target.RelayName)]
		if !ok {
			result.Notes = append(result.Notes, fmt.Sprintf("winning relay %s missing from relay client map", target.RelayName))
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			traces, err := relayClient.fetchBidTraces(ctx, builderBidsPath, slot)
			resultsCh <- fetchResult{
				Target: target,
				Traces: traces,
				Err:    err,
			}
		}()
	}

	wg.Wait()
	close(resultsCh)

	out := make([]BidSample, 0, len(targets))
	for fr := range resultsCh {
		if fr.Err != nil {
			result.Notes = append(result.Notes, fmt.Sprintf("builder bids failed for %s: %v", fr.Target.RelayName, fr.Err))
			continue
		}

		filtered := make([]BidTrace, 0, len(fr.Traces))
		for _, trace := range fr.Traces {
			if fr.Target.BlockHash != "" && !strings.EqualFold(strings.TrimSpace(trace.BlockHash), fr.Target.BlockHash) {
				continue
			}
			filtered = append(filtered, trace)
		}
		if len(filtered) == 0 {
			continue
		}

		bestTrace, ok := highestValueTrace(filtered)
		if !ok {
			continue
		}

		valueWei := bestTrace.Value.String()
		valueETH, err := weiToETH(valueWei)
		if err != nil {
			result.Notes = append(result.Notes, fmt.Sprintf("invalid winning value for relay %s block %s: %v", fr.Target.RelayName, bestTrace.BlockHash, err))
			continue
		}

		ts := bestTrace.TimestampMS.Uint64()
		msIntoSlot, hasTimestamp := normalizeMSIntoSlot(ts, slotStartMS)
		out = append(out, BidSample{
			Slot:         slot,
			Relay:        fr.Target.RelayName,
			BlockHash:    strings.TrimSpace(bestTrace.BlockHash),
			ValueWei:     valueWei,
			ValueETH:     valueETH,
			TimestampMS:  ts,
			MSIntoSlot:   msIntoSlot,
			HasTimestamp: hasTimestamp,
			IsWinningBid: true,
		})
	}

	return out
}

func highestValueTrace(traces []BidTrace) (BidTrace, bool) {
	if len(traces) == 0 {
		return BidTrace{}, false
	}

	best := traces[0]
	bestValue := parseWei(best.Value.String())
	bestTS := best.TimestampMS.Uint64()

	for i := 1; i < len(traces); i++ {
		value := parseWei(traces[i].Value.String())
		ts := traces[i].TimestampMS.Uint64()
		cmp := value.Cmp(bestValue)
		if cmp > 0 || (cmp == 0 && ts > bestTS) {
			best = traces[i]
			bestValue = value
			bestTS = ts
		}
	}

	return best, true
}

func normalizeMSIntoSlot(timestampMS uint64, slotStartMS int64) (int64, bool) {
	if timestampMS == 0 {
		return 0, false
	}
	return int64(timestampMS) - slotStartMS, true
}

func highestValueCandidate(candidates []winnerCandidate) winnerCandidate {
	if len(candidates) == 0 {
		return winnerCandidate{}
	}

	best := candidates[0]
	bestValue := parseWei(best.Trace.Value.String())

	for i := 1; i < len(candidates); i++ {
		value := parseWei(candidates[i].Trace.Value.String())
		if value.Cmp(bestValue) > 0 {
			best = candidates[i]
			bestValue = value
		}
	}

	return best
}

func parseWei(raw string) *big.Int {
	n := new(big.Int)
	if _, ok := n.SetString(strings.TrimSpace(raw), 10); ok {
		return n
	}
	return big.NewInt(0)
}

func weiToETH(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("empty wei value")
	}

	value := new(big.Int)
	if _, ok := value.SetString(raw, 10); !ok {
		return 0, fmt.Errorf("invalid wei value %q", raw)
	}

	f := new(big.Float).SetInt(value)
	eth := new(big.Float).Quo(f, weiPerETH)
	result, _ := eth.Float64()
	return result, nil
}

func gweiToETH(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("empty gwei value")
	}

	value := new(big.Int)
	if _, ok := value.SetString(raw, 10); !ok {
		return 0, fmt.Errorf("invalid gwei value %q", raw)
	}

	f := new(big.Float).SetInt(value)
	eth := new(big.Float).Quo(f, gweiPerETH)
	result, _ := eth.Float64()
	return result, nil
}

func fetchBeaconBlockRewardETH(
	ctx context.Context,
	httpClient *http.Client,
	beaconEndpoint string,
	slot uint64,
	retryPolicy RetryPolicy,
) (float64, bool, error) {
	endpoint := fmt.Sprintf("%s/eth/v1/beacon/rewards/blocks/%d", strings.TrimRight(beaconEndpoint, "/"), slot)
	resp, err := getWithRetry(ctx, httpClient, endpoint, retryPolicy)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return 0, false, nil
	}

	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		return 0, false, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return 0, false, fmt.Errorf("status %d from %s: %s", resp.StatusCode, endpoint, strings.TrimSpace(string(body)))
	}

	out := beaconBlockRewardResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, false, err
	}

	if strings.TrimSpace(out.Data.Total.String()) == "" {
		return 0, false, nil
	}

	rewardETH, err := gweiToETH(out.Data.Total.String())
	if err != nil {
		return 0, false, err
	}

	return rewardETH, true, nil
}

func fetchBeaconExecutionInfo(
	ctx context.Context,
	httpClient *http.Client,
	beaconEndpoint string,
	slot uint64,
	retryPolicy RetryPolicy,
) (string, string, bool, error) {
	endpoint := fmt.Sprintf("%s/eth/v2/beacon/blocks/%d", strings.TrimRight(beaconEndpoint, "/"), slot)
	var lastErr error
	for attempt := 0; attempt <= retryPolicy.MaxRetries; attempt++ {
		resp, err := getWithRetry(ctx, httpClient, endpoint, RetryPolicy{
			MaxRetries:  0,
			BaseBackoff: retryPolicy.BaseBackoff,
		})
		if err != nil {
			lastErr = err
			if attempt < retryPolicy.MaxRetries && isRetryableRequestErr(err) {
				if sleepErr := sleepForRetry(ctx, retryDelay(attempt, retryPolicy.BaseBackoff)); sleepErr != nil {
					return "", "", false, sleepErr
				}
				continue
			}
			return "", "", false, err
		}

		if resp.StatusCode == http.StatusNotFound {
			_ = resp.Body.Close()
			return "", "", false, nil
		}

		if isRetryableHTTPStatus(resp.StatusCode) && attempt < retryPolicy.MaxRetries {
			retryDelayHint := retryDelay(attempt, retryPolicy.BaseBackoff)
			if hinted, ok := retryAfterDelay(resp.Header.Get("Retry-After")); ok {
				retryDelayHint = hinted
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
			_ = resp.Body.Close()
			if sleepErr := sleepForRetry(ctx, retryDelayHint); sleepErr != nil {
				return "", "", false, sleepErr
			}
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			_ = resp.Body.Close()
			return "", "", false, fmt.Errorf("beacon response status %d from %s: %s", resp.StatusCode, endpoint, strings.TrimSpace(string(body)))
		}

		out := beaconBlockResponse{}
		err = json.NewDecoder(resp.Body).Decode(&out)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			if attempt < retryPolicy.MaxRetries && isRetryableReadOrDecodeErr(err) {
				if sleepErr := sleepForRetry(ctx, retryDelay(attempt, retryPolicy.BaseBackoff)); sleepErr != nil {
					return "", "", false, sleepErr
				}
				continue
			}
			return "", "", false, err
		}

		blockHash := strings.TrimSpace(out.Data.Message.Body.ExecutionPayload.BlockHash)
		if blockHash == "" {
			blockHash = strings.TrimSpace(out.Data.Message.Body.ExecutionPayloadHeader.BlockHash)
		}
		feeRecipient := strings.TrimSpace(out.Data.Message.Body.ExecutionPayload.FeeRecipient)
		if feeRecipient == "" {
			feeRecipient = strings.TrimSpace(out.Data.Message.Body.ExecutionPayloadHeader.FeeRecipient)
		}
		normalizedFeeRecipient, err := normalizeAddress(feeRecipient)
		if err == nil {
			feeRecipient = normalizedFeeRecipient
		}

		return blockHash, feeRecipient, true, nil
	}

	if lastErr != nil {
		return "", "", false, lastErr
	}
	return "", "", false, fmt.Errorf("beacon block fetch failed for %s", endpoint)
}

func (r RelayClient) fetchBidTraces(ctx context.Context, path string, slot uint64) ([]BidTrace, error) {
	endpoint := fmt.Sprintf("%s%s?slot=%d", strings.TrimRight(r.BaseURL, "/"), path, slot)
	resp, err := getWithRetry(ctx, r.HTTPClient, endpoint, r.Retry)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("status %d from %s: %s", resp.StatusCode, endpoint, strings.TrimSpace(string(body)))
	}

	var traces []BidTrace
	if err := json.NewDecoder(resp.Body).Decode(&traces); err != nil {
		return nil, err
	}
	return traces, nil
}

func getJSON(ctx context.Context, httpClient *http.Client, endpoint string, retryPolicy RetryPolicy, out interface{}) error {
	resp, err := getWithRetry(ctx, httpClient, endpoint, retryPolicy)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d from %s: %s", resp.StatusCode, endpoint, strings.TrimSpace(string(body)))
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

func getWithRetry(ctx context.Context, httpClient *http.Client, endpoint string, retryPolicy RetryPolicy) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= retryPolicy.MaxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt >= retryPolicy.MaxRetries || !isRetryableRequestErr(err) || ctx.Err() != nil {
				return nil, err
			}
			if sleepErr := sleepForRetry(ctx, retryDelay(attempt, retryPolicy.BaseBackoff)); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}

		if isRetryableHTTPStatus(resp.StatusCode) && attempt < retryPolicy.MaxRetries {
			retryDelayHint := retryDelay(attempt, retryPolicy.BaseBackoff)
			if hinted, ok := retryAfterDelay(resp.Header.Get("Retry-After")); ok {
				if hinted > retryDelayHint {
					retryDelayHint = hinted
				}
			}

			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			bodyText := strings.TrimSpace(string(bodyBytes))
			if hinted, ok := retryAfterDelayFromBody(bodyText); ok {
				if hinted > retryDelayHint {
					retryDelayHint = hinted
				}
			}
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusTooManyRequests {
				min429Delay := retryPolicy.BaseBackoff
				if min429Delay < 500*time.Millisecond {
					min429Delay = 500 * time.Millisecond
				}
				if retryDelayHint < min429Delay {
					retryDelayHint = min429Delay
				}
			}

			if sleepErr := sleepForRetry(ctx, retryDelayHint); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}

		return resp, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("request failed for %s", endpoint)
}

func isRetryableHTTPStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode == http.StatusRequestTimeout || statusCode >= 500
}

func isRetryableRequestErr(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return true
		}
	}

	return false
}

func isRetryableReadOrDecodeErr(err error) bool {
	if isRetryableRequestErr(err) {
		return true
	}

	// Often surfaced when timeout/cancellation interrupts response body reads.
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}

	return false
}

func retryDelay(attempt int, base time.Duration) time.Duration {
	if base <= 0 {
		base = 300 * time.Millisecond
	}

	// Exponential backoff with a small deterministic jitter.
	delay := base * time.Duration(1<<attempt)
	delay += time.Duration((attempt+1)*25) * time.Millisecond
	maxDelay := 8 * time.Second
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func retryAfterDelay(retryAfterHeader string) (time.Duration, bool) {
	retryAfterHeader = strings.TrimSpace(retryAfterHeader)
	if retryAfterHeader == "" {
		return 0, false
	}

	if sec, err := strconv.Atoi(retryAfterHeader); err == nil && sec >= 0 {
		return time.Duration(sec) * time.Second, true
	}

	if t, err := http.ParseTime(retryAfterHeader); err == nil {
		delay := time.Until(t)
		if delay < 0 {
			return 0, false
		}
		return delay, true
	}

	return 0, false
}

func retryAfterDelayFromBody(bodyText string) (time.Duration, bool) {
	matches := waitForSecondsPattern.FindStringSubmatch(bodyText)
	if len(matches) != 2 {
		return 0, false
	}

	sec, err := strconv.Atoi(matches[1])
	if err != nil || sec < 0 {
		return 0, false
	}

	return time.Duration(sec) * time.Second, true
}

func sleepForRetry(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isETHGasRelayName(raw string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(raw)), "ethgas")
}

func normalizeAddress(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if !strings.HasPrefix(raw, "0x") && !strings.HasPrefix(raw, "0X") {
		return "", fmt.Errorf("address must start with 0x")
	}
	if len(raw) != 42 {
		return "", fmt.Errorf("address must be 42 chars, got %d", len(raw))
	}
	for i := 2; i < len(raw); i++ {
		c := raw[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return "", fmt.Errorf("address has non-hex character at position %d", i)
		}
	}
	return strings.ToLower(raw), nil
}

func buildETHGasRelaySet(relays []RelayClient) map[string]struct{} {
	out := make(map[string]struct{}, len(relays))
	for _, relay := range relays {
		if !relay.IsETHGas {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(relay.Name))
		if key == "" {
			continue
		}
		out[key] = struct{}{}
	}
	return out
}

func tagETHGasModes(results []SlotAnalysis, ethGasRelaySet map[string]struct{}, ethGasPoolAddr string) {
	for i := range results {
		mode := ""
		for _, relay := range results[i].WinningRelays {
			if _, ok := ethGasRelaySet[strings.ToLower(strings.TrimSpace(relay))]; ok {
				mode = "ETHGasRelay"
				break
			}
		}
		if mode == "" && ethGasPoolAddr != "" && strings.EqualFold(results[i].BeaconFeeRecipient, ethGasPoolAddr) {
			mode = "ETHGasExternal"
		}
		results[i].ETHGasMode = mode
	}
}

func printSlotSummary(results []SlotAnalysis) {
	fmt.Println("=== Slot Winner Summary ===")
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "SLOT\tBEACON_BLOCK_HASH\tBEACON_FEE_RECIPIENT\tETHGAS_MODE\tWINNING_RELAYS\tWINNING_BID_ETH\tWIN_BID_MS_INTO_SLOT\tMAX_BID_ETH\tMAX_BID_MS_INTO_SLOT\tBID_COUNT\tNOTES")

	for _, r := range results {
		winningETH := "-"
		if r.WinningValueKnown {
			winningETH = fmt.Sprintf("%.6f", r.WinningValueETH)
		}

		winningMS := "-"
		if r.WinningMSInto != nil {
			winningMS = strconv.FormatInt(*r.WinningMSInto, 10)
		}

		maxBidETH := "-"
		if r.MaxBidKnown {
			maxBidETH = fmt.Sprintf("%.6f", r.MaxBidETH)
		}

		maxBidMS := "-"
		if r.MaxBidMSInto != nil {
			maxBidMS = strconv.FormatInt(*r.MaxBidMSInto, 10)
		}

		blockHash := r.BeaconBlockHash
		if blockHash == "" {
			blockHash = "-"
		}
		feeRecipient := r.BeaconFeeRecipient
		if feeRecipient == "" {
			feeRecipient = "-"
		}
		ethGasMode := r.ETHGasMode
		if ethGasMode == "" {
			ethGasMode = "-"
		}

		winner := r.WinningRelay
		if winner == "" {
			winner = "-"
		}

		notes := "-"
		if len(r.Notes) > 0 {
			notes = strings.Join(r.Notes, " | ")
		}

		fmt.Fprintf(
			w,
			"%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
			r.Slot,
			blockHash,
			feeRecipient,
			ethGasMode,
			winner,
			winningETH,
			winningMS,
			maxBidETH,
			maxBidMS,
			r.TotalBids,
			notes,
		)
	}

	_ = w.Flush()
}

func printLatenessStats(results []SlotAnalysis, bucketMS int64, allRelayBids bool) {
	buckets := map[int64]*latenessAgg{}

	totalCount := 0
	totalETH := 0.0
	totalWins := 0
	skippedNoTimestamp := 0

	for _, slotResult := range results {
		for _, bid := range slotResult.Bids {
			if !bid.HasTimestamp {
				skippedNoTimestamp++
				continue
			}
			key := bucketStart(bid.MSIntoSlot, bucketMS)
			if _, ok := buckets[key]; !ok {
				buckets[key] = &latenessAgg{
					Slots:        map[uint64]struct{}{},
					WinningSlots: map[uint64]struct{}{},
				}
			}

			buckets[key].Count++
			buckets[key].TotalETH += bid.ValueETH
			buckets[key].Slots[bid.Slot] = struct{}{}
			if buckets[key].Count == 1 || bid.ValueETH > buckets[key].MaxBidETH {
				buckets[key].MaxBidETH = bid.ValueETH
			}
			if bid.IsWinningBid {
				buckets[key].WinCount++
				buckets[key].WinningTotalETH += bid.ValueETH
				buckets[key].WinningSlots[bid.Slot] = struct{}{}
				totalWins++
			}

			totalCount++
			totalETH += bid.ValueETH
		}
	}

	title := "=== Lateness vs Reward (winning bids only) ==="
	if allRelayBids {
		title = "=== Lateness vs Reward (all bids) ==="
	}
	fmt.Println(title)
	if totalCount == 0 {
		if skippedNoTimestamp > 0 {
			fmt.Printf("No timestamped bids returned in selected range (skipped %d bids without timestamp_ms).\n", skippedNoTimestamp)
			return
		}
		fmt.Println("No bids returned in selected range.")
		return
	}

	keys := make([]int64, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "BUCKET_MS_FROM_SLOT_START\tBID_COUNT\tSLOT_COUNT\tAVG_REWARD_ETH\tMEAN_BID_ETH\tMAX_BID_ETH\tWIN_SLOT_COUNT\tWIN_RATE")
	for _, k := range keys {
		agg := buckets[k]
		slotCount := len(agg.Slots)
		winSlotCount := len(agg.WinningSlots)
		avgReward := "-"
		if agg.WinCount > 0 {
			avgReward = fmt.Sprintf("%.6f", agg.WinningTotalETH/float64(agg.WinCount))
		}
		meanBid := agg.TotalETH / float64(agg.Count)
		winRate := 0.0
		if slotCount > 0 {
			winRate = 100.0 * float64(winSlotCount) / float64(slotCount)
		}
		label := fmt.Sprintf("[%d,%d)", k, k+bucketMS)
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%.6f\t%.6f\t%d\t%.2f%%\n", label, agg.Count, slotCount, avgReward, meanBid, agg.MaxBidETH, winSlotCount, winRate)
	}
	_ = w.Flush()

	overallAvg := totalETH / float64(totalCount)
	fmt.Printf("\nTotal bids: %d\n", totalCount)
	fmt.Printf("Overall avg reward (ETH): %.6f\n", overallAvg)
	fmt.Printf("Winning bids captured in relay traces: %d\n", totalWins)
	if skippedNoTimestamp > 0 {
		fmt.Printf("Skipped bids without timestamp_ms: %d\n", skippedNoTimestamp)
	}
}

func bucketStart(ms int64, bucketSize int64) int64 {
	if ms >= 0 {
		return (ms / bucketSize) * bucketSize
	}
	return -(((-ms + bucketSize - 1) / bucketSize) * bucketSize)
}

func printAllBids(results []SlotAnalysis) {
	type row struct {
		SlotAnalysisIndex int
		Bid               BidSample
	}

	rows := make([]row, 0)
	for i, result := range results {
		for _, bid := range result.Bids {
			rows = append(rows, row{SlotAnalysisIndex: i, Bid: bid})
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Bid.Slot != rows[j].Bid.Slot {
			return rows[i].Bid.Slot < rows[j].Bid.Slot
		}
		if rows[i].Bid.TimestampMS != rows[j].Bid.TimestampMS {
			return rows[i].Bid.TimestampMS < rows[j].Bid.TimestampMS
		}
		return rows[i].Bid.Relay < rows[j].Bid.Relay
	})

	fmt.Println("=== All Bids ===")
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "SLOT\tRELAY\tBLOCK_HASH\tVALUE_ETH\tTIMESTAMP_MS\tMS_INTO_SLOT\tWINNING_BID")
	for _, row := range rows {
		ts := strconv.FormatUint(row.Bid.TimestampMS, 10)
		msInto := strconv.FormatInt(row.Bid.MSIntoSlot, 10)
		if !row.Bid.HasTimestamp {
			ts = "-"
			msInto = "-"
		}
		fmt.Fprintf(
			w,
			"%d\t%s\t%s\t%.6f\t%s\t%s\t%t\n",
			row.Bid.Slot,
			row.Bid.Relay,
			row.Bid.BlockHash,
			row.Bid.ValueETH,
			ts,
			msInto,
			row.Bid.IsWinningBid,
		)
	}
	_ = w.Flush()
}

func printETHGasModeStats(results []SlotAnalysis, ethGasPoolAddr string) {
	fmt.Println("=== ETHGas Mode Stats ===")
	totalBlocks := 0
	ethGasRelayBlocks := 0
	ethGasExternalBlocks := 0

	for _, result := range results {
		if !result.HasBeaconBlock {
			continue
		}
		totalBlocks++
		switch result.ETHGasMode {
		case "ETHGasRelay":
			ethGasRelayBlocks++
		case "ETHGasExternal":
			ethGasExternalBlocks++
		}
	}

	if totalBlocks == 0 {
		fmt.Println("No canonical beacon blocks found in the selected range.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "MODE\tBLOCK_COUNT\tPERCENT_OF_BLOCKS")
	fmt.Fprintf(w, "ETHGasRelay\t%d\t%.2f%%\n", ethGasRelayBlocks, 100.0*float64(ethGasRelayBlocks)/float64(totalBlocks))
	fmt.Fprintf(w, "ETHGasExternal\t%d\t%.2f%%\n", ethGasExternalBlocks, 100.0*float64(ethGasExternalBlocks)/float64(totalBlocks))
	fmt.Fprintf(w, "ETHGas (total)\t%d\t%.2f%%\n", ethGasRelayBlocks+ethGasExternalBlocks, 100.0*float64(ethGasRelayBlocks+ethGasExternalBlocks)/float64(totalBlocks))
	_ = w.Flush()

	fmt.Printf("\nTotal canonical blocks: %d\n", totalBlocks)
	if ethGasPoolAddr == "" {
		fmt.Println("ETHGasExternal detection disabled: set -ethgas-pool-address to enable.")
	} else {
		fmt.Printf("ETHGas pool fee recipient: %s\n", ethGasPoolAddr)
	}
}

func printPerRelayBidStats(results []SlotAnalysis) {
	type relayAgg struct {
		Count    int
		TotalETH float64
		MaxETH   float64
	}

	aggs := map[string]*relayAgg{}
	for _, slotResult := range results {
		for _, bid := range slotResult.Bids {
			relay := strings.TrimSpace(bid.Relay)
			if relay == "" {
				relay = "-"
			}
			agg, ok := aggs[relay]
			if !ok {
				agg = &relayAgg{}
				aggs[relay] = agg
			}
			agg.Count++
			agg.TotalETH += bid.ValueETH
			if agg.Count == 1 || bid.ValueETH > agg.MaxETH {
				agg.MaxETH = bid.ValueETH
			}
		}
	}

	fmt.Println("=== Per-Relay Bid Stats ===")
	if len(aggs) == 0 {
		fmt.Println("No bids returned in selected range.")
		return
	}

	names := make([]string, 0, len(aggs))
	for name := range aggs {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		a := strings.ToLower(names[i])
		b := strings.ToLower(names[j])
		if a != b {
			return a < b
		}
		return names[i] < names[j]
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "RELAY\tBID_COUNT\tAVG_BID_ETH\tMEAN_BID_ETH\tMAX_BID_ETH")
	for _, name := range names {
		agg := aggs[name]
		mean := agg.TotalETH / float64(agg.Count)
		fmt.Fprintf(w, "%s\t%d\t%.6f\t%.6f\t%.6f\n", name, agg.Count, mean, mean, agg.MaxETH)
	}
	_ = w.Flush()
}
