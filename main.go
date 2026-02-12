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

type Config struct {
	BeaconEndpoint string
	RelayArg       string
	StartSlotArg   string
	EndSlotArg     string
	StartTimeArg   string
	EndTimeArg     string
	Concurrency    int
	HTTPTimeout    time.Duration
	BucketMS       int64
	PrintAllBids   bool
	AllowInferred  bool
	MaxRetries     int
	RetryBackoff   time.Duration
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
	HTTPClient *http.Client
	Retry      RetryPolicy
}

type SlotAnalysis struct {
	Slot              uint64
	BeaconBlockHash   string
	WinningRelay      string
	WinningBlockHash  string
	WinningValueETH   float64
	WinningValueKnown bool
	WinningMSInto     *int64
	TotalBids         int
	Bids              []BidSample
	Notes             []string
}

type BidSample struct {
	Slot         uint64
	Relay        string
	BlockHash    string
	ValueWei     string
	ValueETH     float64
	TimestampMS  uint64
	MSIntoSlot   int64
	IsWinningBid bool
}

type winnerCandidate struct {
	Relay string
	Trace BidTrace
}

type latenessAgg struct {
	Count    int
	TotalETH float64
	WinCount int
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
					BlockHash string `json:"block_hash"`
				} `json:"execution_payload"`
				ExecutionPayloadHeader struct {
					BlockHash string `json:"block_hash"`
				} `json:"execution_payload_header"`
			} `json:"body"`
		} `json:"message"`
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
	flag.DurationVar(&cfg.HTTPTimeout, "http-timeout", 10*time.Second, "HTTP timeout per request")
	flag.Int64Var(&cfg.BucketMS, "bucket-ms", 100, "Bucket size in milliseconds for lateness stats")
	flag.BoolVar(&cfg.PrintAllBids, "print-bids", false, "Print every bid row in addition to summary tables")
	flag.BoolVar(&cfg.AllowInferred, "allow-inferred-winner", false, "Allow inferred winner attribution when canonical matching is unavailable")
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
	if cfg.BucketMS <= 0 {
		return errors.New("-bucket-ms must be >= 1")
	}
	if cfg.MaxRetries < 0 {
		return errors.New("-max-retries must be >= 0")
	}
	if cfg.RetryBackoff <= 0 {
		return errors.New("-retry-backoff must be > 0")
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
		cfg.AllowInferred,
		retryPolicy,
	)
	sort.Slice(results, func(i, j int) bool { return results[i].Slot < results[j].Slot })

	printSlotSummary(results)
	fmt.Println()
	printLatenessStats(results, cfg.BucketMS)

	if cfg.PrintAllBids {
		fmt.Println()
		printAllBids(results)
	}

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
				resultsCh <- analyzeSingleSlot(ctx, httpClient, beaconEndpoint, relays, spec, slot, allowInferred, retryPolicy)
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
	allowInferred bool,
	retryPolicy RetryPolicy,
) SlotAnalysis {
	result := SlotAnalysis{Slot: slot}
	slotStartMS := spec.GenesisTime.UnixMilli() + int64(slot)*int64(spec.SecondsPerSlot)*1000

	beaconBlockHash, hasBeaconBlock, err := fetchBeaconExecutionBlockHash(ctx, httpClient, beaconEndpoint, slot, retryPolicy)
	if err != nil {
		result.Notes = append(result.Notes, "beacon block lookup failed: "+err.Error())
	}
	if hasBeaconBlock {
		result.BeaconBlockHash = beaconBlockHash
	}

	candidates := make([]winnerCandidate, 0, len(relays))

	for _, relay := range relays {
		bids, err := relay.fetchBidTraces(ctx, builderBidsPath, slot)
		if err != nil {
			result.Notes = append(result.Notes, fmt.Sprintf("builder bids failed for %s: %v", relay.Name, err))
			continue
		}

		delivered, err := relay.fetchBidTraces(ctx, deliveredBidsPath, slot)
		if err != nil {
			result.Notes = append(result.Notes, fmt.Sprintf("delivered bids failed for %s: %v", relay.Name, err))
		} else {
			for _, trace := range delivered {
				candidates = append(candidates, winnerCandidate{Relay: relay.Name, Trace: trace})
			}
		}

		for _, trace := range bids {
			valueETH, err := weiToETH(trace.Value.String())
			if err != nil {
				result.Notes = append(result.Notes, fmt.Sprintf("invalid value for relay %s block %s: %v", relay.Name, trace.BlockHash, err))
				continue
			}

			ts := trace.TimestampMS.Uint64()
			result.Bids = append(result.Bids, BidSample{
				Slot:        slot,
				Relay:       relay.Name,
				BlockHash:   strings.TrimSpace(trace.BlockHash),
				ValueWei:    trace.Value.String(),
				ValueETH:    valueETH,
				TimestampMS: ts,
				MSIntoSlot:  int64(ts) - slotStartMS,
			})
		}
	}

	result.TotalBids = len(result.Bids)

	selected, note := chooseWinningCandidate(candidates, beaconBlockHash, allowInferred)
	if note != "" {
		result.Notes = append(result.Notes, note)
	}

	if selected != nil {
		result.WinningRelay = selected.Relay
		result.WinningBlockHash = strings.TrimSpace(selected.Trace.BlockHash)
		valueETH, err := weiToETH(selected.Trace.Value.String())
		if err == nil {
			result.WinningValueETH = valueETH
			result.WinningValueKnown = true
		} else {
			result.Notes = append(result.Notes, "winning value parse error: "+err.Error())
		}

		if selected.Trace.TimestampMS.Uint64() > 0 {
			ms := int64(selected.Trace.TimestampMS.Uint64()) - slotStartMS
			result.WinningMSInto = &ms
		}
	}

	matchingDeliveredToCanonical := 0
	if result.BeaconBlockHash != "" {
		matchingDeliveredToCanonical = countMatchingCandidatesByBlockHash(candidates, result.BeaconBlockHash)
	}

	if selected == nil && hasBeaconBlock && result.BeaconBlockHash != "" && matchingDeliveredToCanonical == 0 {
		result.WinningRelay = "self-built"
		result.WinningBlockHash = result.BeaconBlockHash
		result.Notes = append(result.Notes, "canonical block not found in relay delivered traces; attributed as self-built")
	}

	for i := range result.Bids {
		if result.WinningRelay == "" {
			break
		}

		if !strings.EqualFold(result.Bids[i].Relay, result.WinningRelay) {
			continue
		}
		if result.WinningBlockHash != "" && !strings.EqualFold(result.Bids[i].BlockHash, result.WinningBlockHash) {
			continue
		}

		result.Bids[i].IsWinningBid = true
		if result.WinningMSInto == nil {
			ms := result.Bids[i].MSIntoSlot
			result.WinningMSInto = &ms
		}
		break
	}

	if hasBeaconBlock && result.WinningRelay == "" && matchingDeliveredToCanonical == 0 {
		result.Notes = append(result.Notes, "no relay candidate matched this canonical beacon block")
	}

	return result
}

func chooseWinningCandidate(candidates []winnerCandidate, beaconBlockHash string, allowInferred bool) (*winnerCandidate, string) {
	if len(candidates) == 0 {
		return nil, ""
	}

	beaconBlockHash = strings.TrimSpace(beaconBlockHash)
	if beaconBlockHash == "" {
		if allowInferred {
			selected := highestValueCandidate(candidates)
			return &selected, "inferred winner: beacon block hash unavailable; selected highest value delivered trace"
		}
		return nil, "beacon block hash unavailable; winner relay cannot be attributed safely"
	}

	matching := make([]winnerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.EqualFold(strings.TrimSpace(candidate.Trace.BlockHash), beaconBlockHash) {
			matching = append(matching, candidate)
		}
	}

	if len(matching) == 1 {
		selected := matching[0]
		return &selected, ""
	}

	if len(matching) > 1 {
		if allowInferred {
			selected := highestValueCandidate(matching)
			return &selected, "inferred winner: multiple relays delivered canonical block hash; selected highest value trace"
		}
		return nil, "multiple relays delivered the canonical block hash; winner relay attribution is ambiguous"
	}

	return nil, "no relay delivered trace matched the canonical beacon block hash"
}

func countMatchingCandidatesByBlockHash(candidates []winnerCandidate, blockHash string) int {
	blockHash = strings.TrimSpace(blockHash)
	if blockHash == "" {
		return 0
	}

	count := 0
	for _, candidate := range candidates {
		if strings.EqualFold(strings.TrimSpace(candidate.Trace.BlockHash), blockHash) {
			count++
		}
	}

	return count
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

func fetchBeaconExecutionBlockHash(
	ctx context.Context,
	httpClient *http.Client,
	beaconEndpoint string,
	slot uint64,
	retryPolicy RetryPolicy,
) (string, bool, error) {
	endpoint := fmt.Sprintf("%s/eth/v2/beacon/blocks/%d", strings.TrimRight(beaconEndpoint, "/"), slot)
	resp, err := getWithRetry(ctx, httpClient, endpoint, retryPolicy)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", false, fmt.Errorf("beacon response status %d from %s: %s", resp.StatusCode, endpoint, strings.TrimSpace(string(body)))
	}

	out := beaconBlockResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", false, err
	}

	blockHash := strings.TrimSpace(out.Data.Message.Body.ExecutionPayload.BlockHash)
	if blockHash == "" {
		blockHash = strings.TrimSpace(out.Data.Message.Body.ExecutionPayloadHeader.BlockHash)
	}

	return blockHash, true, nil
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
				retryDelayHint = hinted
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
			_ = resp.Body.Close()

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

func printSlotSummary(results []SlotAnalysis) {
	fmt.Println("=== Slot Winner Summary ===")
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "SLOT\tBEACON_BLOCK_HASH\tWINNING_RELAY\tWINNING_BID_ETH\tWIN_BID_MS_INTO_SLOT\tBID_COUNT\tNOTES")

	for _, r := range results {
		winningETH := "-"
		if r.WinningValueKnown {
			winningETH = fmt.Sprintf("%.6f", r.WinningValueETH)
		}

		winningMS := "-"
		if r.WinningMSInto != nil {
			winningMS = strconv.FormatInt(*r.WinningMSInto, 10)
		}

		blockHash := r.BeaconBlockHash
		if blockHash == "" {
			blockHash = "-"
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
			"%d\t%s\t%s\t%s\t%s\t%d\t%s\n",
			r.Slot,
			blockHash,
			winner,
			winningETH,
			winningMS,
			r.TotalBids,
			notes,
		)
	}

	_ = w.Flush()
}

func printLatenessStats(results []SlotAnalysis, bucketMS int64) {
	buckets := map[int64]*latenessAgg{}

	totalCount := 0
	totalETH := 0.0
	totalWins := 0

	for _, slotResult := range results {
		for _, bid := range slotResult.Bids {
			key := bucketStart(bid.MSIntoSlot, bucketMS)
			if _, ok := buckets[key]; !ok {
				buckets[key] = &latenessAgg{}
			}

			buckets[key].Count++
			buckets[key].TotalETH += bid.ValueETH
			if bid.IsWinningBid {
				buckets[key].WinCount++
				totalWins++
			}

			totalCount++
			totalETH += bid.ValueETH
		}
	}

	fmt.Println("=== Lateness vs Reward (all bids) ===")
	if totalCount == 0 {
		fmt.Println("No bids returned in selected range.")
		return
	}

	keys := make([]int64, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "BUCKET_MS_FROM_SLOT_START\tBID_COUNT\tAVG_REWARD_ETH\tWIN_COUNT\tWIN_RATE")
	for _, k := range keys {
		agg := buckets[k]
		avg := agg.TotalETH / float64(agg.Count)
		winRate := 100.0 * float64(agg.WinCount) / float64(agg.Count)
		label := fmt.Sprintf("[%d,%d)", k, k+bucketMS)
		fmt.Fprintf(w, "%s\t%d\t%.6f\t%d\t%.2f%%\n", label, agg.Count, avg, agg.WinCount, winRate)
	}
	_ = w.Flush()

	overallAvg := totalETH / float64(totalCount)
	fmt.Printf("\nTotal bids: %d\n", totalCount)
	fmt.Printf("Overall avg reward (ETH): %.6f\n", overallAvg)
	fmt.Printf("Winning bids captured in relay traces: %d\n", totalWins)
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
		fmt.Fprintf(
			w,
			"%d\t%s\t%s\t%.6f\t%d\t%d\t%t\n",
			row.Bid.Slot,
			row.Bid.Relay,
			row.Bid.BlockHash,
			row.Bid.ValueETH,
			row.Bid.TimestampMS,
			row.Bid.MSIntoSlot,
			row.Bid.IsWinningBid,
		)
	}
	_ = w.Flush()
}
