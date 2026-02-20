# relay-observer

Query relay bid traces across a slot range (or time range), identify the winning block attribution, and compute reward stats by bid lateness.

## What it does

- Resolves slots from:
  - `-start-slot` and `-end-slot`, or
  - `-start-time` and `-end-time` (RFC3339 or unix timestamp)
- Collects bids in one of two modes:
  - default: only winning relay bids per slot
  - `-all-relay-bids`: all relay builder bids
- Pulls bid traces from each relay:
  - `builder_blocks_received`
  - `proposer_payload_delivered`
- Pulls canonical beacon block data for each slot from a beacon API endpoint.
- Tracks beacon `fee_recipient` per slot and classifies ETHGas mode as:
  - `ETHGasRelay`: winning relay name/URL contains `ethgas`
  - `ETHGasExternal`: non-ethgas winning relay, but beacon `fee_recipient` equals `-ethgas-pool-address`
- Attributes winner as:
  - matching relay (strict canonical `block_hash` match), or
  - `self-built` if canonical block hash exists and no relay-delivered trace matches
- Computes lateness-vs-reward stats for bids in-window.
- Prints:
  - slot winner summary (with `BEACON_FEE_RECIPIENT` and `ETHGAS_MODE`)
  - ETHGas mode percentages across canonical blocks
  - per-relay bid stats (`AVG`/`MEAN`/`MAX`)

## Prerequisites

- Go 1.22+
- Access to:
  - a beacon REST endpoint (for example Lighthouse/Prysm/Teku/Nimbus endpoint)
  - one or more relay APIs

## Install / Build

Build binary:

```bash
go build -o relay-observer .
```

Run without building:

```bash
go run .
```

## Usage

```bash
./relay-observer \
  -beacon-endpoint http://127.0.0.1:5052 \
  -relays "flashbots=https://boost-relay.flashbots.net,ultrasound=https://relay.ultrasound.money" \
  -start-slot 12000000 \
  -end-slot 12000100
```

Use time range instead of slot range:

```bash
./relay-observer \
  -beacon-endpoint http://127.0.0.1:5052 \
  -relays "flashbots=https://boost-relay.flashbots.net,ultrasound=https://relay.ultrasound.money" \
  -start-time 2026-02-11T00:00:00Z \
  -end-time 2026-02-11T01:00:00Z
```

## Options

```text
-all-relay-bids
    Collect all relay bids (default: only winning bids)
-allow-inferred-winner
    Allow inferred winner attribution when canonical matching is unavailable
-beacon-endpoint string
    Beacon REST API endpoint (required), e.g. http://127.0.0.1:5052
-bucket-ms int
    Bucket size in milliseconds for lateness stats (default 100)
-concurrency int
    Number of slots analyzed in parallel (default 8)
-relay-concurrency int
    Number of per-slot relay requests in parallel (default 4)
-end-slot string
    End slot (inclusive)
-end-time string
    End time (RFC3339 or unix seconds/millis)
-ethgas-pool-address string
    EthGasPool fee recipient address (0x...) used to classify ETHGasExternal blocks
-http-timeout duration
    HTTP timeout per request (default 10s)
-max-retries int
    Max retries for timeout/rate-limit/server errors (default 3)
-print-bids
    Print every bid row in addition to summary tables
-relays string
    Comma-separated relays as URL or name=url (required)
-retry-backoff duration
    Base exponential backoff for retries (default 300ms)
-start-slot string
    Start slot (inclusive)
-start-time string
    Start time (RFC3339 or unix seconds/millis)
```

## Winner attribution rules

- Default mode is strict:
  - winner is set only when relay-delivered trace(s) match canonical beacon `block_hash`.
  - if multiple relays delivered the same canonical block hash, all matching relays are reported.
  - for those multi-relay matches, winning reward uses the highest delivered value among the matching relays.
- If canonical block exists but no relay in your configured relay list matches:
  - winner is reported as `self-built`.
  - reward is sourced from beacon `block rewards` API when available (`/eth/v1/beacon/rewards/blocks/{slot}`).
- If `-allow-inferred-winner` is enabled:
  - when canonical matching is unavailable, winner can be inferred from highest delivered value trace.

## ETHGas mode detection

- `ETHGasRelay`:
  - slot winner includes a relay whose configured name or URL contains `ethgas` (case-insensitive)
- `ETHGasExternal`:
  - winner is not from an `ethgas` relay, and
  - beacon `fee_recipient` equals `-ethgas-pool-address`
- If `-ethgas-pool-address` is not set, `ETHGasExternal` detection is disabled.

## Bid collection mode

- Default mode (no `-all-relay-bids`):
  - only winning bid(s) are kept per slot for bid table and lateness/reward stats.
  - timing for those winning bids is sourced from relay `builder_blocks_received` for winning relay/block pairs (fallback to delivered traces if unavailable).
- `-all-relay-bids` enabled:
  - all relay builder bids are collected and used in bid table and lateness/reward stats.

## Retry behavior

- Retries apply to beacon and relay API GET calls.
- Retry triggers:
  - timeout/network timeout errors
  - HTTP `408`, `429`, and `5xx`
- Backoff:
  - exponential using `-retry-backoff` as base
  - respects `Retry-After` header when present
  - for `429` responses with body hints like `Wait for Ns`, that hint is also honored

## Notes

- `self-built` means "not matched in the provided relay list". If your relay list is incomplete, external relay blocks may also appear as `self-built`.
- All returned bids are considered for stats and attribution; no late-bid cutoff is applied.
- If a slot has no beacon block available from the endpoint, canonical winner attribution may be unavailable.
