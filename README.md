# relay-observer

Query relay bid traces across a slot range (or time range), identify the winning block attribution, and compute reward stats by bid lateness.

## What it does

- Resolves slots from:
  - `-start-slot` and `-end-slot`, or
  - `-start-time` and `-end-time` (RFC3339 or unix timestamp)
- Pulls bid traces from each relay:
  - `builder_blocks_received`
  - `proposer_payload_delivered`
- Pulls canonical beacon block data for each slot from a beacon API endpoint.
- Attributes winner as:
  - matching relay (strict canonical `block_hash` match), or
  - `self-built` if canonical block hash exists and no relay-delivered trace matches
- Computes lateness-vs-reward stats for bids in-window.

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
-allow-inferred-winner
    Allow inferred winner attribution when canonical matching is unavailable
-beacon-endpoint string
    Beacon REST API endpoint (required), e.g. http://127.0.0.1:5052
-bucket-ms int
    Bucket size in milliseconds for lateness stats (default 100)
-concurrency int
    Number of slots analyzed in parallel (default 8)
-end-slot string
    End slot (inclusive)
-end-time string
    End time (RFC3339 or unix seconds/millis)
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
  - winner is set only when a relay-delivered trace matches canonical beacon `block_hash`.
- If canonical block exists but no relay in your configured relay list matches:
  - winner is reported as `self-built`.
- If `-allow-inferred-winner` is enabled:
  - when canonical matching is unavailable/ambiguous, winner can be inferred from highest delivered value trace.

## Retry behavior

- Retries apply to beacon and relay API GET calls.
- Retry triggers:
  - timeout/network timeout errors
  - HTTP `408`, `429`, and `5xx`
- Backoff:
  - exponential using `-retry-backoff` as base
  - respects `Retry-After` header when present

## Notes

- `self-built` means "not matched in the provided relay list". If your relay list is incomplete, external relay blocks may also appear as `self-built`.
- All returned bids are considered for stats and attribution; no late-bid cutoff is applied.
- If a slot has no beacon block available from the endpoint, canonical winner attribution may be unavailable.
