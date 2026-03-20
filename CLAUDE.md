# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Football scouting data pipeline. Fetches player/team/league/season data from the SportMonks API (v3) and stores it in a PostgreSQL database (AWS RDS). A separate feature layer queries the DB for analytics.

## Environment Setup

Requires a `.env` file in the working directory with:
```
SPORTMONKS_API_TOKEN=...
T_DB_NAME=...
T_DB_HOST=...
T_DB_PORT=...
T_DB_PASSWORD=...
```

## Commands

```bash
# Initialize/reset DB tables (destructive — drops and recreates all tables)
python srcRaw/db_table_init.py

# Test DB connection
python srcRaw/dbhelper.py

# Run full data ingestion (edit league_ids in the file first)
python srcRaw/main.py

# Test API helpers directly
python srcRaw/funcHelper.py
```

There are no automated tests or package manager config files. Dependencies: `psycopg2`, `requests`, `pandas`.

## Architecture

```
srcRaw/   ← data ingestion pipeline (API → DB)
srcFeat/  ← analytics layer (DB → DataFrames)
```

### srcRaw (ingestion)

- `env.py` — minimal `.env` loader; must call `env.load()` before `env.get()`
- `HTTPHelper.py` — `send_request(url)` with in-memory URL-keyed cache (one cache per process lifetime)
- `funcHelper.py` — core logic: URL builders, API response parsers, insert helpers, and all caching logic
- `dbhelper.py` — PostgreSQL connection + INSERT statements; imports helpers from `funcHelper`
- `db_table_init.py` — one-time schema setup: `leagues → seasons → teams → players → player_season_stats`
- `main.py` — entry point; set `league_ids`, then calls `build_all_description_tables()` which orchestrates the full pipeline

### Data flow in `main.py`

League → most recent season → teams in season → squad player IDs → per-player: insert description + upload all historical season stats

### Caching pattern

All expensive lookups (API calls, repeated DB reads) use a `caches` dict passed through the call stack. Cache keys are defined in `make_caches()`. This dict is **not** persisted between runs.

### srcFeat (analytics)

- `queries.py` — pandas `pull_*` functions over the DB tables; imports `connect_db` from `srcRaw.dbhelper`
- `main.py` — stub entry point importing from `queries.py`

## DB Schema

Five tables with foreign key chain: `leagues → seasons → teams → players → player_season_stats`

`player_season_stats` primary key is `(player_id, season_id, team_id)` — all inserts use `ON CONFLICT DO NOTHING`.

## Notes

- `league_ids` in `srcRaw/main.py` must be set manually before running ingestion
- Commits happen in batches of 10 players (`BATCHSIZE`); errors on a single player trigger a rollback and continue
- `funcHelper.py` contains duplicate `get_teams_for_season` definition (lines 143 and 155) — the second one wins
