# Data Transfer Script

A high-performance Go tool that runs a configurable `SELECT` query and bulk-loads the result into a destination table using Postgres `COPY`.

## Features

- Connection pooling for optimal performance
- Bulk insert via `COPY FROM` for speed
- Environment-driven configuration via `.env`
- Progress and timing logs
- Resumable transfers via `ROW_OFFSET`

## Setup

1. Install dependencies:

```bash
go mod download
```

2. Create a `.env` file based on `.env.example`:

```bash
cp .env.example .env
```

3. Update the `.env` file with your database credentials:

```
DATABASE_URL=postgres://username:password@host:port/database?sslmode=disable
DEST_TABLE=pm.snmp_metrics_interface
SELECT_QUERY=SELECT * FROM pm.snmp_metrics_interface_2 WHERE "timestamp" >= DATE_TRUNC('DAY', now()) - interval '1 days'
```

## Usage

Run the script:

```bash
go run main.go
```

Or build and run:

```bash
go build -o datatransfer.exe
./datatransfer.exe

# On Linux

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 LDFLAGS="-s -w" go build .
```

## Configuration

Required environment variables:

- `DATABASE_URL`: Postgres connection string
- `DEST_TABLE`: Destination table (`table` or `schema.table`; `table` defaults to `public`)
- `SELECT_QUERY`: A `SELECT ...` query whose output columns match the destination table columns (use aliases if needed)

Optional environment variables:

- `BATCH_SIZE` (default: 5000): Number of rows per `COPY` batch
- `PROGRESS_EVERY_ROWS` (default: 1000000): Log progress every N rows
- `ROW_OFFSET` (default: 0): Skip the first N rows of `SELECT_QUERY`; use it to resume an interrupted transfer

## Resuming a Transfer

If the transfer fails or is cancelled (Ctrl+C / `SIGTERM`), the script logs the row offset of the first row that was not written:

```
2026/10/02 10:30:16 Transfer stopped after 325000 rows. To resume, set ROW_OFFSET=325000
2026/10/02 10:30:16 Transfer cancelled: context canceled (resume with ROW_OFFSET=325000)
```

Rerun with the same `SELECT_QUERY` and set `ROW_OFFSET` to that value:

```bash
ROW_OFFSET=325000 go run main.go
```

or add it to your `.env`:

```
SELECT_QUERY=SELECT * FROM pm.snmp_metrics_interface_2 WHERE "timestamp" >= DATE_TRUNC('DAY', now()) - interval '1 days' ORDER BY "timestamp", id
ROW_OFFSET=325000
```

The query is run as `SELECT * FROM (<SELECT_QUERY>) AS src OFFSET <ROW_OFFSET>`, so:

- `SELECT_QUERY` **must have a deterministic `ORDER BY`** (on a unique key or combination of columns). Without it, Postgres can return the rows in a different order, and the offset would skip or duplicate rows.
- The source data covered by the query must not change between runs. A query relative to `now()` returns different rows later, so pin it to fixed timestamps when you need to resume (e.g. `"timestamp" >= '2026-10-01'`).
- Remove `ROW_OFFSET` (or set it to `0`) when you start a new transfer.

If one batch fails while the transfer keeps going, its row range and offset are logged so you can retry just that range:

```
failed to insert batch (rows 1990000-1994999, retry with ROW_OFFSET=1990000): ERROR: ...
```

## Output

The script logs:

- Database connection status
- Destination table and query
- Detected result columns
- Rows processed (with current row offset) and overall time
- The row offset to resume from on error or cancellation
- Any errors encountered
