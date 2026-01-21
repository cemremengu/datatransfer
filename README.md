# Data Transfer Script

A high-performance Go tool that runs a configurable `SELECT` query and bulk-loads the result into a destination table using Postgres `COPY`.

## Features

- Connection pooling for optimal performance
- Bulk insert via `COPY FROM` for speed
- Environment-driven configuration via `.env`
- Progress and timing logs

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
```

## Configuration

Required environment variables:

- `DATABASE_URL`: Postgres connection string
- `DEST_TABLE`: Destination table (`table` or `schema.table`; `table` defaults to `public`)
- `SELECT_QUERY`: A `SELECT ...` query whose output columns match the destination table columns (use aliases if needed)

Optional environment variables:

- `BATCH_SIZE` (default: 5000): Number of rows per `COPY` batch
- `PROGRESS_EVERY_ROWS` (default: 1000000): Log progress every N rows

## Output

The script logs:
- Database connection status
- Destination table and query
- Detected result columns
- Rows processed and overall time
- Any errors encountered
