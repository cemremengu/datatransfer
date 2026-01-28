package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const (
	defaultBatchSize         = 5000
	defaultProgressEveryRows = int64(1_000_000)
)

type config struct {
	databaseURL       string
	selectQuery       string
	destTable         pgx.Identifier
	batchSize         int
	progressEveryRows int64
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	poolConfig, err := pgxpool.ParseConfig(cfg.databaseURL)
	if err != nil {
		log.Fatalf("Failed to parse connection string: %v", err)
	}

	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	log.Println("Connected to database")

	if err := transferData(ctx, pool, cfg); err != nil {
		log.Fatalf("Transfer failed: %v", err)
	}

	log.Println("Data transfer completed successfully")
}

func loadConfig() (config, error) {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return config{}, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	selectQuery := strings.TrimSpace(os.Getenv("SELECT_QUERY"))
	if selectQuery == "" {
		return config{}, fmt.Errorf("SELECT_QUERY environment variable is required")
	}

	destTableRaw := strings.TrimSpace(os.Getenv("DEST_TABLE"))
	if destTableRaw == "" {
		return config{}, fmt.Errorf("DEST_TABLE environment variable is required (e.g. pm.snmp_metrics_interface)")
	}
	destTable, err := parsePgIdentifier(destTableRaw)
	if err != nil {
		return config{}, fmt.Errorf("invalid DEST_TABLE: %w", err)
	}

	batchSize := defaultBatchSize
	if raw := strings.TrimSpace(os.Getenv("BATCH_SIZE")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return config{}, fmt.Errorf("invalid BATCH_SIZE: %q", raw)
		}
		batchSize = v
	}

	progressEveryRows := defaultProgressEveryRows
	if raw := strings.TrimSpace(os.Getenv("PROGRESS_EVERY_ROWS")); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v <= 0 {
			return config{}, fmt.Errorf("invalid PROGRESS_EVERY_ROWS: %q", raw)
		}
		progressEveryRows = v
	}

	return config{
		databaseURL:       databaseURL,
		selectQuery:       selectQuery,
		destTable:         destTable,
		batchSize:         batchSize,
		progressEveryRows: progressEveryRows,
	}, nil
}

func parsePgIdentifier(raw string) (pgx.Identifier, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.Trim(raw, `"`)
	raw = strings.Trim(raw, `'`)
	if raw == "" {
		return nil, fmt.Errorf("empty identifier")
	}

	parts := strings.Split(raw, ".")
	switch len(parts) {
	case 1:
		return pgx.Identifier{"public", strings.TrimSpace(parts[0])}, nil
	case 2:
		schema := strings.TrimSpace(parts[0])
		table := strings.TrimSpace(parts[1])
		if schema == "" || table == "" {
			return nil, fmt.Errorf("expected schema.table")
		}
		return pgx.Identifier{schema, table}, nil
	default:
		return nil, fmt.Errorf("expected table or schema.table")
	}
}

func transferData(ctx context.Context, pool *pgxpool.Pool, cfg config) error {
	startTime := time.Now()

	log.Printf("Destination table: %s", strings.Join(cfg.destTable, "."))
	log.Printf("Select query: %s", cfg.selectQuery)
	log.Printf("Batch size: %d", cfg.batchSize)

	rows, columns, err := queryRows(ctx, pool, cfg.selectQuery)
	if err != nil {
		return fmt.Errorf("failed to execute select query: %w", err)
	}
	defer rows.Close()

	log.Printf("Detected columns (%d): %s", len(columns), strings.Join(columns, ", "))

	batch := make([][]any, 0, cfg.batchSize)
	var processedCount int64

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}

		batch = append(batch, values)
		if len(batch) < cfg.batchSize {
			continue
		}

		if err := insertBatch(ctx, pool, cfg.destTable, columns, batch); err != nil {
			log.Printf("failed to insert batch: %s", err.Error())
		}
		processedCount += int64(len(batch))
		batch = batch[:0]

		if processedCount%cfg.progressEveryRows == 0 {
			elapsed := time.Since(startTime)
			rate := float64(processedCount) / elapsed.Seconds()
			log.Printf("Processed: %d rows - Rate: %.0f rows/sec", processedCount, rate)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if len(batch) > 0 {
		if err := insertBatch(ctx, pool, cfg.destTable, columns, batch); err != nil {
			log.Printf("failed to insert final batch: %s", err.Error())
		}
		processedCount += int64(len(batch))
	}

	log.Printf("Transfer complete. Total: %d rows in %v", processedCount, time.Since(startTime))
	return nil
}

func queryRows(ctx context.Context, pool *pgxpool.Pool, selectQuery string) (pgx.Rows, []string, error) {
	rows, err := pool.Query(ctx, selectQuery)
	if err != nil {
		return nil, nil, err
	}
	columns := make([]string, 0, len(rows.FieldDescriptions()))
	for _, fd := range rows.FieldDescriptions() {
		columns = append(columns, string(fd.Name))
	}
	if len(columns) == 0 {
		rows.Close()
		return nil, nil, fmt.Errorf("select query returned zero columns")
	}
	return rows, columns, nil
}

func insertBatch(ctx context.Context, pool *pgxpool.Pool, destTable pgx.Identifier, columns []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	copyCount, err := pool.CopyFrom(
		ctx,
		destTable,
		columns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return err
	}

	if copyCount != int64(len(rows)) {
		return fmt.Errorf("expected to insert %d rows, but inserted %d", len(rows), copyCount)
	}

	return nil
}
