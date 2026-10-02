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
	cursorName               = "datatransfer_cursor"
)

type config struct {
	databaseURL       string
	selectQuery       string
	destTable         pgx.Identifier
	batchSize         int
	fetchSize         int
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

	// DECLARE ... CURSOR FOR <query> does not accept a trailing semicolon.
	selectQuery := strings.TrimSpace(strings.TrimRight(strings.TrimSpace(os.Getenv("SELECT_QUERY")), ";"))
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

	fetchSize := batchSize
	if raw := strings.TrimSpace(os.Getenv("FETCH_SIZE")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return config{}, fmt.Errorf("invalid FETCH_SIZE: %q", raw)
		}
		fetchSize = v
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
		fetchSize:         fetchSize,
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
	log.Printf("Fetch size: %d", cfg.fetchSize)

	// The source query is read through a server-side cursor on its own
	// connection, so at most fetchSize rows are requested from the server at
	// a time. Batches are written through other pool connections.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Cursors only live inside a transaction.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	declare := fmt.Sprintf("DECLARE %s NO SCROLL CURSOR FOR %s", cursorName, cfg.selectQuery)
	if _, err := tx.Exec(ctx, declare); err != nil {
		return fmt.Errorf("failed to declare cursor for select query: %w", err)
	}

	fetch := fmt.Sprintf("FETCH FORWARD %d FROM %s", cfg.fetchSize, cursorName)
	batch := make([][]any, 0, cfg.batchSize)
	var columns []string
	var processedCount int64

	for {
		rows, err := tx.Query(ctx, fetch)
		if err != nil {
			return fmt.Errorf("failed to fetch from cursor: %w", err)
		}

		if columns == nil {
			columns, err = columnNames(rows)
			if err != nil {
				rows.Close()
				return err
			}
			log.Printf("Detected columns (%d): %s", len(columns), strings.Join(columns, ", "))
		}

		var fetched int
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return err
			}
			fetched++

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
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to read fetched rows: %w", err)
		}

		if fetched < cfg.fetchSize {
			break
		}
	}

	if len(batch) > 0 {
		if err := insertBatch(ctx, pool, cfg.destTable, columns, batch); err != nil {
			log.Printf("failed to insert final batch: %s", err.Error())
		}
		processedCount += int64(len(batch))
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to close cursor transaction: %w", err)
	}

	log.Printf("Transfer complete. Total: %d rows in %v", processedCount, time.Since(startTime))
	return nil
}

func columnNames(rows pgx.Rows) ([]string, error) {
	columns := make([]string, 0, len(rows.FieldDescriptions()))
	for _, fd := range rows.FieldDescriptions() {
		columns = append(columns, string(fd.Name))
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("select query returned zero columns")
	}
	return columns, nil
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
