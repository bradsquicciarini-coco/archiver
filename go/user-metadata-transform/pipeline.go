package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/schollz/progressbar/v3"
)

func runPipeline(parquetPath string, limit int, uploader *blobUploader) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	total, err := countRows(db, parquetPath, limit)
	if err != nil {
		return fmt.Errorf("count rows: %w", err)
	}
	var bar *progressbar.ProgressBar
	if total > 0 {
		bar = progressbar.NewOptions(total, progressbar.OptionSetDescription("processing"))
	}

	query := "SELECT key, user_metadata FROM read_parquet(?)"
	if limit > 0 {
		query += " LIMIT ?"
	}

	var rows *sql.Rows
	if limit > 0 {
		rows, err = db.Query(query, parquetPath, limit)
	} else {
		rows, err = db.Query(query, parquetPath)
	}
	if err != nil {
		return fmt.Errorf("query parquet: %w", err)
	}
	defer rows.Close()

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)

	var pool *uploadPool
	if uploader != nil {
		pool = newUploadPool(uploader)
		defer pool.cancel()
	}

	for rows.Next() {
		if pool != nil {
			if err := pool.Err(); err != nil {
				return fmt.Errorf("upload to azure: %w", err)
			}
		}

		var key string
		var raw any
		if err := rows.Scan(&key, &raw); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		out, err := buildMetadataFromRaw(raw)
		if err != nil {
			return fmt.Errorf("build metadata from raw for key %q: %w", key, err)
		}

		trimmedKey := strings.TrimPrefix(key, "v3/")
		outputPath := fmt.Sprintf("%s.metadata.json", trimmedKey)

		if pool != nil {
			payload, err := json.Marshal(out)
			if err != nil {
				return fmt.Errorf("marshal output: %w", err)
			}
			if err := pool.Submit(uploadJob{path: outputPath, payload: payload}); err != nil {
				return fmt.Errorf("upload to azure: %w", err)
			}
		} else {
			if err := encoder.Encode(out); err != nil {
				return fmt.Errorf("write output: %w", err)
			}
		}

		if bar != nil {
			_ = bar.Add(1)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	if pool != nil {
		if err := pool.CloseAndWait(); err != nil {
			return fmt.Errorf("upload to azure: %w", err)
		}
	}

	return nil
}

func countRows(db *sql.DB, parquetPath string, limit int) (int, error) {
	if limit > 0 {
		return limit, nil
	}

	var total int
	if err := db.QueryRow("SELECT count(*) FROM read_parquet(?)", parquetPath).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}
