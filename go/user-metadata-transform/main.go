package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

type inputMetadata struct {
	Version             string `json:"__version"`
	ClipAvgSpeed        string `json:"clip_avg_speed"`
	ClipDurationSeconds string `json:"clip_duration_seconds"`
	ClipEndUTC          string `json:"clip_end_utc"`
	ClipStartUTC        string `json:"clip_start_utc"`
	LocationCity        string `json:"location__city"`
	LocationCountry     string `json:"location__country"`
	LocationTimezone    string `json:"location__local_timezone"`
	LocationZone        string `json:"location__zone"`
	ReferenceID         string `json:"reference_id"`
	TripID              string `json:"trip_id"`
	VehicleCamera       string `json:"vehicle__camera_version"`
	VehicleGPS          string `json:"vehicle__gps_version"`
	VehicleAgeDays      string `json:"vehicle__vehicle_age_days"`
	VehicleID           string `json:"vehicle__vehicle_id"`
	VehicleModel        string `json:"vehicle__vehicle_model"`
	WeatherCloudCover   string `json:"weather__cloud_cover"`
	WeatherPrecipMM     string `json:"weather__precipitation_mm"`
	WeatherTempC        string `json:"weather__temperature_c"`
	WeatherTimeOfDay    string `json:"weather__time_of_day"`
	WeatherIcon         string `json:"weather__weather_icon"`
}

type outputMetadata struct {
	ReferenceID  string       `json:"reference_id"`
	Version      int          `json:"version"`
	OutputPath   string       `json:"output_path"`
	Supplemental supplemental `json:"supplemental"`
}

type supplemental struct {
	ClipAvgSpeed        float64  `json:"clip_avg_speed"`
	ClipDurationSeconds int      `json:"clip_duration_seconds"`
	ClipEndUTC          string   `json:"clip_end_utc"`
	ClipStartUTC        string   `json:"clip_start_utc"`
	TripID              string   `json:"trip_id"`
	Location            location `json:"location"`
	Vehicle             vehicle  `json:"vehicle"`
	Weather             weather  `json:"weather"`
}

type location struct {
	City          string `json:"city"`
	Country       string `json:"country"`
	LocalTimezone string `json:"local_timezone"`
	Zone          string `json:"zone"`
}

type vehicle struct {
	CameraVersion  string `json:"camera_version"`
	GPSVersion     string `json:"gps_version"`
	VehicleAgeDays int    `json:"vehicle_age_days"`
	VehicleID      string `json:"vehicle_id"`
	VehicleModel   string `json:"vehicle_model"`
}

type weather struct {
	CloudCover      float64 `json:"cloud_cover"`
	PrecipitationMM float64 `json:"precipitation_mm"`
	TemperatureC    float64 `json:"temperature_c"`
	TimeOfDay       string  `json:"time_of_day"`
	WeatherIcon     string  `json:"weather_icon"`
}

func main() {
	limit := flag.Int("limit", 0, "limit number of rows read from parquet (0 means no limit)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [--limit N] <parquet-path>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		os.Exit(2)
	}

	parquetPath := args[0]
	if err := run(parquetPath, *limit); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(parquetPath string, limit int) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

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

	for rows.Next() {
		var key string
		var raw any
		if err := rows.Scan(&key, &raw); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		// build up metadata
		out, err := buildMetadataFromRaw(raw)
		if err != nil {
			return fmt.Errorf("build metadata from raw for key %q: %w", key, err)
		}

		trimmedKey := strings.TrimPrefix(key, "v3/")
		outputPath := fmt.Sprintf("%s.metadata.json", trimmedKey)
		fmt.Printf("Will write %s to %s", key, outputPath)

		if err := encoder.Encode(out); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	return nil
}

