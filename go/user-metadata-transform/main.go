package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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

		jsonText, err := normalizeJSONText(raw)
		if err != nil {
			return fmt.Errorf("normalize user_metadata: %w", err)
		}
		if jsonText == "" {
			continue
		}

		out, err := transformUserMetadata(jsonText)
		if err != nil {
			return fmt.Errorf("transform user_metadata: %w", err)
		}

		trimmedKey := strings.TrimPrefix(key, "v3/")
		out.OutputPath = fmt.Sprintf("%s.metadata.json", trimmedKey)

		if err := encoder.Encode(out); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	return nil
}

func normalizeJSONText(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

func transformUserMetadata(raw string) (outputMetadata, error) {
	var meta inputMetadata
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return outputMetadata{}, err
	}

	version, err := parseInt(meta.Version)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("parse __version %q: %w", meta.Version, err)
	}

	clipAvgSpeed, err := parseFloat(meta.ClipAvgSpeed)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("parse clip_avg_speed %q: %w", meta.ClipAvgSpeed, err)
	}

	clipDuration, err := parseInt(meta.ClipDurationSeconds)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("parse clip_duration_seconds %q: %w", meta.ClipDurationSeconds, err)
	}

	vehicleAgeDays, err := parseInt(meta.VehicleAgeDays)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("parse vehicle_age_days %q: %w", meta.VehicleAgeDays, err)
	}

	cloudCover, err := parseFloat(meta.WeatherCloudCover)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("parse weather__cloud_cover %q: %w", meta.WeatherCloudCover, err)
	}

	precipitationMM, err := parseFloat(meta.WeatherPrecipMM)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("parse weather__precipitation_mm %q: %w", meta.WeatherPrecipMM, err)
	}

	temperatureC, err := parseFloat(meta.WeatherTempC)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("parse weather__temperature_c %q: %w", meta.WeatherTempC, err)
	}

	out := outputMetadata{
		ReferenceID: meta.ReferenceID,
		Version:     version,
		Supplemental: supplemental{
			ClipAvgSpeed:        clipAvgSpeed,
			ClipDurationSeconds: clipDuration,
			ClipEndUTC:          meta.ClipEndUTC,
			ClipStartUTC:        meta.ClipStartUTC,
			TripID:              meta.TripID,
			Location: location{
				City:          meta.LocationCity,
				Country:       meta.LocationCountry,
				LocalTimezone: meta.LocationTimezone,
				Zone:          meta.LocationZone,
			},
			Vehicle: vehicle{
				CameraVersion:  meta.VehicleCamera,
				GPSVersion:     meta.VehicleGPS,
				VehicleAgeDays: vehicleAgeDays,
				VehicleID:      meta.VehicleID,
				VehicleModel:   meta.VehicleModel,
			},
			Weather: weather{
				CloudCover:      cloudCover,
				PrecipitationMM: precipitationMM,
				TemperatureC:    temperatureC,
				TimeOfDay:       meta.WeatherTimeOfDay,
				WeatherIcon:     meta.WeatherIcon,
			},
		},
	}

	return out, nil
}

func parseFloat(value string) (float64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("empty string")
	}
	return strconv.ParseFloat(trimmed, 64)
}

func parseInt(value string) (int, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("empty string")
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}
