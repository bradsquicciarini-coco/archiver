package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

func buildMetadataFromRaw(value any) (outputMetadata, error) {
	normalized, err := normalizeJSONText(value)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("normalize JSON text: %w", err)
	}

	meta, err := transformUserMetadata(normalized)
	if err != nil {
		return outputMetadata{}, fmt.Errorf("transform user metadata: %w", err)
	}

	return meta, nil
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
