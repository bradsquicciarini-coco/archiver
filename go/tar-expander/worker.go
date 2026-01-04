package main

import (
	"archive/tar"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb"
	"go.uber.org/zap"
)

type queueMessage struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

func runQueueWorker(ctx context.Context, logger *zap.Logger, sqsClient *sqs.Client, s3Client *s3.Client, uploader *manager.Uploader, queueURL, outBucket, outPrefix, skipPattern string, metadataByID map[string]map[string]string, dryRun bool) error {
	for {
		out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			return err
		}
		if len(out.Messages) == 0 {
			continue
		}
		for _, msg := range out.Messages {
			if msg.Body == nil {
				continue
			}
			item := queueMessage{}
			if err := json.Unmarshal([]byte(*msg.Body), &item); err != nil {
				logger.Warn("skipping message with invalid json", zap.Error(err))
				continue
			}
			if item.Bucket == "" || item.Key == "" {
				logger.Warn("skipping message missing bucket/key")
				continue
			}
			logger.Info("processing tar",
				zap.String("bucket", item.Bucket),
				zap.String("key", item.Key),
				zap.Bool("dryrun", dryRun),
			)
			targetBucket := outBucket
			if targetBucket == "" {
				targetBucket = item.Bucket
			}
			if err := expandTar(ctx, logger, s3Client, uploader, item.Bucket, item.Key, targetBucket, outPrefix, skipPattern, metadataByID, dryRun); err != nil {
				logger.Error("expand failed", zap.Error(err))
				continue
			}
			if dryRun {
				continue
			}
			_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &queueURL,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				return err
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func expandTar(ctx context.Context, logger *zap.Logger, client *s3.Client, uploader *manager.Uploader, bucket, key, outBucket, outPrefix, skipPattern string, metadataByID map[string]map[string]string, dryRun bool) error {
	obj, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return err
	}
	defer obj.Body.Close()

	return expandTarReader(ctx, logger.Sugar(), uploader, obj.Body, outBucket, outPrefix, skipPattern, metadataByID, dryRun)
}

func expandLocalTar(ctx context.Context, logger *zap.SugaredLogger, uploader *manager.Uploader, tarPath, outBucket, outPrefix, skipPattern string, metadataByID map[string]map[string]string, dryRun bool) error {
	if tarPath == "" {
		return fmt.Errorf("local tar path is required")
	}
	logger.Infow("starting local tar expansion",
		"path", tarPath,
		"bucket", outBucket,
		"prefix", outPrefix,
		"dryrun", dryRun,
	)
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return expandTarReader(ctx, logger, uploader, f, outBucket, outPrefix, skipPattern, metadataByID, dryRun)
}

func expandTarReader(ctx context.Context, logger *zap.SugaredLogger, uploader *manager.Uploader, r io.Reader, outBucket, outPrefix, skipPattern string, metadataByID map[string]map[string]string, dryRun bool) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if hdr == nil {
			continue
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			continue
		}
		dstKey, err := objectKeyForEntry(outPrefix, hdr.Name, metadataByID)
		if err != nil {
			return err
		}
		if dstKey == "" {
			continue
		}
		if shouldSkip(hdr.Name, skipPattern) {
			continue
		}
		if dryRun {
			logger.Infow("dryrun upload",
				"bucket", outBucket,
				"key", dstKey,
				"entry", hdr.Name,
			)
			continue
		}
		logger.Infow("uploading",
			"bucket", outBucket,
			"key", dstKey,
			"entry", hdr.Name,
		)
		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:   &outBucket,
			Key:      &dstKey,
			Body:     tr,
			Metadata: metadataForEntry(hdr.Name, metadataByID),
		})
		if err != nil {
			return err
		}
	}
}

func objectKeyForEntry(prefix, name string, metadataByID map[string]map[string]string) (string, error) {
	if key, ok, err := metadataKeyForEntry(prefix, name, metadataByID); err != nil {
		return "", err
	} else if ok {
		return key, nil
	}
	clean := path.Clean(name)
	clean = strings.TrimPrefix(clean, "./")
	clean = strings.TrimLeft(clean, "/")
	if clean == "." || clean == "" {
		return "", nil
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("invalid tar entry name: %q", name)
	}
	if prefix == "" {
		return clean, nil
	}
	return path.Join(prefix, clean), nil
}

func metadataKeyForEntry(prefix, name string, metadataByID map[string]map[string]string) (string, bool, error) {
	if len(metadataByID) == 0 {
		return "", false, nil
	}
	base := path.Base(name)
	if !strings.HasSuffix(base, ".mcap") {
		return "", false, nil
	}
	id := strings.TrimSuffix(base, ".mcap")
	meta, ok := metadataByID[id]
	if !ok {
		return "", false, nil
	}
	clipStartUTC := meta["clip_start_utc"]
	if clipStartUTC == "" {
		return "", false, nil
	}
	ts, err := parseClipStartUTC(clipStartUTC)
	if err != nil {
		return "", false, err
	}
	key := path.Join(
		fmt.Sprintf("year=%04d", ts.Year()),
		fmt.Sprintf("month=%02d", int(ts.Month())),
		fmt.Sprintf("day=%02d", ts.Day()),
		base,
	)
	if prefix == "" {
		return key, true, nil
	}
	return path.Join(prefix, key), true, nil
}

func shouldSkip(name, pattern string) bool {
	if pattern == "" {
		return false
	}
	ok, err := path.Match(pattern, name)
	if err != nil {
		return false
	}
	return ok
}

func loadMetadata(ctx context.Context, parquetPath string) (map[string]map[string]string, error) {
	if parquetPath == "" {
		return nil, nil
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "SELECT pilot_assignment_id, user_metadata_parsed FROM read_parquet(?)", parquetPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metadata := make(map[string]map[string]string)
	for rows.Next() {
		var id string
		var value any
		if err := rows.Scan(&id, &value); err != nil {
			return nil, err
		}
		if id == "" || value == nil {
			continue
		}
		normalized := normalizeUUIDs(value)
		stringified := stringifyValues(normalized)
		mapped, err := metadataMapFromValue(stringified)
		if err != nil {
			return nil, err
		}
		if len(mapped) > 0 {
			metadata[id] = mapped
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return metadata, nil
}

func metadataForEntry(name string, metadataByID map[string]map[string]string) map[string]string {
	if len(metadataByID) == 0 {
		return nil
	}
	base := path.Base(name)
	if !strings.HasSuffix(base, ".mcap") {
		return nil
	}
	id := strings.TrimSuffix(base, ".mcap")
	if id == "" {
		return nil
	}
	meta, ok := metadataByID[id]
	if !ok {
		return nil
	}
	return meta
}

func metadataMapFromValue(value any) (map[string]string, error) {
	top, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("metadata is not an object")
	}
	out := make(map[string]string, len(top))
	for key, item := range top {
		switch v := item.(type) {
		case string:
			out[key] = v
		default:
			b, err := json.Marshal(v)
			if err != nil {
				out[key] = fmt.Sprintf("%v", v)
				continue
			}
			out[key] = string(b)
		}
	}
	return out, nil
}

func parseClipStartUTC(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range layouts {
		ts, err := time.Parse(layout, value)
		if err == nil {
			return ts, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func normalizeUUIDs(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = normalizeUUIDs(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = normalizeUUIDs(v[i])
		}
		return out
	case []byte:
		if len(v) == 16 {
			if id, err := uuid.FromBytes(v); err == nil {
				return id.String()
			}
		}
		return string(v)
	default:
		return value
	}
}

func stringifyValues(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = stringifyValues(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = stringifyValues(v[i])
		}
		return out
	case nil:
		return ""
	case string:
		return v
	case time.Time:
		return v.UTC().Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v)
	}
}
