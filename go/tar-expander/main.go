package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/zap"
)

func main() {
	defaultOutBucket := envOrDefault("OUT_BUCKET", "coco-trip-clips-976053906881-us-west-2")
	defaultOutPrefix := envOrDefault("OUT_PREFIX", "v3")
	defaultSkipPattern := envOrDefault("SKIP_PATTERN", "*.metadata.json")
	defaultMetadataParquet := os.Getenv("METADATA_PARQUET")
	defaultQueueURL := os.Getenv("QUEUE_URL")
	defaultLocalTar := os.Getenv("LOCAL_TAR")
	defaultDryRun := envBool("DRYRUN")

	var outBucket string
	var outPrefix string
	var skipPattern string
	var metadataParquet string
	var queueURL string
	var localTar string
	var dryRun bool

	flag.StringVar(&outBucket, "out-bucket", defaultOutBucket, "destination S3 bucket (default: source bucket)")
	flag.StringVar(&outPrefix, "out-prefix", defaultOutPrefix, "destination key prefix (default: none)")
	flag.StringVar(&skipPattern, "skip-pattern", defaultSkipPattern, "glob pattern to skip matching tar entries")
	flag.StringVar(&metadataParquet, "metadata-parquet", defaultMetadataParquet, "path to tar-metadata.parquet for S3 object metadata")
	flag.StringVar(&queueURL, "queue-url", defaultQueueURL, "SQS queue URL for work items")
	flag.StringVar(&localTar, "local-tar", defaultLocalTar, "path to local tar file to expand to S3")
	flag.BoolVar(&dryRun, "dryrun", defaultDryRun, "log uploads without writing to S3")
	flag.Parse()

	s3Endpoint := os.Getenv("S3_ENDPOINT_URL")
	sqsEndpoint := os.Getenv("SQS_ENDPOINT_URL")

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = logger.Sync()
	}()

	if queueURL == "" && localTar == "" {
		logger.Error("queue-url or local-tar is required")
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-2"))
	if err != nil {
		logger.Error("failed to load aws config", zap.Error(err))
		os.Exit(1)
	}

	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		if s3Endpoint != "" {
			options.BaseEndpoint = aws.String(s3Endpoint)
			options.UsePathStyle = true
		}
	})
	uploader := manager.NewUploader(client)
	sqsClient := sqs.NewFromConfig(cfg, func(options *sqs.Options) {
		if sqsEndpoint != "" {
			options.BaseEndpoint = aws.String(sqsEndpoint)
		}
	})

	metadataByID, err := loadMetadata(ctx, metadataParquet)
	if err != nil {
		logger.Error("failed to load metadata", zap.Error(err))
		os.Exit(1)
	}

	if queueURL != "" {
		if err := runQueueWorker(ctx, logger, sqsClient, client, uploader, queueURL, outBucket, outPrefix, skipPattern, metadataByID, dryRun); err != nil {
			logger.Error("queue worker failed", zap.Error(err))
			os.Exit(1)
		}
		return
	}

	if err := expandLocalTar(ctx, logger.Sugar(), client, uploader, localTar, outBucket, outPrefix, skipPattern, metadataByID, dryRun); err != nil {
		logger.Error("expand failed", zap.Error(err))
		os.Exit(1)
	}
	if !dryRun {
		if err := os.Remove(localTar); err != nil {
			logger.Error("failed to delete local tar", zap.Error(err), zap.String("path", localTar))
			os.Exit(1)
		}
		logger.Info("deleted local tar", zap.String("path", localTar))
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string) bool {
	value := os.Getenv(key)
	if value == "" {
		return false
	}
	switch strings.ToLower(value) {
	case "1", "true", "t", "yes", "y":
		return true
	default:
		return false
	}
}
