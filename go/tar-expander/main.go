package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/zap"
)

func main() {
	var outBucket string
	var outPrefix string
	var skipPattern string
	var metadataParquet string
	var queueURL string
	var localTar string
	var dryRun bool

	flag.StringVar(&outBucket, "out-bucket", "coco-trip-clips-976053906881-us-west-2", "destination S3 bucket (default: source bucket)")
	flag.StringVar(&outPrefix, "out-prefix", "v3", "destination key prefix (default: none)")
	flag.StringVar(&skipPattern, "skip-pattern", "*.metadata.json", "glob pattern to skip matching tar entries")
	flag.StringVar(&metadataParquet, "metadata-parquet", "", "path to tar-metadata.parquet for S3 object metadata")
	flag.StringVar(&queueURL, "queue-url", "", "SQS queue URL for work items")
	flag.StringVar(&localTar, "local-tar", "", "path to local tar file to expand to S3")
	flag.BoolVar(&dryRun, "dryrun", false, "log uploads without writing to S3")
	flag.Parse()

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

	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client)
	sqsClient := sqs.NewFromConfig(cfg)

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

	if err := expandLocalTar(ctx, logger.Sugar(), uploader, localTar, outBucket, outPrefix, skipPattern, metadataByID, dryRun); err != nil {
		logger.Error("expand failed", zap.Error(err))
		os.Exit(1)
	}
}
