package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"tar-reader/tarreader"
)

func main() {
	var (
		bucket = flag.String("bucket", "", "s3 bucket")
		key    = flag.String("key", "", "s3 key")
		read   = flag.Bool("read", false, "read and discard file contents")
		profile = flag.String("profile", "", "aws profile (supports SSO)")
		region  = flag.String("region", "", "aws region override")
	)
	flag.Parse()

	if *bucket == "" && *key == "" && flag.NArg() == 1 {
		b, k, err := parseS3URI(flag.Arg(0))
		if err != nil {
			exitErr(err)
		}
		*bucket, *key = b, k
	}
	if *bucket == "" || *key == "" {
		exitErr(fmt.Errorf("usage: tar-reader -bucket BUCKET -key KEY [or s3://bucket/key]"))
	}

	ctx := context.Background()
	opts := []func(*config.LoadOptions) error{}
	if *profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(*profile))
	}
	if *region != "" {
		opts = append(opts, config.WithRegion(*region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		exitErr(err)
	}
	client := s3.NewFromConfig(cfg)
	getter := &tarreader.S3Getter{Client: client, Bucket: *bucket, Key: *key}

	err = tarreader.Walk(ctx, getter, func(e tarreader.Entry, r io.Reader) error {
		fmt.Printf("%s\t%d\n", e.Name, e.Size)
		if *read {
			_, err := io.Copy(io.Discard, r)
			return err
		}
		return nil
	})
	if err != nil {
		exitErr(err)
	}
	fmt.Printf("bytes_read\t%d\n", getter.Read)
	fmt.Printf("requests\t%d\n", getter.Requests)
}

func parseS3URI(s string) (string, string, error) {
	if !strings.HasPrefix(s, "s3://") {
		return "", "", fmt.Errorf("invalid s3 uri: %s", s)
	}
	rest := strings.TrimPrefix(s, "s3://")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid s3 uri: %s", s)
	}
	return parts[0], parts[1], nil
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
