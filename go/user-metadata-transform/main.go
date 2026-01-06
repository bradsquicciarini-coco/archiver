package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/spf13/cobra"
)

func main() {
	var (
		limit       int
		sasURL      string
		concurrency int
	)

	rootCmd := &cobra.Command{
		Use:   "user-metadata-transform <parquet-path>",
		Short: "Transform user_metadata from parquet and optionally upload to Azure Blob Storage",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			parquetPath := args[0]

			var uploader *blobUploader
			trimmedSAS := strings.TrimSpace(sasURL)
			if trimmedSAS == "" {
				trimmedSAS = strings.TrimSpace(os.Getenv("AZURE_STORAGE_SAS_URL"))
			}

			if trimmedSAS != "" {
				if concurrency < 1 {
					return fmt.Errorf("--concurrency must be >= 1")
				}

				normalizedURL, err := normalizeSASURL(trimmedSAS)
				if err != nil {
					return fmt.Errorf("invalid sas url: %w", err)
				}

				client, err := container.NewClientWithNoCredential(normalizedURL, nil)
				if err != nil {
					return fmt.Errorf("create azure container client: %w", err)
				}

				uploader = &blobUploader{
					client:      client,
					concurrency: concurrency,
				}
			}

			return runPipeline(parquetPath, limit, uploader)
		},
	}

	rootCmd.Flags().IntVar(&limit, "limit", 0, "limit number of rows read from parquet (0 means no limit)")
	rootCmd.Flags().StringVar(&sasURL, "sas-url", "", "azure container SAS URL (or set AZURE_STORAGE_SAS_URL)")
	rootCmd.Flags().IntVar(&concurrency, "concurrency", 8, "number of concurrent uploads")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func normalizeSASURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("missing scheme or host")
	}

	if strings.Contains(parsed.Host, ".dfs.core.windows.net") {
		parsed.Host = strings.Replace(parsed.Host, ".dfs.core.windows.net", ".blob.core.windows.net", 1)
	}

	return parsed.String(), nil
}
