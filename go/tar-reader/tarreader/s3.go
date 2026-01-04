package tarreader

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Getter struct {
	Client *s3.Client
	Bucket string
	Key    string
	Read   int64
	Requests int64
}

func (g *S3Getter) Get(ctx context.Context, offset, length int64) ([]byte, error) {
	if g == nil || g.Client == nil {
		return nil, fmt.Errorf("nil s3 client")
	}
	if length <= 0 {
		return nil, nil
	}
	end := offset + length - 1
	r := fmt.Sprintf("bytes=%d-%d", offset, end)
	out, err := g.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &g.Bucket,
		Key:    &g.Key,
		Range:  &r,
	})
	if err != nil {
		return nil, err
	}
	g.Requests++
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	g.Read += int64(len(b))
	return b, nil
}
