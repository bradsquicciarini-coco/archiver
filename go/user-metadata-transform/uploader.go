package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

type blobUploader struct {
	client      *container.Client
	concurrency int
}

type uploadJob struct {
	path    string
	payload []byte
}

type uploadPool struct {
	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan uploadJob
	errCh  chan error
	wg     sync.WaitGroup
}

func newUploadPool(u *blobUploader) *uploadPool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &uploadPool{
		ctx:    ctx,
		cancel: cancel,
		jobs:   make(chan uploadJob, u.concurrency*2),
		errCh:  make(chan error, 1),
	}

	for i := 0; i < u.concurrency; i++ {
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			for job := range pool.jobs {
				if err := u.upload(ctx, job); err != nil {
					select {
					case pool.errCh <- err:
						cancel()
					default:
					}
					return
				}
			}
		}()
	}

	return pool
}

func (p *uploadPool) Err() error {
	select {
	case err := <-p.errCh:
		return err
	default:
		return nil
	}
}

func (p *uploadPool) Submit(job uploadJob) error {
	select {
	case p.jobs <- job:
		return nil
	case err := <-p.errCh:
		return err
	case <-p.ctx.Done():
		return fmt.Errorf("upload canceled")
	}
}

func (p *uploadPool) CloseAndWait() error {
	close(p.jobs)
	p.wg.Wait()
	return p.Err()
}

func (u *blobUploader) upload(ctx context.Context, job uploadJob) error {
	_, err := u.client.NewBlockBlobClient(job.path).UploadBuffer(ctx, job.payload, nil)
	return err
}
