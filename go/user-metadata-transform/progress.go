package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

type progressTracker struct {
	mu     sync.Mutex
	seen   map[string]struct{}
	file   *os.File
	writer *bufio.Writer
}

func newProgressTracker(path string) (*progressTracker, error) {
	if path == "" {
		return nil, nil
	}

	seen := make(map[string]struct{})
	if err := loadProgress(path, seen); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &progressTracker{
		seen:   seen,
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

func loadProgress(path string, seen map[string]struct{}) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		seen[line] = struct{}{}
	}
	return scanner.Err()
}

func (t *progressTracker) Has(path string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.seen[path]
	return ok
}

func (t *progressTracker) Mark(path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.seen[path]; ok {
		return nil
	}

	if _, err := fmt.Fprintln(t.writer, path); err != nil {
		return err
	}
	if err := t.writer.Flush(); err != nil {
		return err
	}
	t.seen[path] = struct{}{}
	return nil
}

func (t *progressTracker) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.writer != nil {
		if err := t.writer.Flush(); err != nil {
			_ = t.file.Close()
			return err
		}
	}
	if t.file != nil {
		return t.file.Close()
	}
	return nil
}
