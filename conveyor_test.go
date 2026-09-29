package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type testProducer struct {
	mu          sync.Mutex
	nextCalls   int
	commitCalls int
}

func (p *testProducer) Next(ctx context.Context, input <-chan message) (message, bool, error) {
	p.mu.Lock()
	p.nextCalls++
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		return message{}, false, ctx.Err()
	case msg, ok := <-input:
		if !ok {
			return message{}, false, io.EOF
		}
		return msg, true, nil
	}
}

func (p *testProducer) Commit(cookie int) error {
	p.mu.Lock()
	p.commitCalls++
	p.mu.Unlock()
	return nil
}

type testConsumer struct {
	mu         sync.Mutex
	batches    int
	totalItems int
	failAfter  int
}

func (c *testConsumer) Process(items []message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.batches++
	c.totalItems += len(items)

	if c.failAfter > 0 && c.batches >= c.failAfter {
		return errors.New("process failed")
	}
	return nil
}

func TestNext_ReadsMessage(t *testing.T) {
	ctx := context.Background()
	ch := make(chan message, 1)

	want := message{data: "hello"}
	ch <- want
	close(ch)

	got, ok, err := Next(ctx, ch)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.data != want.data {
		t.Fatalf("expected %v, got %v", want.data, got.data)
	}
}

func TestNext_ClosedChannel(t *testing.T) {
	ctx := context.Background()
	ch := make(chan message)
	close(ch)

	_, ok, err := Next(ctx, ch)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if ok {
		t.Fatal("expected ok=false")
	}
}

func TestNext_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan message)

	_, ok, err := Next(ctx, ch)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if ok {
		t.Fatal("expected ok=false")
	}
}

func TestProcess_EmptyBatch(t *testing.T) {
	err := Process(nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	err = Process([]message{})
	if err == nil {
		t.Fatal("expected error for empty slice, got nil")
	}
}

func TestPipe_NilArgs(t *testing.T) {
	ctx := context.Background()
	in := make(chan message)

	err := Pipe(ctx, nil, &testConsumer{}, in, Config{})
	if err == nil {
		t.Fatal("expected error for nil producer")
	}

	err = Pipe(ctx, &testProducer{}, nil, in, Config{})
	if err == nil {
		t.Fatal("expected error for nil consumer")
	}
}

func TestPipe_ProcessesBatches(t *testing.T) {
	ctx := context.Background()
	in := make(chan message, 5)

	for i := 0; i < 5; i++ {
		in <- message{data: i}
	}
	close(in)

	p := &testProducer{}
	c := &testConsumer{}

	cfg := Config{
		MaxBatchSize:    2,
		BatchTimeout:    100 * time.Millisecond,
		MaxRetries:      1,
		RetryBackoff:    10 * time.Millisecond,
		WorkerCount:     2,
		ShutdownTimeout: 1 * time.Second,
	}

	err := Pipe(ctx, p, c, in, cfg)
	if err != nil {
		t.Fatalf("Pipe() error = %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.totalItems != 5 {
		t.Fatalf("expected 5 items processed, got %d", c.totalItems)
	}
	if c.batches == 0 {
		t.Fatal("expected at least 1 batch processed")
	}
}

func TestPipe_UsesDefaultConfigOnInvalidConfig(t *testing.T) {
	ctx := context.Background()
	in := make(chan message, 3)

	in <- message{data: 1}
	in <- message{data: 2}
	in <- message{data: 3}
	close(in)

	p := &testProducer{}
	c := &testConsumer{}

	cfg := Config{} // невалидная, должна замениться defaultConfig

	err := Pipe(ctx, p, c, in, cfg)
	if err != nil {
		t.Fatalf("Pipe() error = %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.totalItems != 3 {
		t.Fatalf("expected 3 items processed, got %d", c.totalItems)
	}
}

func TestPipe_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan message)

	p := &testProducer{}
	c := &testConsumer{}

	done := make(chan error, 1)

	go func() {
		done <- Pipe(ctx, p, c, in, Config{
			MaxBatchSize:    2,
			BatchTimeout:    100 * time.Millisecond,
			MaxRetries:      1,
			RetryBackoff:    10 * time.Millisecond,
			WorkerCount:     2,
			ShutdownTimeout: 1 * time.Second,
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	err := <-done
	if err != nil {
		t.Fatalf("Pipe() returned error unexpectedly: %v", err)
	}
}

func TestPipe_ClosesOnInputClose(t *testing.T) {
	ctx := context.Background()
	in := make(chan message, 1)

	in <- message{data: "x"}
	close(in)

	p := &testProducer{}
	c := &testConsumer{}

	err := Pipe(ctx, p, c, in, Config{
		MaxBatchSize:    10,
		BatchTimeout:    100 * time.Millisecond,
		MaxRetries:      1,
		RetryBackoff:    10 * time.Millisecond,
		WorkerCount:     1,
		ShutdownTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("Pipe() error = %v", err)
	}
}
