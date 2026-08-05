package snowflake

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestResolveConfigDefaultsAndNamespace(t *testing.T) {
	now := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	resolved, err := resolveConfig(Config{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.table != defaultTableName || resolved.epoch != DefaultEpoch {
		t.Fatalf("defaults = (%q, %d), want (%q, %d)", resolved.table, resolved.epoch, defaultTableName, DefaultEpoch)
	}

	resolved, err = resolveConfig(Config{Namespace: "infra", TableName: "workers", Epoch: now.Add(-time.Hour).UnixMilli()}, now)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.table != "infra.workers" {
		t.Fatalf("qualified table = %q, want infra.workers", resolved.table)
	}
}

func TestResolveConfigRejectsInvalidValues(t *testing.T) {
	now := time.Now()
	tests := []Config{
		{TableName: "worker-id"},
		{TableName: "workers;DROP_TABLE"},
		{Namespace: "infra.prod"},
		{Epoch: -1},
		{Epoch: now.Add(time.Second).UnixMilli()},
		{Epoch: now.UnixMilli() - maxTimestamp - 1},
	}
	for _, config := range tests {
		if _, err := resolveConfig(config, now); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("resolveConfig(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}
}

func TestNewRejectsInvalidInputs(t *testing.T) {
	if _, err := New(nil, nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(nil) error = %v, want ErrInvalidConfig", err)
	}
	if _, err := New(context.Background(), nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(nil db) error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewFromResource(context.Background(), "", Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewFromResource(empty) error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewFromResource(context.Background(), "snowflake-test-missing", Config{}); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("NewFromResource(missing) error = %v, want ErrResourceNotFound", err)
	}
}

func TestNodeGenerateLayoutAndTime(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	current := epoch + 12345
	node, err := newNode(37, epoch)
	if err != nil {
		t.Fatal(err)
	}
	node.now = func() time.Time { return time.UnixMilli(current) }

	id, err := node.generate()
	if err != nil {
		t.Fatal(err)
	}
	workerID := (id >> workerIDShift) & MaxWorkerID
	sequence := id & MaxSequence
	if workerID != 37 || sequence != 0 {
		t.Fatalf("decoded ID = worker %d sequence %d, want worker 37 sequence 0", workerID, sequence)
	}
	if got := node.getTimeFromID(id); got != current {
		t.Fatalf("decoded timestamp = %d, want %d", got, current)
	}

	secondID, err := node.generate()
	if err != nil {
		t.Fatal(err)
	}
	if secondID != id+1 {
		t.Fatalf("second ID = %d, want %d", secondID, id+1)
	}
}

func TestNodeConcurrentUniquenessAndMonotonicity(t *testing.T) {
	node, err := newNode(1, DefaultEpoch)
	if err != nil {
		t.Fatal(err)
	}

	const count = 5000
	ids := make(chan int64, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := node.generate()
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	unique := make(map[int64]struct{}, count)
	for id := range ids {
		unique[id] = struct{}{}
	}
	if len(unique) != count {
		t.Fatalf("unique IDs = %d, want %d", len(unique), count)
	}

	last := int64(0)
	for range 1000 {
		id, err := node.generate()
		if err != nil {
			t.Fatal(err)
		}
		if id <= last {
			t.Fatalf("ID %d is not greater than %d", id, last)
		}
		last = id
	}
}

func TestNodeSequenceOverflowWaitsForNextMillisecond(t *testing.T) {
	const epoch = int64(1000)
	node, err := newNode(2, epoch)
	if err != nil {
		t.Fatal(err)
	}
	current := int64(2000)
	node.timestamp = current
	node.sequence = MaxSequence
	node.now = func() time.Time { return time.UnixMilli(current) }
	node.sleep = func(time.Duration) { current++ }

	id, err := node.generate()
	if err != nil {
		t.Fatal(err)
	}
	if sequence := id & MaxSequence; sequence != 0 {
		t.Fatalf("sequence after overflow = %d, want 0", sequence)
	}
	if got := node.getTimeFromID(id); got != 2001 {
		t.Fatalf("timestamp after overflow = %d, want 2001", got)
	}
}

func TestNodeReportsClockRollbackAndTimestampOverflow(t *testing.T) {
	node, err := newNode(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	node.timestamp = 2000
	node.now = func() time.Time { return time.UnixMilli(1999) }
	if _, err := node.generate(); !errors.Is(err, ErrClockBackwards) {
		t.Fatalf("rollback error = %v, want ErrClockBackwards", err)
	}

	node.timestamp = 0
	node.now = func() time.Time { return time.UnixMilli(1000 + maxTimestamp + 1) }
	if _, err := node.generate(); !errors.Is(err, ErrTimestampOverflow) {
		t.Fatalf("overflow error = %v, want ErrTimestampOverflow", err)
	}
}

func TestGenerateRejectsClosedAndExpiredLease(t *testing.T) {
	node, err := newNode(1, DefaultEpoch)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	generator := &Snowflake{node: node, ctx: ctx}
	generator.state.Store(stateClosed)
	if _, err := generator.Generate(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Generate error = %v, want ErrClosed", err)
	}

	generator.state.Store(stateActive)
	generator.lastHeartbeat.Store(time.Now().Add(-defaultSafetyThreshold - time.Second).UnixNano())
	if _, err := generator.Generate(); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired Generate error = %v, want ErrLeaseLost", err)
	}
	if generator.state.Load() != stateLeaseLost {
		t.Fatal("expired lease must transition to stateLeaseLost")
	}
}

func TestDuplicateKeyDetection(t *testing.T) {
	mysqlError := &mysql.MySQLError{Number: 1062, Message: "duplicate"}
	if !isDuplicateKey(mysqlError) {
		t.Fatal("MySQL duplicate key error was not detected")
	}
	postgresError := &pgconn.PgError{Code: "23505", Message: "duplicate"}
	if !isDuplicateKey(postgresError) {
		t.Fatal("PostgreSQL duplicate key error was not detected")
	}
	if isDuplicateKey(errors.New("other")) {
		t.Fatal("unrelated error was detected as duplicate key")
	}
}

func TestRetryableTransactionDetection(t *testing.T) {
	tests := []error{
		&mysql.MySQLError{Number: 1205, Message: "lock wait timeout"},
		&mysql.MySQLError{Number: 1213, Message: "deadlock"},
		&pgconn.PgError{Code: "40001", Message: "serialization failure"},
		&pgconn.PgError{Code: "40P01", Message: "deadlock"},
	}
	for _, err := range tests {
		if !isRetryableTransaction(err) {
			t.Fatalf("error %v should be retryable", err)
		}
	}
	if isRetryableTransaction(errors.New("other")) {
		t.Fatal("unrelated error was detected as retryable")
	}
}
