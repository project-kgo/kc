package segment

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeTag struct {
	maxID int64
	step  int32
}

type fakeSegmentStore struct {
	mu sync.Mutex

	tags       map[string]*fakeTag
	initCalls  int
	fetchCalls int
	failNext   int
	blockNext  chan struct{}
	fetchStart chan struct{}
}

func newFakeSegmentStore() *fakeSegmentStore {
	return &fakeSegmentStore{tags: make(map[string]*fakeTag)}
}

func (s *fakeSegmentStore) initTag(_ context.Context, bizTag string, startID int64, step int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initCalls++
	if tag := s.tags[bizTag]; tag != nil {
		tag.step = step
		return nil
	}
	s.tags[bizTag] = &fakeTag{maxID: startID, step: step}
	return nil
}

func (s *fakeSegmentStore) fetchSegment(ctx context.Context, bizTag string) (*Segment, error) {
	s.mu.Lock()
	s.fetchCalls++
	if s.fetchStart != nil {
		select {
		case s.fetchStart <- struct{}{}:
		default:
		}
	}
	if s.failNext > 0 {
		s.failNext--
		s.mu.Unlock()
		return nil, errors.New("fake fetch failure")
	}
	block := s.blockNext
	s.blockNext = nil
	s.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	tag := s.tags[bizTag]
	if tag == nil {
		return nil, ErrBizTagNotFound
	}
	if tag.step <= 0 {
		return nil, ErrInvalidConfig
	}
	if tag.maxID > math.MaxInt64-int64(tag.step) {
		return nil, ErrIDOverflow
	}
	tag.maxID += int64(tag.step)
	return makeSegment(tag.maxID, tag.step)
}

func (s *fakeSegmentStore) stats(bizTag string) (maxID int64, step int32, initCalls, fetchCalls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tag := s.tags[bizTag]
	if tag != nil {
		maxID, step = tag.maxID, tag.step
	}
	return maxID, step, s.initCalls, s.fetchCalls
}

func TestDetectDialect(t *testing.T) {
	if got, err := detectDialect("pgx"); err != nil || got != dialectPostgreSQL {
		t.Fatalf("detectDialect(pgx) = (%v, %v)", got, err)
	}
	if got, err := detectDialect("mysql"); err != nil || got != dialectMySQL {
		t.Fatalf("detectDialect(mysql) = (%v, %v)", got, err)
	}
	if _, err := detectDialect("sqlite3"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported dialect error = %v, want ErrInvalidConfig", err)
	}
}

func TestConfigAndBizTagValidation(t *testing.T) {
	resolved, err := resolveConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.table != defaultTableName {
		t.Fatalf("default table = %q, want %q", resolved.table, defaultTableName)
	}
	resolved, err = resolveConfig(Config{Namespace: "infra", TableName: "segments"})
	if err != nil || resolved.table != "infra.segments" {
		t.Fatalf("qualified table = (%q, %v), want infra.segments", resolved.table, err)
	}

	invalidConfigs := []Config{
		{Namespace: "infra.prod"},
		{TableName: "segment-id"},
		{TableName: "segments;drop"},
	}
	for _, config := range invalidConfigs {
		if _, err := resolveConfig(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("resolveConfig(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}

	invalidTags := []string{"", "   ", strings.Repeat("业", maxBizTagLength+1), string([]byte{0xff})}
	for _, bizTag := range invalidTags {
		if err := validateBizTag(bizTag); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("validateBizTag(%q) error = %v, want ErrInvalidConfig", bizTag, err)
		}
	}
	if err := validateBizTag(strings.Repeat("业", maxBizTagLength)); err != nil {
		t.Fatalf("valid UTF-8 biz tag rejected: %v", err)
	}
}

func TestNewFromResourceRejectsMissingResource(t *testing.T) {
	if _, err := New(nil, nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v, want ErrInvalidConfig", err)
	}
	if _, err := New(context.Background(), nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil database error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewFromResource(context.Background(), "", Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty resource error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewFromResource(context.Background(), "segment-test-missing", Config{}); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("missing resource error = %v, want ErrResourceNotFound", err)
	}
}

func TestInitRejectsInvalidArgumentsAndOverflow(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })

	if err := generator.Init(nil, "orders", 0, 10); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v, want ErrInvalidConfig", err)
	}
	if err := generator.Init(context.Background(), "orders", -1, 10); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative start error = %v, want ErrInvalidConfig", err)
	}
	if err := generator.Init(context.Background(), "overflow", math.MaxInt64, 1); !errors.Is(err, ErrIDOverflow) {
		t.Fatalf("overflow initialization error = %v, want ErrIDOverflow", err)
	}
	if generator.getBuffer("overflow") != nil {
		t.Fatal("overflow initialization must remove the buffer")
	}
}

func TestLastInt64IDDoesNotWrapAround(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })
	if err := generator.Init(context.Background(), "last-id", math.MaxInt64-1, 1); err != nil {
		t.Fatal(err)
	}
	id, err := generator.GetID(context.Background(), "last-id")
	if err != nil || id != math.MaxInt64 {
		t.Fatalf("last ID = (%d, %v), want (%d, nil)", id, err, int64(math.MaxInt64))
	}
	if _, err := generator.GetID(context.Background(), "last-id"); !errors.Is(err, ErrIDOverflow) {
		t.Fatalf("ID after MaxInt64 error = %v, want ErrIDOverflow", err)
	}
}

func TestGetIDAutoInitializesWithDefaults(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })

	id, err := generator.GetID(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("first ID = %d, want 1", id)
	}
	_, step, initCalls, _ := store.stats("orders")
	if step != DefaultStep || initCalls != 1 {
		t.Fatalf("default initialization = step %d calls %d, want %d/1", step, initCalls, DefaultStep)
	}
}

func TestConcurrentGetIDPublishesOneInitializedBuffer(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })

	const count = 200
	ids := make(chan int64, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := generator.GetID(context.Background(), "auto-orders")
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
	_, step, initCalls, _ := store.stats("auto-orders")
	if step != DefaultStep || initCalls != 1 {
		t.Fatalf("initialization = step %d calls %d, want %d/1", step, initCalls, DefaultStep)
	}
}

func TestInitUpdatesStepWithoutResettingProgress(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })

	if err := generator.Init(context.Background(), "orders", 100, 10); err != nil {
		t.Fatal(err)
	}
	if err := generator.Init(context.Background(), "orders", 0, 5); err != nil {
		t.Fatal(err)
	}

	for want := int64(101); want <= 111; want++ {
		id, err := generator.GetID(context.Background(), "orders")
		if err != nil {
			t.Fatal(err)
		}
		if id != want {
			t.Fatalf("ID = %d, want %d", id, want)
		}
	}
	maxID, step, initCalls, _ := store.stats("orders")
	if maxID < 115 || step != 5 || initCalls != 2 {
		t.Fatalf("tag state = max %d step %d init calls %d", maxID, step, initCalls)
	}
}

func TestDoubleBufferPreloadsAtTwentyPercent(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })
	if err := generator.Init(context.Background(), "orders", 0, 10); err != nil {
		t.Fatal(err)
	}

	for want := int64(1); want <= 2; want++ {
		id, err := generator.GetID(context.Background(), "orders")
		if err != nil || id != want {
			t.Fatalf("GetID = (%d, %v), want (%d, nil)", id, err, want)
		}
	}
	waitFor(t, time.Second, func() bool {
		_, _, _, fetchCalls := store.stats("orders")
		return fetchCalls >= 2
	})

	for want := int64(3); want <= 11; want++ {
		id, err := generator.GetID(context.Background(), "orders")
		if err != nil || id != want {
			t.Fatalf("GetID = (%d, %v), want (%d, nil)", id, err, want)
		}
	}
}

func TestAsyncLoadFailureFallsBackToSynchronousFetch(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })
	if err := generator.Init(context.Background(), "orders", 0, 5); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.failNext = 1
	store.mu.Unlock()

	for want := int64(1); want <= 6; want++ {
		id, err := generator.GetID(context.Background(), "orders")
		if err != nil || id != want {
			t.Fatalf("GetID = (%d, %v), want (%d, nil)", id, err, want)
		}
		if want == 1 {
			waitFor(t, time.Second, func() bool {
				_, _, _, fetchCalls := store.stats("orders")
				return fetchCalls >= 2
			})
		}
	}
}

func TestConcurrentGetIDIsUnique(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })
	if err := generator.Init(context.Background(), "orders", 0, 17); err != nil {
		t.Fatal(err)
	}

	const count = 3000
	ids := make(chan int64, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := generator.GetID(context.Background(), "orders")
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

	values := make([]int64, 0, count)
	for id := range ids {
		values = append(values, id)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) != count {
		t.Fatalf("IDs = %d, want %d", len(values), count)
	}
	for index, id := range values {
		if want := int64(index + 1); id != want {
			t.Fatalf("sorted ID %d = %d, want %d", index, id, want)
		}
	}
}

func TestInitRespectsContextCancellation(t *testing.T) {
	store := newFakeSegmentStore()
	store.blockNext = make(chan struct{})
	store.fetchStart = make(chan struct{}, 1)
	generator := newSegmentIDGen(context.Background(), store)
	t.Cleanup(func() { _ = generator.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	errChannel := make(chan error, 1)
	go func() {
		errChannel <- generator.Init(ctx, "orders", 0, 10)
	}()
	select {
	case <-store.fetchStart:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("initial segment fetch did not start")
	}
	if err := <-errChannel; !errors.Is(err, context.Canceled) {
		t.Fatalf("Init error = %v, want context.Canceled", err)
	}
	if generator.getBuffer("orders") != nil {
		t.Fatal("failed initialization must remove the buffer")
	}
}

func TestCloseCancelsAsyncLoadAndDoesNotAllowNewIDs(t *testing.T) {
	store := newFakeSegmentStore()
	generator := newSegmentIDGen(context.Background(), store)
	if err := generator.Init(context.Background(), "orders", 0, 5); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.blockNext = make(chan struct{})
	store.fetchStart = make(chan struct{}, 1)
	store.mu.Unlock()
	if _, err := generator.GetID(context.Background(), "orders"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.fetchStart:
	case <-time.After(time.Second):
		t.Fatal("async segment fetch did not start")
	}
	if err := generator.Close(); err != nil {
		t.Fatal(err)
	}
	if err := generator.Close(); err != nil {
		t.Fatalf("second Close error = %v", err)
	}
	if _, err := generator.GetID(context.Background(), "orders"); !errors.Is(err, ErrClosed) {
		t.Fatalf("GetID after Close error = %v, want ErrClosed", err)
	}
}

func TestDatabaseErrorClassification(t *testing.T) {
	if !isDuplicateKey(&mysql.MySQLError{Number: 1062, Message: "duplicate"}) {
		t.Fatal("MySQL duplicate error not detected")
	}
	if !isDuplicateKey(&pgconn.PgError{Code: "23505", Message: "duplicate"}) {
		t.Fatal("PostgreSQL duplicate error not detected")
	}
	for _, err := range []error{
		&mysql.MySQLError{Number: 1205, Message: "timeout"},
		&mysql.MySQLError{Number: 1213, Message: "deadlock"},
		&pgconn.PgError{Code: "40001", Message: "serialization"},
		&pgconn.PgError{Code: "40P01", Message: "deadlock"},
	} {
		if !isRetryableTransaction(err) {
			t.Fatalf("error %v should be retryable", err)
		}
	}
	if !isNumericOverflow(&pgconn.PgError{Code: "22003", Message: "overflow"}) {
		t.Fatal("PostgreSQL numeric overflow not detected")
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
