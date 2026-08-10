package sqlxhelper

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jmoiron/sqlx"
)

func TestTransactionCommitsWhenHandlerSucceeds(t *testing.T) {
	state := &transactionDriverState{}
	db := openTransactionTestDB(t, state)

	called := false
	err := Transaction(context.Background(), db, func(tx *sqlx.Tx) error {
		called = true
		if tx == nil {
			t.Fatal("handler received a nil transaction")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
	if got := state.commits.Load(); got != 1 {
		t.Fatalf("commit calls = %d, want 1", got)
	}
	if got := state.rollbacks.Load(); got != 0 {
		t.Fatalf("rollback calls = %d, want 0", got)
	}
}

func TestTransactionRollsBackWhenHandlerFails(t *testing.T) {
	state := &transactionDriverState{}
	db := openTransactionTestDB(t, state)
	wantErr := errors.New("handler failed")

	err := Transaction(context.Background(), db, func(*sqlx.Tx) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Transaction error = %v, want %v", err, wantErr)
	}
	if got := state.commits.Load(); got != 0 {
		t.Fatalf("commit calls = %d, want 0", got)
	}
	if got := state.rollbacks.Load(); got != 1 {
		t.Fatalf("rollback calls = %d, want 1", got)
	}
}

func TestTransactionReturnsBeginError(t *testing.T) {
	wantErr := errors.New("begin failed")
	state := &transactionDriverState{beginErr: wantErr}
	db := openTransactionTestDB(t, state)

	called := false
	err := Transaction(context.Background(), db, func(*sqlx.Tx) error {
		called = true
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Transaction error = %v, want %v", err, wantErr)
	}
	if called {
		t.Fatal("handler must not run when begin fails")
	}
}

func TestTransactionReturnsCommitError(t *testing.T) {
	wantErr := errors.New("commit failed")
	state := &transactionDriverState{commitErr: wantErr}
	db := openTransactionTestDB(t, state)

	err := Transaction(context.Background(), db, func(*sqlx.Tx) error { return nil })
	if !errors.Is(err, wantErr) {
		t.Fatalf("Transaction error = %v, want %v", err, wantErr)
	}
	if got := state.commits.Load(); got != 1 {
		t.Fatalf("commit calls = %d, want 1", got)
	}
}

func TestTransactionJoinsRollbackError(t *testing.T) {
	handlerErr := errors.New("handler failed")
	rollbackErr := errors.New("rollback failed")
	state := &transactionDriverState{rollbackErr: rollbackErr}
	db := openTransactionTestDB(t, state)

	err := Transaction(context.Background(), db, func(*sqlx.Tx) error { return handlerErr })
	if !errors.Is(err, handlerErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("Transaction error = %v, want joined handler and rollback errors", err)
	}
}

func TestTransactionRollsBackAndRepanics(t *testing.T) {
	state := &transactionDriverState{}
	db := openTransactionTestDB(t, state)
	wantPanic := "handler panic"

	func() {
		defer func() {
			if got := recover(); got != wantPanic {
				t.Fatalf("panic = %v, want %q", got, wantPanic)
			}
		}()
		_ = Transaction(context.Background(), db, func(*sqlx.Tx) error {
			panic(wantPanic)
		})
	}()

	if got := state.rollbacks.Load(); got != 1 {
		t.Fatalf("rollback calls = %d, want 1", got)
	}
}

func TestTransactionValidatesArguments(t *testing.T) {
	db := openTransactionTestDB(t, &transactionDriverState{})
	handler := Handler(func(*sqlx.Tx) error { return nil })

	tests := []struct {
		name    string
		ctx     context.Context
		db      *sqlx.DB
		handler Handler
	}{
		{name: "nil context", db: db, handler: handler},
		{name: "nil db", ctx: context.Background(), handler: handler},
		{name: "nil handler", ctx: context.Background(), db: db},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Transaction(test.ctx, test.db, test.handler)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Transaction error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

type transactionDriverState struct {
	beginErr    error
	commitErr   error
	rollbackErr error
	commits     atomic.Int64
	rollbacks   atomic.Int64
}

type transactionTestDriver struct {
	state *transactionDriverState
}

func (d transactionTestDriver) Open(string) (driver.Conn, error) {
	return &transactionTestConn{state: d.state}, nil
}

type transactionTestConn struct {
	state *transactionDriverState
}

func (c *transactionTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c *transactionTestConn) Close() error { return nil }

func (c *transactionTestConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *transactionTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.state.beginErr != nil {
		return nil, c.state.beginErr
	}
	return &transactionTestTx{state: c.state}, nil
}

type transactionTestTx struct {
	state *transactionDriverState
}

func (tx *transactionTestTx) Commit() error {
	tx.state.commits.Add(1)
	return tx.state.commitErr
}

func (tx *transactionTestTx) Rollback() error {
	tx.state.rollbacks.Add(1)
	return tx.state.rollbackErr
}

func openTransactionTestDB(t *testing.T, state *transactionDriverState) *sqlx.DB {
	t.Helper()
	db := sqlx.NewDb(
		sql.OpenDB(transactionTestConnector{driver: transactionTestDriver{state: state}}),
		"transaction-test",
	)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type transactionTestConnector struct {
	driver transactionTestDriver
}

func (c transactionTestConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.Open("")
}

func (c transactionTestConnector) Driver() driver.Driver { return c.driver }
