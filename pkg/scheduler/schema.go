package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// 新建任务表时采用的存储参数沿用 compensator：按绝对死元组数触发 autovacuum 并降低节流，
// 使 DELETE 产生的死元组被及时回收，堆页和索引页的空闲空间被后续 INSERT 复用。
// 已存在的表不会被覆盖，运维可通过 ALTER TABLE 按实际吞吐调整。
const taskTableOptions = `fillfactor = 90,
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 2000,
  autovacuum_vacuum_insert_scale_factor = 0,
  autovacuum_vacuum_insert_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_cost_delay = 1,
  toast.autovacuum_vacuum_scale_factor = 0,
  toast.autovacuum_vacuum_threshold = 2000`

var taskColumns = map[string]string{
	"id": "uuid", "task_type": "text", "payload": "text", "status": "smallint",
	"run_at": "timestamp with time zone", "attempts": "integer", "last_error": "text",
	"created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
	"completed_at": "timestamp with time zone",
}

func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// indexName 按 PostgreSQL 的 63 字节上限截断索引名。表名加后缀最长可达 65 字节，
// 截断后三个索引名仍然互不相同，且与 pg_indexes 中记录的名字一致。
func indexName(table, suffix string) string {
	name := table + suffix
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

// ensureSchema 在事务级 advisory lock 下建表并校验，多节点并发启动时串行执行。
func (s *pgStorage) ensureSchema(ctx context.Context) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	schema := s.config.Schema
	if schema == "" {
		var name sql.NullString
		if err = tx.QueryRowContext(ctx, "SELECT current_schema()").Scan(&name); err != nil {
			return err
		}
		if !name.Valid || name.String == "" {
			return fmt.Errorf("%w: current schema is empty", ErrInvalidConfig)
		}
		schema = name.String
	}
	s.setNames(schema)
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "scheduler:"+schema+"."+s.tableName); err != nil {
		return err
	}
	indexes := []string{
		indexName(s.tableName, "_load_idx"),
		indexName(s.tableName, "_queued_idx"),
		indexName(s.tableName, "_done_idx"),
	}
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  id uuid PRIMARY KEY,
  task_type text NOT NULL,
  payload text NOT NULL,
  status smallint NOT NULL,
  run_at timestamptz NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  last_error text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  completed_at timestamptz
) WITH (%s)`, s.tasks, taskTableOptions),
		// loader 只覆盖尚未入队的任务。
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (run_at) WHERE status = 0",
			quoteIdent(indexes[0]), s.tasks),
		// reconciler 只覆盖已入队未完成的任务。
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (run_at) WHERE status = 1",
			quoteIdent(indexes[1]), s.tasks),
		// cleaner 只覆盖终态任务。
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (completed_at) WHERE status >= 2",
			quoteIdent(indexes[2]), s.tasks),
	}
	for _, statement := range statements {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err = validateColumns(ctx, tx, schema, s.tableName, taskColumns); err != nil {
		return err
	}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = $2 AND indexname = ANY($3::text[])",
		schema, s.tableName, indexes).Scan(&count); err != nil {
		return err
	}
	if count != len(indexes) {
		return fmt.Errorf("%w: table %s is missing required indexes", ErrInvalidConfig, s.tableName)
	}
	return tx.Commit()
}

type columnQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func validateColumns(ctx context.Context, q columnQuerier, schema, table string, want map[string]string) error {
	rows, err := q.QueryContext(ctx, "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", schema, table)
	if err != nil {
		return err
	}
	defer rows.Close()
	found := make(map[string]string, len(want))
	for rows.Next() {
		var name, kind string
		if err = rows.Scan(&name, &kind); err != nil {
			return err
		}
		found[name] = kind
	}
	if err = rows.Err(); err != nil {
		return err
	}
	for name, kind := range want {
		if found[name] != kind {
			return fmt.Errorf("%w: table %s column %s must be %s, got %q", ErrInvalidConfig, table, name, kind, found[name])
		}
	}
	return nil
}
