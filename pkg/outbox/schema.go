package outbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryxContext(context.Context, string, ...any) (*sqlx.Rows, error)
	QueryRowxContext(context.Context, string, ...any) *sqlx.Row
}

type columnSpec struct {
	name, kind string
	size       int
}
type indexSpec struct {
	table, name string
	columns     []string
	status      *Status
}

func (s *sqlStorage) quote(name string) string {
	if s.postgres {
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (s *sqlStorage) table(name string) string { return s.quote(s.schema) + "." + s.quote(name) }

// 初始化锁仅保护同一套表的 DDL，不进入消息处理热路径。
func (s *sqlStorage) ensureSchema(ctx context.Context) error {
	conn, err := s.db.Connx(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	schema := s.config.SchemaName
	if schema == "" {
		query := "SELECT DATABASE()"
		if s.postgres {
			query = "SELECT current_schema()"
		}
		var name sql.NullString
		if err = conn.QueryRowxContext(ctx, query).Scan(&name); err != nil {
			return err
		}
		if !name.Valid || name.String == "" {
			return fmt.Errorf("%w: current schema is empty", ErrInvalidConfig)
		}
		schema = name.String
	}
	// 自动发现的 schema 同样需要安全引用，允许数据库已有的特殊字符。
	s.schema = schema
	lockName := fmt.Sprintf("outbox_%x", sha256.Sum256([]byte(schema+"."+s.messages)))
	lockName = lockName[:63]
	if s.postgres {
		tx, err := conn.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockName); err != nil {
			return err
		}
		if err = s.createAndValidate(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	}
	var acquired sql.NullInt64
	if err = conn.QueryRowxContext(ctx, "SELECT GET_LOCK(?, 10)", lockName).Scan(&acquired); err != nil {
		return err
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return fmt.Errorf("outbox: schema lock unavailable")
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), s.config.OperationTimeout)
		defer cancel()
		var released sql.NullInt64
		if err := conn.QueryRowxContext(unlockCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released); err != nil || !released.Valid || released.Int64 != 1 {
			// 命名锁属于物理连接，解锁失败时禁止将仍带锁的连接放回池中。
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	return s.createAndValidate(ctx, conn)
}

func (s *sqlStorage) createAndValidate(ctx context.Context, exec sqlExecutor) error {
	for _, name := range []string{s.messages, s.nodes, s.shards} {
		var exists int
		if err := exec.QueryRowxContext(ctx, s.bind("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?"), s.schema, name).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			if _, err := exec.ExecContext(ctx, s.tableDDL(name)); err != nil {
				return fmt.Errorf("outbox: create %s: %w", name, err)
			}
			// MySQL 把索引放进原子的 CREATE TABLE，避免初始化中断留下半张表。
			if s.postgres {
				for _, idx := range s.indexes() {
					if idx.table == name {
						if _, err := exec.ExecContext(ctx, s.indexDDL(idx)); err != nil {
							return fmt.Errorf("outbox: create index: %w", err)
						}
					}
				}
			}
		}
		if err := s.validateTable(ctx, exec, name); err != nil {
			return err
		}
	}
	// 各节点共享固定的 64 个逻辑分片，行本身不会随着业务消息清理。
	for shard := range shardCount {
		query := fmt.Sprintf("INSERT INTO %s (shard_id) VALUES (?)", s.table(s.shards))
		if s.postgres {
			query += " ON CONFLICT (shard_id) DO NOTHING"
		} else {
			query += " ON DUPLICATE KEY UPDATE shard_id=VALUES(shard_id)"
		}
		if _, err := exec.ExecContext(ctx, s.bind(query), shard); err != nil {
			return err
		}
	}
	var count int
	if err := exec.QueryRowxContext(ctx, "SELECT COUNT(*) FROM "+s.table(s.shards)).Scan(&count); err != nil {
		return err
	}
	if count != shardCount {
		return fmt.Errorf("%w: shard table must contain exactly %d rows", ErrInvalidConfig, shardCount)
	}
	return nil
}

func (s *sqlStorage) columns(name string) []columnSpec {
	switch name {
	case s.nodes:
		return []columnSpec{{"node_id", "varchar", 64}, {"lease_until", "bigint", 0}}
	case s.shards:
		return []columnSpec{{"shard_id", "integer", 0}, {"owner_id", "varchar", 64}, {"lease_token", "varchar", 64}, {"lease_until", "bigint", 0}}
	default:
		return []columnSpec{
			{"id", "bigint", 0}, {"message_type", "varchar", 128}, {"payload", "json", 0}, {"shard_id", "integer", 0},
			{"status", "smallint", 0}, {"attempts", "integer", 0}, {"next_attempt_at", "bigint", 0},
			{"lease_token", "varchar", 64}, {"lease_until", "bigint", 0}, {"last_error", "text", 0},
			{"created_at", "bigint", 0}, {"updated_at", "bigint", 0}, {"completed_at", "bigint", 0},
		}
	}
}

func (s *sqlStorage) tableDDL(name string) string {
	var defs []string
	for _, c := range s.columns(name) {
		kind := strings.ToUpper(c.kind)
		if c.size > 0 {
			kind = fmt.Sprintf("VARCHAR(%d)", c.size)
		}
		if c.kind == "json" && s.postgres {
			kind = "JSONB"
		}
		if c.kind == "varchar" && !s.postgres {
			if c.name == "message_type" {
				kind += " CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
			} else {
				kind += " CHARACTER SET ascii COLLATE ascii_bin"
			}
		}
		def := s.quote(c.name) + " " + kind + " NOT NULL"
		if c.name == "id" {
			if s.postgres {
				def = s.quote(c.name) + " BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"
			} else {
				def += " AUTO_INCREMENT PRIMARY KEY"
			}
		} else if (name == s.nodes && c.name == "node_id") || (name == s.shards && c.name == "shard_id") {
			def += " PRIMARY KEY"
		} else if c.kind == "varchar" || c.kind == "text" {
			def += " DEFAULT ''"
		} else if c.kind != "json" {
			def += " DEFAULT 0"
		}
		// MySQL 8.0 早期版本不支持 TEXT 默认值，写入时总是显式提供 last_error。
		if !s.postgres && c.kind == "text" {
			def = s.quote(c.name) + " TEXT NOT NULL"
		}
		defs = append(defs, def)
	}
	if name != s.nodes {
		defs = append(defs, "CHECK (shard_id BETWEEN 0 AND 63)")
	}
	if name == s.messages {
		defs = append(defs, "CHECK (status BETWEEN 0 AND 3)", "CHECK (attempts >= 0)")
	}
	if !s.postgres {
		for _, idx := range s.indexes() {
			if idx.table != name {
				continue
			}
			columns := make([]string, len(idx.columns))
			for i, column := range idx.columns {
				columns[i] = s.quote(column)
			}
			defs = append(defs, "KEY "+s.quote(idx.name)+" ("+strings.Join(columns, ", ")+")")
		}
	}
	query := fmt.Sprintf("CREATE TABLE %s (%s)", s.table(name), strings.Join(defs, ", "))
	if !s.postgres {
		return query + " ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
	}
	if name != s.messages {
		return query
	}
	p := s.config.postgres
	return query + fmt.Sprintf(` WITH (fillfactor=%d, autovacuum_enabled=true,
		autovacuum_vacuum_scale_factor=%g, autovacuum_vacuum_threshold=%d,
		autovacuum_analyze_scale_factor=%g, autovacuum_analyze_threshold=%d, vacuum_truncate=%t,
		toast.autovacuum_enabled=true, toast.autovacuum_vacuum_scale_factor=%g,
		toast.autovacuum_vacuum_threshold=%d, toast.vacuum_truncate=%t)`,
		p.FillFactor, p.VacuumScaleFactor, p.VacuumThreshold, p.AnalyzeScaleFactor, p.AnalyzeThreshold, p.VacuumTruncate,
		p.VacuumScaleFactor, p.VacuumThreshold, p.VacuumTruncate)
}

func (s *sqlStorage) indexes() []indexSpec {
	var indexes []indexSpec
	for _, v := range []struct {
		label, column string
		status        Status
	}{{"pending", "next_attempt_at", StatusPending}, {"processing", "lease_until", StatusProcessing}, {"cleanup", "completed_at", StatusSucceeded}} {
		cols := []string{"shard_id", "status", v.column, "id"}
		var predicate *Status
		if s.postgres {
			cols = []string{"shard_id", v.column, "id"}
			status := v.status
			predicate = &status
		}
		hash := sha256.Sum256([]byte(s.messages + "_" + v.label))
		indexes = append(indexes, indexSpec{s.messages, fmt.Sprintf("outbox_%s_%x", v.label, hash[:6]), cols, predicate})
	}
	for _, v := range []struct {
		table, label string
		cols         []string
	}{{s.nodes, "nodes", []string{"lease_until"}}, {s.shards, "owners", []string{"owner_id", "lease_until"}}} {
		hash := sha256.Sum256([]byte(v.table))
		indexes = append(indexes, indexSpec{table: v.table, name: fmt.Sprintf("outbox_%s_%x", v.label, hash[:6]), columns: v.cols})
	}
	return indexes
}

func (s *sqlStorage) indexDDL(idx indexSpec) string {
	cols := make([]string, len(idx.columns))
	for i, c := range idx.columns {
		cols[i] = s.quote(c)
	}
	query := fmt.Sprintf("CREATE INDEX %s ON %s (%s)", s.quote(idx.name), s.table(idx.table), strings.Join(cols, ", "))
	if idx.status != nil {
		query += fmt.Sprintf(" WHERE status = %d", *idx.status)
	}
	return query
}

func (s *sqlStorage) validateTable(ctx context.Context, exec sqlExecutor, name string) error {
	rows, err := exec.QueryxContext(ctx, s.bind(`SELECT column_name,data_type,is_nullable,COALESCE(character_maximum_length,0)
		FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ordinal_position`), s.schema, name)
	if err != nil {
		return err
	}
	type actualColumn struct {
		name, kind, nullable string
		size                 int64
	}
	var actual []actualColumn
	for rows.Next() {
		var c actualColumn
		if err = rows.Scan(&c.name, &c.kind, &c.nullable, &c.size); err != nil {
			rows.Close()
			return err
		}
		actual = append(actual, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	expected := s.columns(name)
	if len(actual) != len(expected) {
		return fmt.Errorf("%w: incompatible columns in %s", ErrInvalidConfig, name)
	}
	for i, c := range expected {
		kind := c.kind
		if s.postgres {
			if kind == "varchar" {
				kind = "character varying"
			}
			if kind == "json" {
				kind = "jsonb"
			}
		} else if kind == "integer" {
			kind = "int"
		}
		a := actual[i]
		if a.name != c.name || a.kind != kind || a.nullable != "NO" || (c.size > 0 && a.size != int64(c.size)) {
			return fmt.Errorf("%w: incompatible %s.%s", ErrInvalidConfig, name, c.name)
		}
	}
	pk := "id"
	if name == s.nodes {
		pk = "node_id"
	}
	if name == s.shards {
		pk = "shard_id"
	}
	rows, err = exec.QueryxContext(ctx, s.bind(`SELECT k.column_name FROM information_schema.table_constraints t
		JOIN information_schema.key_column_usage k ON t.constraint_catalog=k.constraint_catalog
		AND t.constraint_schema=k.constraint_schema AND t.constraint_name=k.constraint_name AND t.table_name=k.table_name
		WHERE t.table_schema=? AND t.table_name=? AND t.constraint_type='PRIMARY KEY' ORDER BY k.ordinal_position`), s.schema, name)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(keys) != 1 || keys[0] != pk {
		return fmt.Errorf("%w: invalid primary key in %s", ErrInvalidConfig, name)
	}
	if name == s.messages {
		var generated string
		query := "SELECT extra FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name='id'"
		want := "auto_increment"
		if s.postgres {
			query = "SELECT is_identity FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name='id'"
			want = "YES"
		}
		if err = exec.QueryRowxContext(ctx, s.bind(query), s.schema, name).Scan(&generated); err != nil {
			return err
		}
		if !strings.Contains(generated, want) {
			return fmt.Errorf("%w: message ID must be database-generated", ErrInvalidConfig)
		}
	}
	if !s.postgres {
		var engine string
		if err = exec.QueryRowxContext(ctx, "SELECT engine FROM information_schema.tables WHERE table_schema=? AND table_name=?", s.schema, name).Scan(&engine); err != nil {
			return err
		}
		if !strings.EqualFold(engine, "InnoDB") {
			return fmt.Errorf("%w: table must use InnoDB", ErrInvalidConfig)
		}
		if name == s.messages {
			var incompatible int
			if err = exec.QueryRowxContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
				WHERE table_schema=? AND table_name=? AND column_name IN ('message_type','last_error')
				AND character_set_name<>'utf8mb4'`, s.schema, name).Scan(&incompatible); err != nil {
				return err
			}
			if incompatible != 0 {
				return fmt.Errorf("%w: message text columns must use utf8mb4", ErrInvalidConfig)
			}
		}
	}
	for _, idx := range s.indexes() {
		if idx.table == name {
			if err = s.validateIndex(ctx, exec, idx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sqlStorage) validateIndex(ctx context.Context, exec sqlExecutor, idx indexSpec) error {
	invalid := func() error { return fmt.Errorf("%w: missing or incompatible index %s", ErrInvalidConfig, idx.name) }
	if s.postgres {
		var definition string
		err := exec.QueryRowxContext(ctx, `SELECT pg_get_indexdef(i.indexrelid) FROM pg_index i
			JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace
			JOIN pg_class x ON x.oid=i.indexrelid WHERE n.nspname=$1 AND c.relname=$2 AND x.relname=$3 AND i.indisvalid AND i.indisready AND NOT i.indisunique`, s.schema, idx.table, idx.name).Scan(&definition)
		if err == sql.ErrNoRows {
			return invalid()
		}
		if err != nil {
			return err
		}
		normalize := func(v string) string {
			return strings.ToLower(strings.NewReplacer(" ", "", "\n", "", "\t", "", `"`, "").Replace(v))
		}
		want := "usingbtree(" + strings.Join(idx.columns, ",") + ")"
		if !strings.Contains(normalize(definition), want) {
			return invalid()
		}
		if idx.status != nil && !strings.HasSuffix(normalize(definition), fmt.Sprintf("where(status=%d)", *idx.status)) {
			return invalid()
		}
		if idx.status == nil && strings.Contains(normalize(definition), ")where") {
			return invalid()
		}
		return nil
	}
	rows, err := exec.QueryxContext(ctx, `SELECT column_name,sub_part,index_type,is_visible,non_unique FROM information_schema.statistics
		WHERE table_schema=? AND table_name=? AND index_name=? ORDER BY seq_in_index`, s.schema, idx.table, idx.name)
	if err != nil {
		return err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column, kind, visible string
		var prefix sql.NullInt64
		var nonUnique int
		if err = rows.Scan(&column, &prefix, &kind, &visible, &nonUnique); err != nil {
			return err
		}
		if prefix.Valid || kind != "BTREE" || visible != "YES" || nonUnique != 1 {
			return invalid()
		}
		columns = append(columns, column)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if strings.Join(columns, ",") != strings.Join(idx.columns, ",") {
		return invalid()
	}
	return nil
}
