// Package sqlxgen 提供生成代码依赖的轻量 SQLx 运行时。
package sqlxgen

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	// ErrInvalidArgument 表示查询参数或生成配置非法。
	ErrInvalidArgument = errors.New("sqlxgen: invalid argument")
	// ErrUnsafeMutation 表示尝试执行没有条件的更新或删除。
	ErrUnsafeMutation = errors.New("sqlxgen: unsafe mutation")
	// ErrUnsupportedDriver 表示执行器使用了尚未支持的数据库驱动。
	ErrUnsupportedDriver = errors.New("sqlxgen: unsupported driver")
)

// Model 是触发代码生成的零大小标记，不包含任何隐式数据库字段。
type Model struct{}

// Executor 是生成查询所需的最小 SQLx 执行接口。
// *sqlx.DB 和 *sqlx.Tx 均实现该接口。
type Executor interface {
	DriverName() string
	Rebind(string) string
	BindNamed(string, any) (string, []any, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryxContext(context.Context, string, ...any) (*sqlx.Rows, error)
	QueryRowxContext(context.Context, string, ...any) *sqlx.Row
}

// Dialect 表示生成运行时支持的数据库方言。
type Dialect uint8

const (
	DialectPostgreSQL Dialect = iota + 1
	DialectMySQL
)

// DetectDialect 根据 SQLx 驱动名判断数据库方言。
func DetectDialect(executor Executor) (Dialect, error) {
	if executor == nil || isNilInterface(executor) {
		return 0, fmt.Errorf("%w: executor is nil", ErrInvalidArgument)
	}
	switch executor.DriverName() {
	case "pgx":
		return DialectPostgreSQL, nil
	case "mysql":
		return DialectMySQL, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrUnsupportedDriver, executor.DriverName())
	}
}

func isNilInterface(value any) bool {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// QuoteIdentifier 引用一个已由生成器校验过的 SQL 标识符。
func QuoteIdentifier(dialect Dialect, identifier string) string {
	if dialect == DialectMySQL {
		return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// QuoteTable 引用可选 schema 和表名。
func QuoteTable(dialect Dialect, schema, table string) string {
	quoted := QuoteIdentifier(dialect, table)
	if schema != "" {
		return QuoteIdentifier(dialect, schema) + "." + quoted
	}
	return quoted
}

// Expr 是只能由 sqlxgen 构造的查询条件。
type Expr interface {
	render(Dialect) (string, []any, error)
}

type comparisonExpr struct {
	owner  string
	column string
	op     string
	values []any
}

func (e comparisonExpr) render(dialect Dialect) (string, []any, error) {
	if e.column == "" {
		return "", nil, fmt.Errorf("%w: expression column is empty", ErrInvalidArgument)
	}
	column := QuoteIdentifier(dialect, e.column)
	switch e.op {
	case "IS NULL", "IS NOT NULL":
		return column + " " + e.op, nil, nil
	case "IN", "NOT IN":
		if len(e.values) == 0 {
			if e.op == "IN" {
				return "1 = 0", nil, nil
			}
			return "1 = 1", nil, nil
		}
		var builder strings.Builder
		builder.Grow(len(column) + len(e.op) + 4 + len(e.values)*3)
		builder.WriteString(column)
		builder.WriteByte(' ')
		builder.WriteString(e.op)
		builder.WriteString(" (")
		for index := range e.values {
			if index > 0 {
				builder.WriteString(", ")
			}
			builder.WriteByte('?')
		}
		builder.WriteByte(')')
		return builder.String(), e.values, nil
	case "BETWEEN":
		return column + " BETWEEN ? AND ?", e.values, nil
	default:
		return column + " " + e.op + " ?", e.values, nil
	}
}

type groupExpr struct {
	op    string
	exprs []Expr
}

func (e groupExpr) render(dialect Dialect) (string, []any, error) {
	if len(e.exprs) == 0 {
		if e.op == "AND" {
			return "1 = 1", nil, nil
		}
		return "1 = 0", nil, nil
	}
	var builder strings.Builder
	builder.Grow(len(e.exprs) * 16)
	arguments := make([]any, 0, len(e.exprs))
	for index, expression := range e.exprs {
		if expression == nil {
			return "", nil, fmt.Errorf("%w: expression is nil", ErrInvalidArgument)
		}
		part, args, err := expression.render(dialect)
		if err != nil {
			return "", nil, err
		}
		if index > 0 {
			builder.WriteByte(' ')
			builder.WriteString(e.op)
			builder.WriteByte(' ')
		}
		builder.WriteByte('(')
		builder.WriteString(part)
		builder.WriteByte(')')
		arguments = append(arguments, args...)
	}
	return builder.String(), arguments, nil
}

type notExpr struct{ expr Expr }

func (e notExpr) render(dialect Dialect) (string, []any, error) {
	if e.expr == nil {
		return "", nil, fmt.Errorf("%w: expression is nil", ErrInvalidArgument)
	}
	query, args, err := e.expr.render(dialect)
	if err != nil {
		return "", nil, err
	}
	var builder strings.Builder
	builder.Grow(len(query) + len("NOT ()"))
	builder.WriteString("NOT (")
	builder.WriteString(query)
	builder.WriteByte(')')
	return builder.String(), args, nil
}

// And 组合多个必须同时成立的条件。
func And(exprs ...Expr) Expr { return groupExpr{op: "AND", exprs: cloneSlice(exprs)} }

// Or 组合多个任意一个成立的条件。
func Or(exprs ...Expr) Expr { return groupExpr{op: "OR", exprs: cloneSlice(exprs)} }

// Not 对条件取反。
func Not(expr Expr) Expr { return notExpr{expr: expr} }

// Assignment 表示一个类型安全的更新赋值。
type Assignment interface {
	ownerName() string
	columnName() string
	columnKey() string
	renderAssignment(Dialect) (string, []any, error)
}

type assignment struct {
	owner  string
	column string
	op     string
	value  any
}

func (a assignment) ownerName() string  { return a.owner }
func (a assignment) columnName() string { return a.column }
func (a assignment) columnKey() string  { return ColumnKey(a.owner, a.column) }

func (a assignment) renderAssignment(dialect Dialect) (string, []any, error) {
	if a.column == "" {
		return "", nil, fmt.Errorf("%w: assignment column is empty", ErrInvalidArgument)
	}
	column := QuoteIdentifier(dialect, a.column)
	var builder strings.Builder
	builder.Grow(len(column)*2 + 8)
	builder.WriteString(column)
	switch a.op {
	case "NULL":
		builder.WriteString(" = NULL")
		return builder.String(), nil, nil
	case "+", "-":
		builder.WriteString(" = ")
		builder.WriteString(column)
		builder.WriteByte(' ')
		builder.WriteString(a.op)
		builder.WriteString(" ?")
		return builder.String(), []any{a.value}, nil
	default:
		builder.WriteString(" = ?")
		return builder.String(), []any{a.value}, nil
	}
}

// Order 表示一个只引用生成列的排序规则。
type Order interface {
	ownerName() string
	columnName() string
	columnKey() string
	renderOrder(Dialect) (string, error)
}

type order struct {
	owner  string
	column string
	desc   bool
}

func (o order) ownerName() string  { return o.owner }
func (o order) columnName() string { return o.column }
func (o order) columnKey() string  { return ColumnKey(o.owner, o.column) }

func (o order) renderOrder(dialect Dialect) (string, error) {
	if o.column == "" {
		return "", fmt.Errorf("%w: order column is empty", ErrInvalidArgument)
	}
	direction := " ASC"
	if o.desc {
		direction = " DESC"
	}
	column := QuoteIdentifier(dialect, o.column)
	var builder strings.Builder
	builder.Grow(len(column) + len(direction))
	builder.WriteString(column)
	builder.WriteString(direction)
	return builder.String(), nil
}

// ColumnKey 构造生成列的模型内唯一键。
func ColumnKey(owner, column string) string { return owner + "\x00" + column }

// Column 提供所有列共有的等值、集合、赋值和排序操作。
type Column[T any] struct {
	owner string
	name  string
}

func NewColumn[T any](name string, owner ...string) Column[T] {
	column := Column[T]{name: name}
	if len(owner) > 0 {
		column.owner = owner[0]
	}
	return column
}
func (c Column[T]) Eq(value T) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "=", values: []any{value}}
}
func (c Column[T]) Ne(value T) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "<>", values: []any{value}}
}
func (c Column[T]) In(values ...T) Expr    { return listExpr(c.owner, c.name, "IN", values) }
func (c Column[T]) NotIn(values ...T) Expr { return listExpr(c.owner, c.name, "NOT IN", values) }
func (c Column[T]) Set(value T) Assignment {
	return assignment{owner: c.owner, column: c.name, value: value}
}
func (c Column[T]) Asc() Order  { return order{owner: c.owner, column: c.name} }
func (c Column[T]) Desc() Order { return order{owner: c.owner, column: c.name, desc: true} }

func listExpr[T any](owner, column, operator string, values []T) Expr {
	arguments := make([]any, len(values))
	for index := range values {
		arguments[index] = values[index]
	}
	return comparisonExpr{owner: owner, column: column, op: operator, values: arguments}
}

// Ordered 是可使用大小比较的 Go 标量类型。
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

// Number 是可使用加减更新的 Go 数值类型。
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64
}

type OrderedColumn[T Ordered] struct{ Column[T] }

func NewOrderedColumn[T Ordered](name string, owner ...string) OrderedColumn[T] {
	return OrderedColumn[T]{Column: NewColumn[T](name, owner...)}
}
func (c OrderedColumn[T]) Gt(value T) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: ">", values: []any{value}}
}
func (c OrderedColumn[T]) Gte(value T) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: ">=", values: []any{value}}
}
func (c OrderedColumn[T]) Lt(value T) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "<", values: []any{value}}
}
func (c OrderedColumn[T]) Lte(value T) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "<=", values: []any{value}}
}
func (c OrderedColumn[T]) Between(a, b T) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "BETWEEN", values: []any{a, b}}
}

type NumericColumn[T Number] struct{ OrderedColumn[T] }

func NewNumericColumn[T Number](name string, owner ...string) NumericColumn[T] {
	return NumericColumn[T]{OrderedColumn: NewOrderedColumn[T](name, owner...)}
}
func (c NumericColumn[T]) Add(value T) Assignment {
	return assignment{owner: c.owner, column: c.name, op: "+", value: value}
}
func (c NumericColumn[T]) Sub(value T) Assignment {
	return assignment{owner: c.owner, column: c.name, op: "-", value: value}
}

type StringColumn[T ~string] struct{ OrderedColumn[T] }

func NewStringColumn[T ~string](name string, owner ...string) StringColumn[T] {
	return StringColumn[T]{OrderedColumn: NewOrderedColumn[T](name, owner...)}
}
func (c StringColumn[T]) Like(pattern string) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "LIKE", values: []any{pattern}}
}

type TimeColumn struct{ Column[time.Time] }

func NewTimeColumn(name string, owner ...string) TimeColumn {
	return TimeColumn{Column: NewColumn[time.Time](name, owner...)}
}
func (c TimeColumn) Gt(value time.Time) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: ">", values: []any{value}}
}
func (c TimeColumn) Gte(value time.Time) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: ">=", values: []any{value}}
}
func (c TimeColumn) Lt(value time.Time) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "<", values: []any{value}}
}
func (c TimeColumn) Lte(value time.Time) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "<=", values: []any{value}}
}
func (c TimeColumn) Between(a, b time.Time) Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "BETWEEN", values: []any{a, b}}
}

// NullableColumn 为 nullable 列增加 NULL 条件和赋值。
type NullableColumn[T any] struct{ Column[T] }

func NewNullableColumn[T any](name string, owner ...string) NullableColumn[T] {
	return NullableColumn[T]{Column: NewColumn[T](name, owner...)}
}
func (c NullableColumn[T]) IsNull() Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "IS NULL"}
}
func (c NullableColumn[T]) IsNotNull() Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "IS NOT NULL"}
}
func (c NullableColumn[T]) SetNull() Assignment {
	return assignment{owner: c.owner, column: c.name, op: "NULL"}
}

type NullableNumericColumn[T Number] struct{ NumericColumn[T] }

func NewNullableNumericColumn[T Number](name string, owner ...string) NullableNumericColumn[T] {
	return NullableNumericColumn[T]{NumericColumn: NewNumericColumn[T](name, owner...)}
}
func (c NullableNumericColumn[T]) IsNull() Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "IS NULL"}
}
func (c NullableNumericColumn[T]) IsNotNull() Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "IS NOT NULL"}
}
func (c NullableNumericColumn[T]) SetNull() Assignment {
	return assignment{owner: c.owner, column: c.name, op: "NULL"}
}

type NullableStringColumn[T ~string] struct{ StringColumn[T] }

func NewNullableStringColumn[T ~string](name string, owner ...string) NullableStringColumn[T] {
	return NullableStringColumn[T]{StringColumn: NewStringColumn[T](name, owner...)}
}
func (c NullableStringColumn[T]) IsNull() Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "IS NULL"}
}
func (c NullableStringColumn[T]) IsNotNull() Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "IS NOT NULL"}
}
func (c NullableStringColumn[T]) SetNull() Assignment {
	return assignment{owner: c.owner, column: c.name, op: "NULL"}
}

type NullableTimeColumn struct{ TimeColumn }

func NewNullableTimeColumn(name string, owner ...string) NullableTimeColumn {
	return NullableTimeColumn{TimeColumn: NewTimeColumn(name, owner...)}
}
func (c NullableTimeColumn) IsNull() Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "IS NULL"}
}
func (c NullableTimeColumn) IsNotNull() Expr {
	return comparisonExpr{owner: c.owner, column: c.name, op: "IS NOT NULL"}
}
func (c NullableTimeColumn) SetNull() Assignment {
	return assignment{owner: c.owner, column: c.name, op: "NULL"}
}

// BuildWhere 渲染并校验查询条件。
func BuildWhere(dialect Dialect, exprs []Expr, allowed map[string]struct{}) (string, []any, error) {
	if len(exprs) == 0 {
		return "", nil, nil
	}
	// 列白名单由生成代码提供，防止跨表表达式被误用。
	if err := validateExprColumns(exprs, allowed); err != nil {
		return "", nil, err
	}
	query, args, err := And(exprs...).render(dialect)
	if err != nil {
		return "", nil, err
	}
	var builder strings.Builder
	builder.Grow(len(query) + len(" WHERE "))
	builder.WriteString(" WHERE ")
	builder.WriteString(query)
	return builder.String(), args, nil
}

func validateExprColumns(exprs []Expr, allowed map[string]struct{}) error {
	for _, expression := range exprs {
		switch value := expression.(type) {
		case comparisonExpr:
			if !isAllowedColumn(value.owner, value.column, allowed) {
				return fmt.Errorf("%w: column %q does not belong to model", ErrInvalidArgument, value.column)
			}
		case groupExpr:
			if err := validateExprColumns(value.exprs, allowed); err != nil {
				return err
			}
		case notExpr:
			if err := validateExprColumns([]Expr{value.expr}, allowed); err != nil {
				return err
			}
		case nil:
			return fmt.Errorf("%w: expression is nil", ErrInvalidArgument)
		default:
			return fmt.Errorf("%w: unsupported expression", ErrInvalidArgument)
		}
	}
	return nil
}

type predicateClass uint8

const (
	predicateConditional predicateClass = iota
	predicateAlwaysFalse
	predicateAlwaysTrue
)

// IsUnrestricted 判断条件是否在静态上恒为真，供更新和删除保护使用。
func IsUnrestricted(exprs []Expr) bool {
	if len(exprs) == 0 {
		return true
	}
	return classifyPredicate(groupExpr{op: "AND", exprs: exprs}) == predicateAlwaysTrue
}

func classifyPredicate(expr Expr) predicateClass {
	switch value := expr.(type) {
	case comparisonExpr:
		if len(value.values) == 0 {
			switch value.op {
			case "IN":
				return predicateAlwaysFalse
			case "NOT IN":
				return predicateAlwaysTrue
			}
		}
		return predicateConditional
	case notExpr:
		switch classifyPredicate(value.expr) {
		case predicateAlwaysTrue:
			return predicateAlwaysFalse
		case predicateAlwaysFalse:
			return predicateAlwaysTrue
		default:
			return predicateConditional
		}
	case groupExpr:
		if len(value.exprs) == 0 {
			if value.op == "AND" {
				return predicateAlwaysTrue
			}
			return predicateAlwaysFalse
		}
		allTrue := true
		allFalse := true
		for _, child := range value.exprs {
			class := classifyPredicate(child)
			if value.op == "AND" && class == predicateAlwaysFalse {
				return predicateAlwaysFalse
			}
			if value.op == "OR" && class == predicateAlwaysTrue {
				return predicateAlwaysTrue
			}
			allTrue = allTrue && class == predicateAlwaysTrue
			allFalse = allFalse && class == predicateAlwaysFalse
		}
		if allTrue {
			return predicateAlwaysTrue
		}
		if allFalse {
			return predicateAlwaysFalse
		}
		return predicateConditional
	default:
		return predicateConditional
	}
}

func isAllowedColumn(owner, column string, allowed map[string]struct{}) bool {
	if _, ok := allowed[ColumnKey(owner, column)]; ok {
		return true
	}
	// 兼容直接使用运行时列构造器的场景；生成列始终带 owner。
	if owner == "" {
		_, ok := allowed[column]
		return ok
	}
	return false
}

// BuildAssignments 渲染并校验更新赋值。
func BuildAssignments(dialect Dialect, assignments []Assignment, allowed map[string]struct{}) (string, []any, error) {
	if len(assignments) == 0 {
		return "", nil, fmt.Errorf("%w: assignments are empty", ErrInvalidArgument)
	}
	var builder strings.Builder
	builder.Grow(len(assignments) * 16)
	args := make([]any, 0, len(assignments))
	seen := make(map[string]struct{}, len(assignments))
	for _, item := range assignments {
		if item == nil {
			return "", nil, fmt.Errorf("%w: assignment is nil", ErrInvalidArgument)
		}
		column := item.columnName()
		key := item.columnKey()
		if !isAllowedColumn(item.ownerName(), column, allowed) {
			return "", nil, fmt.Errorf("%w: column %q does not belong to model", ErrInvalidArgument, column)
		}
		if _, ok := seen[key]; ok {
			return "", nil, fmt.Errorf("%w: column %q is assigned more than once", ErrInvalidArgument, column)
		}
		seen[key] = struct{}{}
		part, values, err := item.renderAssignment(dialect)
		if err != nil {
			return "", nil, err
		}
		if builder.Len() > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(part)
		args = append(args, values...)
	}
	return builder.String(), args, nil
}

// BuildOrder 渲染并校验排序。
func BuildOrder(dialect Dialect, orders []Order, allowed map[string]struct{}) (string, error) {
	if len(orders) == 0 {
		return "", nil
	}
	var builder strings.Builder
	builder.Grow(len(orders) * 16)
	builder.WriteString(" ORDER BY ")
	for _, item := range orders {
		if item == nil {
			return "", fmt.Errorf("%w: order is nil", ErrInvalidArgument)
		}
		column := item.columnName()
		if !isAllowedColumn(item.ownerName(), column, allowed) {
			return "", fmt.Errorf("%w: column %q does not belong to model", ErrInvalidArgument, item.columnName())
		}
		part, err := item.renderOrder(dialect)
		if err != nil {
			return "", err
		}
		if index := builder.Len(); index > len(" ORDER BY ") {
			builder.WriteString(", ")
		}
		builder.WriteString(part)
	}
	return builder.String(), nil
}

// BuildPagination 渲染分页语句并返回对应参数。
func BuildPagination(dialect Dialect, limit, offset *int) (string, []any, error) {
	if limit != nil && *limit < 0 || offset != nil && *offset < 0 {
		return "", nil, fmt.Errorf("%w: limit and offset must not be negative", ErrInvalidArgument)
	}
	if limit == nil && offset == nil {
		return "", nil, nil
	}
	if limit != nil && offset == nil {
		return " LIMIT ?", []any{*limit}, nil
	}
	if limit != nil {
		return " LIMIT ? OFFSET ?", []any{*limit, *offset}, nil
	}
	if dialect == DialectMySQL {
		return " LIMIT 18446744073709551615 OFFSET ?", []any{*offset}, nil
	}
	return " OFFSET ?", []any{*offset}, nil
}

// BindNamedIn 按 named、IN 展开、方言重绑定的顺序处理自定义 SQL 参数。
func BindNamedIn(executor Executor, query string, argument any) (string, []any, error) {
	if _, err := DetectDialect(executor); err != nil {
		return "", nil, err
	}
	bound, arguments, err := sqlx.Named(query, argument)
	if err != nil {
		return "", nil, fmt.Errorf("bind named query: %w", err)
	}
	bound, arguments, err = sqlx.In(bound, arguments...)
	if err != nil {
		return "", nil, fmt.Errorf("expand IN query: %w", err)
	}
	return executor.Rebind(bound), arguments, nil
}

// AssignAutoID 将 database/sql 返回的自增 ID 安全写入整数目标。
func AssignAutoID(destination any, id int64) error {
	value := reflect.ValueOf(destination)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("%w: auto ID destination must be a non-nil pointer", ErrInvalidArgument)
	}
	element := value.Elem()
	switch element.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if element.OverflowInt(id) {
			return fmt.Errorf("%w: auto ID %d overflows destination", ErrInvalidArgument, id)
		}
		element.SetInt(id)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if id < 0 || element.OverflowUint(uint64(id)) {
			return fmt.Errorf("%w: auto ID %d overflows destination", ErrInvalidArgument, id)
		}
		element.SetUint(uint64(id))
	default:
		return fmt.Errorf("%w: auto ID destination is not an integer", ErrInvalidArgument)
	}
	return nil
}

func cloneSlice[T any](values []T) []T { return append([]T(nil), values...) }
