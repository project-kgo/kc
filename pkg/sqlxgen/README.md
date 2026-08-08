# sqlxgen

`sqlxgen` 是建立在 [`sqlx`](https://github.com/jmoiron/sqlx) 之上的模型代码生成器。它以 Go 结构体为输入，生成：

- 单表强类型 CRUD 和查询 DSL；
- MySQL、PostgreSQL 初始建表 DDL；
- Go 接口绑定原生 SQL 文件的强类型实现；
- 可同时使用 `*sqlx.DB` 和 `*sqlx.Tx` 的数据访问代码。

它不是完整 ORM，不负责关联预加载、Hook、软删除、自动迁移或 schema diff。复杂联表、数据库专用函数和自定义投影应使用原生 SQL 接口。

## 快速开始

### 1. 定义模型

只有非指针匿名嵌入 `sqlxgen.Model` 的结构体才会触发生成：

```go
package model

import (
	"time"

	"github.com/project-kgo/kc/pkg/sqlxgen"
)

type UserStatus string

const (
	UserStatusActive   UserStatus = "active"
	UserStatusDisabled UserStatus = "disabled"
)

type User struct {
	sqlxgen.Model `sqlxgen:"table=users;schema=app"`

	ID        int64      `db:"id" sqlxgen:"pk;auto"`
	TenantID  int64      `db:"tenant_id" sqlxgen:"index=idx_users_tenant_status"`
	Email     string     `db:"email" sqlxgen:"size=320;unique=uix_users_email"`
	Status    UserStatus `db:"status" sqlxgen:"size=32;default='active';index=idx_users_tenant_status"`
	CreatedAt time.Time  `db:"created_at" sqlxgen:"default=CURRENT_TIMESTAMP"`
	DeletedAt *time.Time `db:"deleted_at"`
}
```

`sqlxgen.Model` 只是零大小的生成标记，不会隐式增加 ID、时间或软删除字段。

### 2. 运行生成器

在 Go 模块根目录运行：

```shell
go run github.com/project-kgo/kc/cmd/sqlxgen generate \
  -patterns=./...
```

也可以先安装命令：

```shell
go install github.com/project-kgo/kc/cmd/sqlxgen@latest
sqlxgen generate -patterns=./...
```

默认只生成 Go 代码：

```text
model/
├── user.go
└── zz_sqlxgen.gen.go
```

需要同时生成初始 DDL 时显式指定输出目录：

```shell
sqlxgen generate -patterns=./... -ddl-out=./schema/generated
```

此时额外生成：

```text
schema/generated/
├── schema.mysql.sql
└── schema.postgres.sql
```

生成的 Go 文件与模型位于同一个 package，不需要额外的 query package。

### 3. 使用生成查询

```go
q := model.NewQueries(db)
u := q.User

user, err := u.
	Where(
		u.TenantID.Eq(tenantID),
		u.Status.Eq(model.UserStatusActive),
	).
	Order(u.CreatedAt.Desc()).
	First(ctx)
```

`NewQueries` 接受 `sqlxgen.Executor`。当前内置驱动为：

- `*sqlx.DB`，驱动名为 `mysql` 或 `pgx`；
- `*sqlx.Tx`，事务会继承所属 DB 的驱动名。

其他驱动会返回 `sqlxgen.ErrUnsupportedDriver`。

## 模型规则

### Model 标记

Model 标记支持以下选项：

| 选项 | 必填 | 说明 | 示例 |
| --- | --- | --- | --- |
| `table` | 是 | 数据库表名 | `table=users` |
| `schema` | 否 | PostgreSQL schema 或 MySQL database 名 | `schema=app` |

格式使用分号分隔：

```go
sqlxgen.Model `sqlxgen:"table=users;schema=app"`
```

约束：

- 必须直接嵌入 `sqlxgen.Model`，`*sqlxgen.Model` 会报错；
- 按类型身份识别 Model，因此 import alias 不影响扫描；
- `table`、`schema` 和所有列/索引标识符必须匹配 `[A-Za-z_][A-Za-z0-9_]*`；
- 每个模型至少需要一个 `pk`；
- 同一轮扫描中不能有两个模型映射到同一个 `schema.table`。

### 字段与 `db` 标签

每个导出的数据库字段必须显式声明 `db` 标签：

```go
Name string `db:"name"`
```

字段处理规则：

- `db:"-"` 表示忽略字段；
- 未导出且没有 `db` 标签的字段会被忽略；
- 未导出字段声明数据库列会报错；
- 匿名嵌入结构体在没有 `db` 标签时会递归展开；
- 重复列名或展开后重复的 Go 字段名会报错；
- `db` 标签逗号后的选项不参与列名，例如 `db:"name,omitempty"` 的列名仍是 `name`。

嵌入公共字段示例：

```go
type AuditFields struct {
	CreatedAt time.Time  `db:"created_at" sqlxgen:"default=CURRENT_TIMESTAMP"`
	UpdatedAt time.Time  `db:"updated_at"`
	DeletedAt *time.Time `db:"deleted_at"`
}

type User struct {
	sqlxgen.Model `sqlxgen:"table=users"`
	AuditFields

	ID int64 `db:"id" sqlxgen:"pk;auto"`
}
```

## 字段 `sqlxgen` 标签

字段选项同样使用分号分隔。标志选项推荐直接写成 `pk`；为兼容生成配置，也接受 `pk=true`，但不接受 `pk=false` 或其他值。

| 标签 | 参数 | 作用 | 备注 |
| --- | --- | --- | --- |
| `pk` | 无 | 声明主键列 | 支持多个字段组成联合主键；主键不能 nullable |
| `auto` | 无 | 声明数据库自增/identity 列 | 必须同时是 `pk`，且模型只能有一个主键字段 |
| `null` | 无 | 强制生成 nullable DDL 和 nullable 列 DSL | 建议 Go 字段同时使用指针或 `sql.Null[T]` |
| `notnull` | 无 | 强制生成 NOT NULL | 不能与 `null` 同时使用 |
| `size` | 正整数 | 字符串长度 | `size=320` 生成 `VARCHAR(320)` |
| `precision` | 正整数 | decimal 总精度 | 与浮点字段配合使用 |
| `scale` | 非负整数 | decimal 小数位数 | 必须同时设置 `precision`，且不能大于 precision |
| `default` | SQL 表达式 | 生成数据库 DEFAULT | 原样写入 DDL，不会自动解释或转义 |
| `index` | 索引名 | 生成普通索引 | 多个字段使用相同名称会生成联合索引 |
| `unique` | 索引名 | 生成唯一索引 | 多个字段使用相同名称会生成联合唯一索引 |
| `references` | `table(column)` | 生成外键 | 也支持 `schema.table(column)` |
| `on_delete` | 删除动作 | 设置外键删除行为 | 支持 `CASCADE`、`RESTRICT`、`SET NULL`、`NO ACTION` |
| `mysql_type` | SQL 类型 | 覆盖 MySQL 类型映射 | 只写类型，例如 `DECIMAL(20,6)` |
| `postgres_type` | SQL 类型 | 覆盖 PostgreSQL 类型映射 | 只写类型，例如 `UUID` |

完整示例：

```go
type Order struct {
	sqlxgen.Model `sqlxgen:"table=orders;schema=app"`

	ID       int64  `db:"id" sqlxgen:"pk;auto"`
	TenantID int64  `db:"tenant_id" sqlxgen:"index=idx_orders_tenant_status;references=app.tenants(id);on_delete=CASCADE"`
	Status   string `db:"status" sqlxgen:"size=32;default='pending';index=idx_orders_tenant_status"`
	Amount   string `db:"amount" sqlxgen:"mysql_type=DECIMAL(20,6);postgres_type=NUMERIC(20,6)"`
}
```

### 主键与 `auto`

单主键会按字段名生成方法。例如字段名是 `ID`：

```go
user, err := q.User.GetByID(ctx, id)
rows, err := q.User.UpdateByID(ctx, id, q.User.Status.Set(UserStatusDisabled))
rows, err := q.User.DeleteByID(ctx, id)
```

如果主键字段叫 `Code`，则对应方法是 `GetByCode`、`UpdateByCode` 和 `DeleteByCode`。

联合主键会生成 `<Model>Key`：

```go
type Membership struct {
	sqlxgen.Model `sqlxgen:"table=memberships"`

	TenantID int64 `db:"tenant_id" sqlxgen:"pk"`
	UserID   int64 `db:"user_id" sqlxgen:"pk"`
}

membership, err := q.Membership.GetByKey(ctx, model.MembershipKey{
	TenantID: tenantID,
	UserID:   userID,
})
```

`auto` 的限制：

- 只能用于非 nullable 整数主键；
- 不支持联合主键；
- 不能同时声明 `default`；
- 为保证 PostgreSQL identity 可移植性，不支持 `uint`、`uint64` 和 `uintptr`；
- MySQL 使用 `LastInsertId` 回填，PostgreSQL 使用 `RETURNING` 回填。

主键不支持数据库 `default`。UUID 等键建议在 Go 中生成后写入。

### NULL 规则

以下 Go 类型默认识别为 nullable：

- 指针，例如 `*string`、`*time.Time`；
- `sql.Null[T]`；
- `sql.NullString`、`sql.NullBool`、`sql.NullByte`；
- `sql.NullInt16`、`sql.NullInt32`、`sql.NullInt64`；
- `sql.NullFloat64`、`sql.NullTime`。

nullable 列的条件和赋值使用底层值类型：

```go
// DeletedAt 是 *time.Time，但 Eq/Set 接收 time.Time。
query := q.User.Where(q.User.DeletedAt.IsNull())
query = q.User.Where(q.User.DeletedAt.Eq(deletedAt))

rows, err := query.Update(ctx, q.User.DeletedAt.SetNull())
```

虽然 `null` 可以覆盖推导结果，但普通值字段无法可靠接收 SQL NULL，实际使用时应优先选择指针或 `sql.Null[T]`。

### 索引与外键

多个字段复用相同的 `index` 或 `unique` 名称时，会按字段在结构体中的顺序生成联合索引：

```go
TenantID int64  `db:"tenant_id" sqlxgen:"index=idx_tenant_status"`
Status   string `db:"status" sqlxgen:"index=idx_tenant_status"`
```

生成：

```sql
CREATE INDEX "idx_tenant_status" ON "users" ("tenant_id", "status");
```

限制：

- 同一个索引名不能同时声明为普通索引和唯一索引；
- 为兼容 PostgreSQL，同一 schema 内不同模型的索引名必须唯一；
- `on_delete` 必须与 `references` 一起使用；
- `SET NULL` 要求当前字段 nullable；
- 暂不支持复合外键和 `ON UPDATE`。

## Go 类型与 DDL 映射

命名类型会按底层的 string、bool、整数或浮点类型映射。其他自定义类型必须同时设置 `mysql_type` 和 `postgres_type`。

| Go 类型 | MySQL | PostgreSQL |
| --- | --- | --- |
| `string` | `VARCHAR(255)` | `VARCHAR(255)` |
| `string` + `size=N` | `VARCHAR(N)` | `VARCHAR(N)` |
| `bool` | `BOOLEAN` | `BOOLEAN` |
| `int8` | `TINYINT` | `SMALLINT` |
| `uint8` | `TINYINT UNSIGNED` | `SMALLINT` |
| `int16` | `SMALLINT` | `SMALLINT` |
| `uint16` | `SMALLINT UNSIGNED` | `INTEGER` |
| `int32` | `INTEGER` | `INTEGER` |
| `uint32` | `INTEGER UNSIGNED` | `BIGINT` |
| `int`、`int64` | `BIGINT` | `BIGINT` |
| `uint`、`uint64`、`uintptr` | `BIGINT UNSIGNED` | `NUMERIC(20,0)` |
| `float32` | `REAL` | `REAL` |
| `float64` | `DOUBLE` | `DOUBLE PRECISION` |
| 浮点 + `precision=P;scale=S` | `DECIMAL(P,S)` | `DECIMAL(P,S)` |
| `time.Time` | `DATETIME(6)` | `TIMESTAMPTZ` |
| `[]byte` | `BLOB` | `BYTEA` |
| `json.RawMessage` | `JSON` | `JSONB` |

类型覆盖示例：

```go
ID uuid.UUID `db:"id" sqlxgen:"pk;mysql_type=BINARY(16);postgres_type=UUID"`
```

覆盖值只替换 SQL 类型，生成器仍会追加 `NULL/NOT NULL`、`DEFAULT` 等约束。

## 生成命令

```text
sqlxgen generate [-dir .] [-patterns ./...] [-ddl-out DIR]
```

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-dir` | `.` | Go 模块扫描工作目录 |
| `-patterns` | `./...` | 逗号分隔的 `go/packages` pattern |
| `-ddl-out` | 空 | DDL 输出目录；不指定时不生成 DDL |

相对的 `-ddl-out` 以 `-dir` 为基准解析；目标目录不存在时会自动创建。

示例：

```shell
# 扫描多个 package pattern
sqlxgen generate \
  -dir=. \
  -patterns=./internal/model,./pkg/account/model \
  -ddl-out=./migrations/generated

# 默认只生成 Go 代码
sqlxgen generate -patterns=./internal/model
```

可以在模块根目录的 Go 文件中加入：

```go
//go:generate go run github.com/project-kgo/kc/cmd/sqlxgen generate -patterns=./...
```

生成器先完成所有扫描、标签校验和 Go 格式化，再通过临时文件原子替换旧产物。生成失败时不会用不完整内容覆盖目标文件。

## 查询 DSL

### 条件

所有列都支持：

```go
u.ID.Eq(id)
u.ID.Ne(id)
u.ID.In(ids...)
u.ID.NotIn(ids...)
```

数字、字符串和 `time.Time` 列支持比较：

```go
u.ID.Gt(100)
u.ID.Gte(100)
u.ID.Lt(200)
u.ID.Lte(200)
u.ID.Between(100, 200)
```

字符串列额外支持：

```go
u.Email.Like("%@example.com")
```

nullable 列额外支持：

```go
u.DeletedAt.IsNull()
u.DeletedAt.IsNotNull()
```

组合条件：

```go
query := u.Where(
	sqlxgen.Or(
		u.Email.Like("%@example.com"),
		u.Status.Eq(UserStatusActive),
	),
	sqlxgen.Not(u.DeletedAt.IsNotNull()),
)
```

空集合有确定语义：

- `In()` 渲染为恒假条件；
- `NotIn()` 渲染为恒真条件；
- 空 `And()` 为恒真，空 `Or()` 为恒假。

更新和删除会识别静态恒真条件，不能用空 `NotIn()` 等方式绕过全表保护。

### 排序和分页

```go
users, err := u.
	Where(u.TenantID.Eq(tenantID)).
	Order(u.CreatedAt.Desc(), u.ID.Asc()).
	Limit(50).
	Offset(100).
	All(ctx)
```

- `Limit`、`Offset` 不能为负数；
- `First` 会强制使用 `LIMIT 1`；
- MySQL 的纯 Offset 查询会自动生成最大 LIMIT；
- `Count` 和 `Exists` 只使用 Where 条件，忽略 Order、Limit 和 Offset。

### 查询终结方法

```go
user, err := query.First(ctx)       // *User；无记录时保留 sql.ErrNoRows
users, err := query.All(ctx)        // []User
count, err := query.Count(ctx)      // int64
exists, err := query.Exists(ctx)    // bool
```

查询 Builder 使用值语义。每次 `Where`、`Order`、`Limit`、`Offset` 都返回新的查询值，可以安全复用基础查询：

```go
base := u.Where(u.TenantID.Eq(tenantID))
active, err := base.Where(u.Status.Eq(UserStatusActive)).All(ctx)
disabled, err := base.Where(u.Status.Eq(UserStatusDisabled)).All(ctx)
```

表达式携带模型身份，即使两个表都有名为 `id` 的列，也不能把另一个模型的列表达式传入当前 DAO。

## 新增、更新与删除

### Create

```go
user := User{
	TenantID: tenantID,
	Email:    "dev@example.com",
	Status:   UserStatusActive,
}

err := u.Create(ctx, &user)
```

插入语义：

- `auto` 列始终省略；
- 其他字段默认插入 Model 中的真实值，包括 `0`、`false` 和空字符串；
- MySQL/PostgreSQL 插入成功后会回填自增主键；
- 不会自动刷新其他 DEFAULT 或触发器生成的字段。

`default` 标签声明 DDL 默认值，并为该列生成 `UseDefault()`；它不会让 `Create` 自动忽略 Model 中的值。需要在某次插入中使用数据库默认值时必须显式调用：

```go
err := u.Create(
	ctx,
	&user,
	u.Status.UseDefault(),
	u.CreatedAt.UseDefault(),
)
```

只有声明了 `default` 的非自增列才会生成 `UseDefault()`。

需要读取数据库默认值或触发器结果时：

```go
if err := u.Create(ctx, &user, u.CreatedAt.UseDefault()); err != nil {
	return err
}
if err := u.Reload(ctx, &user); err != nil {
	return err
}
```

### Update

更新使用类型安全 Assignment，并返回受影响行数：

```go
rows, err := u.
	Where(u.ID.Eq(id)).
	Update(ctx,
		u.Status.Set(UserStatusDisabled),
	)
```

数字列额外支持原子加减：

```go
rows, err := q.Account.
	Where(q.Account.ID.Eq(accountID)).
	Update(ctx, q.Account.Balance.Add(100))
```

nullable 列支持 `SetNull()`。同一次更新不能重复赋值同一列，也不能传入其他模型的 Assignment。

### Delete 与全表保护

```go
rows, err := u.Where(u.ID.Eq(id)).Delete(ctx)
```

无条件或静态恒真的 Update/Delete 会返回 `sqlxgen.ErrUnsafeMutation`。确实需要全表操作时必须显式声明：

```go
rows, err := u.AllowAll().Delete(ctx)
```

## 事务与原生 SQLx 混用

生成器不会自动开启、提交或回滚事务：

```go
tx, err := db.BeginTxx(ctx, nil)
if err != nil {
	return err
}
defer tx.Rollback()

tq := q.With(tx)

// 生成 DAO。
if err := tq.User.Create(ctx, &user); err != nil {
	return err
}

// 同一事务中的原生 SQLx。
rawSQL := tx.Rebind("UPDATE audit_records SET processed = ? WHERE user_id = ?")
if _, err := tx.ExecContext(ctx, rawSQL, true, user.ID); err != nil {
	return err
}

return tx.Commit()
```

也可以通过 `q.Executor()` 取回当前底层执行器。

## 自定义原生 SQL

### 定义接口

在接口上使用 `sqlxgen:queries` 注释：

```go
type FindActiveParams struct {
	TenantID int64      `db:"tenant_id"`
	Status   UserStatus `db:"status"`
}

type UserSummary struct {
	ID    int64  `db:"id"`
	Email string `db:"email"`
}

// sqlxgen:queries file=queries/users.sql
type UserCustomQueries interface {
	FindActive(context.Context, FindActiveParams) ([]User, error)
	FindSummary(context.Context, FindActiveParams) (*UserSummary, error)
	CountAll(context.Context) (int64, error)
	Disable(context.Context, FindActiveParams) (int64, error)
	Purge(context.Context) error
}
```

`file` 相对于接口所在 package 的目录解析，也可以使用绝对路径。

接口约束：

- 第一个参数必须是 `context.Context`；
- 最多再接收一个参数结构体或结构体指针；
- 参数结构体的每个导出字段必须有 `db` 标签；
- SQL 中的全部命名参数必须与参数结构体的 `db` 字段完全一致；
- 无参数方法对应的 SQL 不能包含命名参数；
- 每个接口方法必须存在同名 SQL block，SQL 文件也不能有多余 block。

### 编写 SQL 文件

```sql
-- name: FindActive
SELECT id, tenant_id, email, status, created_at, deleted_at
FROM app.users
WHERE tenant_id = :tenant_id AND status = :status;

-- name: FindSummary
SELECT id, email
FROM app.users
WHERE tenant_id = :tenant_id AND status = :status
LIMIT 1;

-- name: CountAll
SELECT COUNT(*) FROM app.users;

-- name: Disable
UPDATE app.users
SET status = 'disabled'
WHERE tenant_id = :tenant_id AND status = :status;

-- name: Purge
DELETE FROM app.users;
```

每个 block 以单独一行 `-- name: MethodName` 开始，一直延续到下一个 name 标记或文件结尾。有效 SQL 不能出现在第一个 name 标记之前，重复 name 会导致生成失败。

参数绑定顺序固定为：

1. `sqlx.Named` 绑定 `:name`；
2. `sqlx.In` 展开切片；
3. 当前 DB/Tx 的 `Rebind` 转换方言占位符。

切片参数示例：

```go
type FindByIDsParams struct {
	IDs []int64 `db:"ids"`
}
```

```sql
-- name: FindByIDs
SELECT id, email FROM users WHERE id IN (:ids);
```

### 支持的接口返回值

| 返回值 | 执行行为 |
| --- | --- |
| `error` | 使用 `ExecContext`，只返回错误 |
| `(int64, error)` + 非查询 SQL | 使用 `ExecContext`，返回 `RowsAffected` |
| `(T, error)` | 使用 `sqlx.GetContext` 扫描一行或一个标量 |
| `(*T, error)` | 使用 `sqlx.GetContext` 扫描一行，无记录返回 `sql.ErrNoRows` |
| `([]T, error)` | 使用 `sqlx.SelectContext` 扫描多行 |

对于 `(int64, error)`，以 `SELECT`、`WITH` 开头或包含 `RETURNING` 的 SQL 被视为单行查询；其他 SQL 被视为返回受影响行数。

结果类型可以是 Model，也可以是只带 `db` 标签的投影结构体。生成器不会静态验证复杂 SELECT 的返回列，列名和目标类型不匹配会在运行时由 `sqlx` 返回错误。

### 创建与事务使用

生成器会为接口生成独立构造函数：

```go
custom := model.NewUserCustomQueries(db)
users, err := custom.FindActive(ctx, FindActiveParams{
	TenantID: tenantID,
	Status:   UserStatusActive,
})
```

事务中直接传入 Tx：

```go
custom := model.NewUserCustomQueries(tx)
```

## DDL 行为

生成器聚合本轮扫描到的全部模型，按 schema、表和索引名稳定排序后生成：

- `schema.mysql.sql`；
- `schema.postgres.sql`。

生成顺序为：

1. 创建所有表和主键；
2. 通过 `ALTER TABLE` 创建外键；
3. 创建普通索引和唯一索引。

DDL 仅用于初始建表：

- 不包含 `DROP` 或 `ALTER COLUMN`；
- 不比较现有数据库结构；
- 不自动执行 SQL；
- 不创建 PostgreSQL schema 或 MySQL database；
- 不提供 up/down 迁移历史。

建议将生成文件交给现有迁移工具审核并执行。模型后续发生不兼容变更时，应手工编写迁移，不要直接把新生成的完整建表文件当作增量迁移。

## 错误与边界

常用运行时错误：

| 错误 | 含义 |
| --- | --- |
| `sqlxgen.ErrInvalidArgument` | nil 执行器、非法分页、空赋值或跨模型列等参数错误 |
| `sqlxgen.ErrUnsafeMutation` | 未显式允许的全表更新或删除 |
| `sqlxgen.ErrUnsupportedDriver` | 驱动名不是 `mysql` 或 `pgx` |

查询和写入产生的数据库错误会原样返回或使用 `%w` 包装。`First`、主键查询和自定义单行查询在没有记录时保留 `sql.ErrNoRows`，可以使用 `errors.Is(err, sql.ErrNoRows)` 判断。

生成阶段会拒绝常见错误，包括：

- 缺少 table、db 或主键标签；
- 指针形式的 Model 标记；
- 重复表、列、索引或查询名称；
- 非法标识符；
- 主键 nullable；
- 非整数自增键或不可移植的自增类型；
- 外键、nullable 和默认值之间的冲突；
- 自定义 SQL 参数或返回签名不匹配。

## 当前不支持

- 类型安全 Join 和任意 Select 投影 DSL；
- 关联关系、预加载和级联保存；
- Hook、软删除、乐观锁、审计和缓存；
- Upsert 和批量 Create；
- 复合外键、`ON UPDATE` 和 Check Constraint 标签；
- 自动 migration、schema diff 或数据库反向生成 Model；
- `mysql`、`pgx` 之外的驱动。

上述场景可以继续使用 `sqlx` 或自定义 SQL 接口，不需要绕过生成 DAO 或建立第二套连接与事务体系。
