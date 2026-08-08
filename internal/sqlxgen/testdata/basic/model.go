package basic

import (
	"context"
	"time"

	gen "github.com/project-kgo/kc/pkg/sqlxgen"
)

type Status string

type AuditFields struct {
	CreatedAt time.Time  `db:"created_at" sqlxgen:"default=CURRENT_TIMESTAMP"`
	DeletedAt *time.Time `db:"deleted_at"`
}

type User struct {
	gen.Model `sqlxgen:"table=users;schema=app"`
	AuditFields

	ID       int64  `db:"id" sqlxgen:"pk;auto"`
	TenantID int64  `db:"tenant_id" sqlxgen:"index=idx_users_tenant"`
	Email    string `db:"email" sqlxgen:"size=320;unique=uix_users_email"`
	Status   Status `db:"status" sqlxgen:"size=32;default='active'"`
}

type Membership struct {
	gen.Model `sqlxgen:"table=memberships;schema=app"`

	TenantID int64 `db:"tenant_id" sqlxgen:"pk"`
	UserID   int64 `db:"user_id" sqlxgen:"pk;references=app.users(id);on_delete=CASCADE"`
}

type FindActiveParams struct {
	TenantID int64  `db:"tenant_id"`
	Status   Status `db:"status"`
}

// sqlxgen:queries file=queries/users.sql
type UserCustomQueries interface {
	FindActive(context.Context, FindActiveParams) ([]User, error)
	CountAll(context.Context) (int64, error)
	Disable(context.Context, FindActiveParams) (int64, error)
}
