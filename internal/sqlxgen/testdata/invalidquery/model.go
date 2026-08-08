package invalidquery

import "context"

type Params struct {
	ID int64 `db:"id"`
}

// sqlxgen:queries file=query.sql
type Queries interface {
	Find(context.Context, Params) (int64, error)
}
