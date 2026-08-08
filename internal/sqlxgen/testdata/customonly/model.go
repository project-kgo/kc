package customonly

import "context"

// sqlxgen:queries file=query.sql
type MaintenanceQueries interface {
	Purge(context.Context) error
}
