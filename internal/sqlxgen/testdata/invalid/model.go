package invalid

import gen "github.com/project-kgo/kc/pkg/sqlxgen"

type Broken struct {
	*gen.Model `sqlxgen:"table=broken"`
	ID         int64 `db:"id" sqlxgen:"pk"`
}
