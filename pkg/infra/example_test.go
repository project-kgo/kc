package infra_test

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/project-kgo/kc/pkg/infra"
	"github.com/project-kgo/kc/pkg/resource"
)

func ExampleInit() {
	err := infra.Init(context.Background(), infra.Config{
		Data: map[string]infra.DataConfig{
			"example-main": {
				Type:      infra.DataTypeMySQL,
				DSN:       "user:pass@tcp(127.0.0.1:3306)/app",
				SkipCheck: true,
				SQL: &infra.SQLConfig{
					MaxOpenConns: 20,
					MaxIdleConns: 5,
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}

	db, ok := resource.Get[*sqlx.DB]("example-main")
	fmt.Println(ok)
	_ = db.Close()
	// Output: true
}
