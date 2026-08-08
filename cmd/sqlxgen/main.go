package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	generator "github.com/project-kgo/kc/internal/sqlxgen"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "sqlxgen:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 || arguments[0] != "generate" {
		return fmt.Errorf("usage: sqlxgen generate [-dir .] [-patterns ./...] [-ddl-out ./schema/generated]")
	}
	flags := flag.NewFlagSet("sqlxgen generate", flag.ContinueOnError)
	directory := flags.String("dir", ".", "待扫描 Go 模块的工作目录")
	patterns := flags.String("patterns", "./...", "逗号分隔的 go/packages 模式")
	ddlOut := flags.String("ddl-out", "./schema/generated", "DDL 输出目录；留空时不写 DDL")
	if err := flags.Parse(arguments[1:]); err != nil {
		return err
	}
	var packagePatterns []string
	for _, pattern := range strings.Split(*patterns, ",") {
		if pattern = strings.TrimSpace(pattern); pattern != "" {
			packagePatterns = append(packagePatterns, pattern)
		}
	}
	if len(packagePatterns) == 0 {
		return fmt.Errorf("patterns must not be empty")
	}
	result, err := generator.Run(generator.GenerateConfig{Dir: *directory, Patterns: packagePatterns, DDLOut: *ddlOut})
	if err != nil {
		return err
	}
	models := 0
	queries := 0
	for _, pkg := range result.Packages {
		models += len(pkg.Models)
		queries += len(pkg.CustomQueries)
	}
	fmt.Fprintf(os.Stdout, "generated %d models and %d custom query interfaces in %d packages\n", models, queries, len(result.Packages))
	return nil
}
