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
		return fmt.Errorf("usage: sqlxgen generate [-dir .] [-patterns ./...] [-ddl-out DIR]")
	}
	config, err := parseGenerateConfig(arguments[1:])
	if err != nil {
		return err
	}
	result, err := generator.Run(config)
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

func parseGenerateConfig(arguments []string) (generator.GenerateConfig, error) {
	flags := flag.NewFlagSet("sqlxgen generate", flag.ContinueOnError)
	directory := flags.String("dir", ".", "待扫描 Go 模块的工作目录")
	patterns := flags.String("patterns", "./...", "逗号分隔的 go/packages 模式")
	ddlOut := flags.String("ddl-out", "", "DDL 输出目录；仅在显式指定时生成 DDL")
	if err := flags.Parse(arguments); err != nil {
		return generator.GenerateConfig{}, err
	}
	var packagePatterns []string
	for _, pattern := range strings.Split(*patterns, ",") {
		if pattern = strings.TrimSpace(pattern); pattern != "" {
			packagePatterns = append(packagePatterns, pattern)
		}
	}
	if len(packagePatterns) == 0 {
		return generator.GenerateConfig{}, fmt.Errorf("patterns must not be empty")
	}
	return generator.GenerateConfig{Dir: *directory, Patterns: packagePatterns, DDLOut: *ddlOut}, nil
}
