// Package sqlxgen 实现 sqlxgen 命令使用的模型扫描与代码生成。
package sqlxgen

import (
	"fmt"
	"os"
	"path/filepath"
)

// Generate 扫描、校验并生成 Go 与双方言 DDL 文件。
func Generate(config GenerateConfig) (*Result, error) {
	packages, err := Scan(config)
	if err != nil {
		return nil, err
	}
	mysql, postgres, err := GenerateDDL(packages)
	if err != nil {
		return nil, err
	}
	return &Result{Packages: packages, MySQLDDL: mysql, PGDDL: postgres}, nil
}

// Write 在全部产物生成成功后，以原子替换方式写入磁盘。
func Write(config GenerateConfig, result *Result) error {
	if result == nil {
		return fmt.Errorf("result is nil")
	}
	outputs := make(map[string][]byte)
	for _, pkg := range result.Packages {
		source, err := GenerateGo(pkg)
		if err != nil {
			return err
		}
		outputs[filepath.Join(pkg.Dir, generatedFileName)] = source
	}
	if config.DDLOut != "" {
		ddlDir := config.DDLOut
		if !filepath.IsAbs(ddlDir) {
			base := config.Dir
			if base == "" {
				base = "."
			}
			ddlDir = filepath.Join(base, ddlDir)
		}
		outputs[filepath.Join(ddlDir, "schema.mysql.sql")] = result.MySQLDDL
		outputs[filepath.Join(ddlDir, "schema.postgres.sql")] = result.PGDDL
	}
	for path, content := range outputs {
		if err := atomicWrite(path, content); err != nil {
			return err
		}
	}
	return nil
}

// Run 执行一次完整生成。
func Run(config GenerateConfig) (*Result, error) {
	result, err := Generate(config)
	if err != nil {
		return nil, err
	}
	if err := Write(config, result); err != nil {
		return nil, err
	}
	return result, nil
}

func atomicWrite(path string, content []byte) (err error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create output directory %s: %w", directory, err)
	}
	temporary, err := os.CreateTemp(directory, ".sqlxgen-*")
	if err != nil {
		return fmt.Errorf("create temporary output for %s: %w", path, err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}()
	if _, err := temporary.Write(content); err != nil {
		return fmt.Errorf("write temporary output for %s: %w", path, err)
	}
	if err := temporary.Chmod(0o644); err != nil {
		return fmt.Errorf("set output permissions for %s: %w", path, err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync output %s: %w", path, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close output %s: %w", path, err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("replace output %s: %w", path, err)
	}
	return nil
}
