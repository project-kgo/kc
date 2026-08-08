package sqlxgen

import (
	"go/types"
	"path/filepath"
)

const (
	runtimePackagePath = "github.com/project-kgo/kc/pkg/sqlxgen"
	generatedFileName  = "zz_sqlxgen.gen.go"
	generatorVersion   = "v0.1.0"
)

type PackageSpec struct {
	Name          string
	Path          string
	Dir           string
	Models        []*ModelSpec
	CustomQueries []*CustomQuerySpec
}

type ModelSpec struct {
	Package *PackageSpec
	Name    string
	Schema  string
	Table   string
	Fields  []*FieldSpec
	Keys    []*FieldSpec
	Auto    *FieldSpec
}

type FieldKind uint8

const (
	FieldOther FieldKind = iota
	FieldString
	FieldBool
	FieldInteger
	FieldFloat
	FieldTime
	FieldBytes
	FieldJSON
)

type FieldSpec struct {
	Owner        string
	GoName       string
	Access       string
	Column       string
	Type         types.Type
	ValueType    types.Type
	Kind         FieldKind
	Nullable     bool
	PrimaryKey   bool
	Auto         bool
	Size         int
	Precision    int
	Scale        int
	Default      string
	Index        string
	Unique       string
	References   string
	OnDelete     string
	MySQLType    string
	PostgresType string
}

type CustomQuerySpec struct {
	Package       *PackageSpec
	Name          string
	SQLFile       string
	Methods       []*CustomMethodSpec
	InterfaceType *types.Interface
}

func (s *CustomQuerySpec) resolvedSQLFile() string {
	if filepath.IsAbs(s.SQLFile) {
		return s.SQLFile
	}
	return filepath.Join(s.Package.Dir, s.SQLFile)
}

type CustomMethodMode uint8

const (
	CustomExec CustomMethodMode = iota + 1
	CustomRowsAffected
	CustomOne
	CustomMany
)

type CustomMethodSpec struct {
	Name       string
	SQL        string
	Signature  *types.Signature
	ParamType  types.Type
	ResultType types.Type
	Mode       CustomMethodMode
	Pointer    bool
}

type GenerateConfig struct {
	Dir      string
	Patterns []string
	DDLOut   string
}

type Result struct {
	Packages []*PackageSpec
	MySQLDDL []byte
	PGDDL    []byte
}
