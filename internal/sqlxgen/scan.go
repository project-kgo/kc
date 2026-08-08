package sqlxgen

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Scan 扫描目标包中的模型和自定义查询接口。
func Scan(config GenerateConfig) ([]*PackageSpec, error) {
	patterns := config.Patterns
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	directory := config.Dir
	if directory == "" {
		directory = "."
	}
	loaded, err := packages.Load(&packages.Config{
		Dir: directory,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedDeps,
	}, patterns...)
	if err != nil {
		return nil, fmt.Errorf("load packages: %w", err)
	}
	if packages.PrintErrors(loaded) > 0 {
		return nil, fmt.Errorf("load packages: type checking failed")
	}

	var result []*PackageSpec
	tables := make(map[string]string)
	indexes := make(map[string]string)
	for _, pkg := range loaded {
		if pkg.Types == nil || len(pkg.Syntax) == 0 || strings.HasSuffix(pkg.PkgPath, "/cmd/sqlxgen") {
			continue
		}
		dir := filepath.Dir(pkg.CompiledGoFiles[0])
		spec := &PackageSpec{Name: pkg.Name, Path: pkg.PkgPath, Dir: dir}
		if err := scanPackage(pkg, spec); err != nil {
			return nil, fmt.Errorf("scan package %s: %w", pkg.PkgPath, err)
		}
		for _, model := range spec.Models {
			qualified := model.Schema + "." + model.Table
			if previous, exists := tables[qualified]; exists {
				return nil, fmt.Errorf("table %q is declared by both %s and %s.%s", qualified, previous, pkg.PkgPath, model.Name)
			}
			tables[qualified] = pkg.PkgPath + "." + model.Name
			owner := pkg.PkgPath + "." + model.Name
			for _, field := range model.Fields {
				for _, indexName := range []string{field.Index, field.Unique} {
					if indexName == "" {
						continue
					}
					qualifiedIndex := model.Schema + "." + indexName
					if previous, exists := indexes[qualifiedIndex]; exists && previous != owner {
						return nil, fmt.Errorf("index %q is declared by both %s and %s", qualifiedIndex, previous, owner)
					}
					indexes[qualifiedIndex] = owner
				}
			}
		}
		if len(spec.Models) > 0 || len(spec.CustomQueries) > 0 {
			sort.Slice(spec.Models, func(i, j int) bool { return spec.Models[i].Name < spec.Models[j].Name })
			sort.Slice(spec.CustomQueries, func(i, j int) bool { return spec.CustomQueries[i].Name < spec.CustomQueries[j].Name })
			result = append(result, spec)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result, nil
}

func scanPackage(pkg *packages.Package, target *PackageSpec) error {
	for _, file := range pkg.Syntax {
		filename := ""
		if pkg.Fset != nil {
			filename = pkg.Fset.Position(file.Pos()).Filename
		}
		if filepath.Base(filename) == generatedFileName {
			continue
		}
		for _, declaration := range file.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok || generic.Tok != token.TYPE {
				continue
			}
			for _, item := range generic.Specs {
				typeSpec := item.(*ast.TypeSpec)
				object, _ := pkg.TypesInfo.Defs[typeSpec.Name].(*types.TypeName)
				if object == nil {
					continue
				}
				named, _ := object.Type().(*types.Named)
				if named == nil {
					continue
				}
				if structure, ok := named.Underlying().(*types.Struct); ok {
					model, found, err := scanModel(target, typeSpec.Name.Name, structure)
					if err != nil {
						return err
					}
					if found {
						target.Models = append(target.Models, model)
					}
				}
				if iface, ok := named.Underlying().(*types.Interface); ok {
					directive := queryDirective(generic.Doc, typeSpec.Doc)
					if directive != "" {
						query, err := scanCustomQuery(target, typeSpec.Name.Name, iface, directive)
						if err != nil {
							return err
						}
						target.CustomQueries = append(target.CustomQueries, query)
					}
				}
			}
		}
	}
	return nil
}

func scanModel(pkg *PackageSpec, name string, structure *types.Struct) (*ModelSpec, bool, error) {
	var markerTag string
	found := false
	for index := 0; index < structure.NumFields(); index++ {
		field := structure.Field(index)
		if !field.Embedded() {
			continue
		}
		if isModelType(field.Type()) {
			found = true
			markerTag = structure.Tag(index)
			break
		}
		if pointer, ok := field.Type().(*types.Pointer); ok && isModelType(pointer.Elem()) {
			return nil, false, fmt.Errorf("model %s must embed sqlxgen.Model as a non-pointer", name)
		}
	}
	if !found {
		return nil, false, nil
	}
	options, err := tagOptions(markerTag)
	if err != nil {
		return nil, false, fmt.Errorf("model %s marker: %w", name, err)
	}
	table := options["table"]
	if table == "" {
		return nil, false, fmt.Errorf("model %s marker requires table option", name)
	}
	if err := validateIdentifier("table", table); err != nil {
		return nil, false, fmt.Errorf("model %s: %w", name, err)
	}
	schema := options["schema"]
	if schema != "" {
		if err := validateIdentifier("schema", schema); err != nil {
			return nil, false, fmt.Errorf("model %s: %w", name, err)
		}
	}
	for option := range options {
		if option != "table" && option != "schema" {
			return nil, false, fmt.Errorf("model %s marker has unsupported option %q", name, option)
		}
	}
	model := &ModelSpec{Package: pkg, Name: name, Schema: schema, Table: table}
	seen := make(map[string]string)
	if err := flattenFields(model, structure, nil, seen); err != nil {
		return nil, false, fmt.Errorf("model %s: %w", name, err)
	}
	if len(model.Keys) == 0 {
		return nil, false, fmt.Errorf("model %s requires at least one primary key", name)
	}
	if model.Auto != nil && len(model.Keys) != 1 {
		return nil, false, fmt.Errorf("model %s auto column requires a single-column primary key", name)
	}
	fieldNames := make(map[string]string)
	indexKinds := make(map[string]bool)
	for _, field := range model.Fields {
		if previous, exists := fieldNames[field.GoName]; exists {
			return nil, false, fmt.Errorf("model %s exposes duplicate generated field name %s through %s and %s", name, field.GoName, previous, field.Access)
		}
		fieldNames[field.GoName] = field.Access
		for _, index := range []struct {
			name   string
			unique bool
		}{{field.Index, false}, {field.Unique, true}} {
			if index.name == "" {
				continue
			}
			if previous, exists := indexKinds[index.name]; exists && previous != index.unique {
				return nil, false, fmt.Errorf("model %s index %s is declared as both unique and non-unique", name, index.name)
			}
			indexKinds[index.name] = index.unique
		}
	}
	return model, true, nil
}

func flattenFields(model *ModelSpec, structure *types.Struct, path []string, seen map[string]string) error {
	for index := 0; index < structure.NumFields(); index++ {
		field := structure.Field(index)
		if isModelType(field.Type()) {
			continue
		}
		tag := reflect.StructTag(structure.Tag(index))
		dbTag := tag.Get("db")
		if field.Embedded() && dbTag == "" {
			nestedType := field.Type()
			if pointer, ok := nestedType.(*types.Pointer); ok {
				nestedType = pointer.Elem()
			}
			if named, ok := nestedType.(*types.Named); ok {
				nestedType = named.Underlying()
			}
			if nested, ok := nestedType.(*types.Struct); ok {
				if err := flattenFields(model, nested, append(path, field.Name()), seen); err != nil {
					return err
				}
				continue
			}
		}
		if !field.Exported() {
			if dbTag != "" && dbTag != "-" {
				return fmt.Errorf("unexported field %s cannot be a database column", field.Name())
			}
			continue
		}
		if dbTag == "-" {
			continue
		}
		column := strings.Split(dbTag, ",")[0]
		if column == "" {
			return fmt.Errorf("field %s requires a db tag", field.Name())
		}
		if err := validateIdentifier("column", column); err != nil {
			return fmt.Errorf("field %s: %w", field.Name(), err)
		}
		if previous, exists := seen[column]; exists {
			return fmt.Errorf("column %q is used by both %s and %s", column, previous, field.Name())
		}
		seen[column] = field.Name()
		fieldSpec, err := buildField(field, tag, column, append(path, field.Name()))
		if err != nil {
			return err
		}
		fieldSpec.Owner = model.Package.Path + "." + model.Name
		model.Fields = append(model.Fields, fieldSpec)
		if fieldSpec.PrimaryKey {
			model.Keys = append(model.Keys, fieldSpec)
		}
		if fieldSpec.Auto {
			if model.Auto != nil {
				return fmt.Errorf("fields %s and %s are both auto columns", model.Auto.GoName, field.Name())
			}
			if fieldSpec.Kind != FieldInteger || fieldSpec.Nullable {
				return fmt.Errorf("auto field %s must be a non-null integer", field.Name())
			}
			kind := basicKind(fieldSpec.ValueType)
			if kind == types.Uint || kind == types.Uint64 || kind == types.Uintptr {
				return fmt.Errorf("auto field %s is too wide for portable PostgreSQL identity values", field.Name())
			}
			model.Auto = fieldSpec
		}
	}
	return nil
}

func buildField(field *types.Var, tag reflect.StructTag, column string, path []string) (*FieldSpec, error) {
	options, err := parseOptions(tag.Get("sqlxgen"))
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", field.Name(), err)
	}
	allowed := map[string]bool{
		"pk": true, "auto": true, "null": true, "notnull": true, "size": true,
		"precision": true, "scale": true, "default": true, "index": true, "unique": true,
		"references": true, "on_delete": true, "mysql_type": true, "postgres_type": true,
	}
	for option := range options {
		if !allowed[option] {
			return nil, fmt.Errorf("field %s has unsupported option %q", field.Name(), option)
		}
	}
	if options["null"] != "" && options["notnull"] != "" {
		return nil, fmt.Errorf("field %s cannot use both null and notnull", field.Name())
	}
	valueType, nullable := nullableValueType(field.Type())
	if options["null"] != "" {
		nullable = true
	}
	if options["notnull"] != "" {
		nullable = false
	}
	size, err := parsePositiveInt(options, "size")
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", field.Name(), err)
	}
	precision, err := parsePositiveInt(options, "precision")
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", field.Name(), err)
	}
	scale := 0
	if raw := options["scale"]; raw != "" {
		scale, err = strconv.Atoi(raw)
		if err != nil || scale < 0 || precision == 0 || scale > precision {
			return nil, fmt.Errorf("field %s: scale must be between zero and precision", field.Name())
		}
	}
	if ref := options["references"]; ref != "" {
		if err := validateQualifiedReference(ref); err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name(), err)
		}
	}
	onDelete := strings.ToUpper(options["on_delete"])
	if onDelete != "" && onDelete != "CASCADE" && onDelete != "RESTRICT" && onDelete != "SET NULL" && onDelete != "NO ACTION" {
		return nil, fmt.Errorf("field %s has unsupported on_delete action %q", field.Name(), onDelete)
	}
	if onDelete == "SET NULL" && !nullable {
		return nil, fmt.Errorf("field %s uses SET NULL but is not nullable", field.Name())
	}
	primary := options["pk"] != ""
	auto := options["auto"] != ""
	if auto && !primary {
		return nil, fmt.Errorf("field %s auto option requires pk", field.Name())
	}
	if primary && nullable {
		return nil, fmt.Errorf("field %s primary key must not be nullable", field.Name())
	}
	if auto && options["default"] != "" {
		return nil, fmt.Errorf("field %s cannot use both auto and default", field.Name())
	}
	if primary && options["default"] != "" {
		return nil, fmt.Errorf("field %s primary key defaults are not supported; generate the key in Go", field.Name())
	}
	if options["on_delete"] != "" && options["references"] == "" {
		return nil, fmt.Errorf("field %s on_delete requires references", field.Name())
	}
	for _, flag := range []string{"pk", "auto", "null", "notnull"} {
		if value := options[flag]; value != "" && value != "true" {
			return nil, fmt.Errorf("field %s option %s does not accept a value", field.Name(), flag)
		}
	}
	for kind, identifier := range map[string]string{"index": options["index"], "unique index": options["unique"]} {
		if identifier != "" {
			if err := validateIdentifier(kind, identifier); err != nil {
				return nil, fmt.Errorf("field %s: %w", field.Name(), err)
			}
		}
	}
	return &FieldSpec{
		GoName: field.Name(), Access: strings.Join(path, "."), Column: column,
		Type: field.Type(), ValueType: valueType, Kind: classifyType(valueType), Nullable: nullable,
		PrimaryKey: primary, Auto: auto, Size: size, Precision: precision, Scale: scale,
		Default: options["default"], Index: options["index"], Unique: options["unique"],
		References: options["references"], OnDelete: onDelete,
		MySQLType: options["mysql_type"], PostgresType: options["postgres_type"],
	}, nil
}

func isModelType(typ types.Type) bool {
	named, ok := typ.(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == runtimePackagePath && named.Obj().Name() == "Model"
}

func nullableValueType(typ types.Type) (types.Type, bool) {
	if pointer, ok := typ.(*types.Pointer); ok {
		return pointer.Elem(), true
	}
	if named, ok := typ.(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "database/sql" {
		if named.Obj().Name() == "Null" && named.TypeArgs().Len() == 1 {
			return named.TypeArgs().At(0), true
		}
		switch named.Obj().Name() {
		case "NullString":
			return types.Typ[types.String], true
		case "NullBool":
			return types.Typ[types.Bool], true
		case "NullByte":
			return types.Typ[types.Uint8], true
		case "NullInt16":
			return types.Typ[types.Int16], true
		case "NullInt32":
			return types.Typ[types.Int32], true
		case "NullInt64":
			return types.Typ[types.Int64], true
		case "NullFloat64":
			return types.Typ[types.Float64], true
		case "NullTime":
			return named.Underlying().(*types.Struct).Field(0).Type(), true
		}
	}
	return typ, false
}

func classifyType(typ types.Type) FieldKind {
	if named, ok := typ.(*types.Named); ok {
		if named.Obj().Pkg() != nil {
			switch named.Obj().Pkg().Path() + "." + named.Obj().Name() {
			case "time.Time":
				return FieldTime
			case "encoding/json.RawMessage":
				return FieldJSON
			}
		}
		typ = named.Underlying()
	}
	switch value := typ.(type) {
	case *types.Basic:
		if value.Info()&types.IsString != 0 {
			return FieldString
		}
		if value.Info()&types.IsBoolean != 0 {
			return FieldBool
		}
		if value.Info()&types.IsInteger != 0 {
			return FieldInteger
		}
		if value.Info()&types.IsFloat != 0 {
			return FieldFloat
		}
	case *types.Slice:
		if basic, ok := value.Elem().(*types.Basic); ok && basic.Kind() == types.Byte {
			return FieldBytes
		}
	}
	return FieldOther
}

func queryDirective(groups ...*ast.CommentGroup) string {
	for _, group := range groups {
		if group == nil {
			continue
		}
		for _, comment := range group.List {
			text := strings.TrimSpace(strings.TrimPrefix(comment.Text, "//"))
			if strings.HasPrefix(text, "sqlxgen:queries ") {
				return strings.TrimSpace(strings.TrimPrefix(text, "sqlxgen:queries "))
			}
		}
	}
	return ""
}

func scanCustomQuery(pkg *PackageSpec, name string, iface *types.Interface, directive string) (*CustomQuerySpec, error) {
	const prefix = "file="
	if !strings.HasPrefix(directive, prefix) || strings.TrimSpace(strings.TrimPrefix(directive, prefix)) == "" {
		return nil, fmt.Errorf("custom query interface %s requires file=... directive", name)
	}
	spec := &CustomQuerySpec{Package: pkg, Name: name, SQLFile: strings.TrimSpace(strings.TrimPrefix(directive, prefix)), InterfaceType: iface}
	blocks, err := parseSQLFile(spec.resolvedSQLFile())
	if err != nil {
		return nil, fmt.Errorf("custom query interface %s: %w", name, err)
	}
	iface.Complete()
	for index := 0; index < iface.NumMethods(); index++ {
		method := iface.Method(index)
		signature, _ := method.Type().(*types.Signature)
		block, ok := blocks[method.Name()]
		if !ok {
			return nil, fmt.Errorf("custom query method %s has no matching SQL block", method.Name())
		}
		methodSpec, err := validateCustomMethod(method.Name(), signature, block)
		if err != nil {
			return nil, fmt.Errorf("custom query method %s: %w", method.Name(), err)
		}
		spec.Methods = append(spec.Methods, methodSpec)
		delete(blocks, method.Name())
	}
	if len(blocks) > 0 {
		var names []string
		for name := range blocks {
			names = append(names, name)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("SQL blocks have no matching interface methods: %s", strings.Join(names, ", "))
	}
	return spec, nil
}

func validateCustomMethod(name string, signature *types.Signature, query string) (*CustomMethodSpec, error) {
	if signature == nil || signature.Params().Len() < 1 || signature.Params().Len() > 2 {
		return nil, fmt.Errorf("signature must contain context.Context and at most one parameter struct")
	}
	if types.TypeString(signature.Params().At(0).Type(), packagePathQualifier) != "context.Context" {
		return nil, fmt.Errorf("first parameter must be context.Context")
	}
	var paramType types.Type
	parameterNames := map[string]struct{}{}
	if signature.Params().Len() == 2 {
		paramType = signature.Params().At(1).Type()
		structure := underlyingStruct(paramType)
		if structure == nil {
			return nil, fmt.Errorf("query parameters must be a struct")
		}
		for index := 0; index < structure.NumFields(); index++ {
			field := structure.Field(index)
			if !field.Exported() {
				continue
			}
			column := reflect.StructTag(structure.Tag(index)).Get("db")
			if column == "" || column == "-" {
				return nil, fmt.Errorf("parameter field %s requires a db tag", field.Name())
			}
			parameterNames[strings.Split(column, ",")[0]] = struct{}{}
		}
	}
	placeholders, err := namedParameters(query)
	if err != nil {
		return nil, err
	}
	if !sameStringSet(placeholders, parameterNames) {
		return nil, fmt.Errorf("SQL named parameters %v do not match parameter struct fields %v", sortedSet(placeholders), sortedSet(parameterNames))
	}
	results := signature.Results()
	method := &CustomMethodSpec{Name: name, SQL: query, Signature: signature, ParamType: paramType}
	if results.Len() == 1 && isErrorType(results.At(0).Type()) {
		method.Mode = CustomExec
		return method, nil
	}
	if results.Len() != 2 || !isErrorType(results.At(1).Type()) {
		return nil, fmt.Errorf("result must be error or (value, error)")
	}
	first := results.At(0).Type()
	if !looksLikeQuery(query) && isInt64Type(first) {
		method.Mode = CustomRowsAffected
		return method, nil
	}
	if slice, ok := first.(*types.Slice); ok {
		method.Mode = CustomMany
		method.ResultType = slice.Elem()
		return method, nil
	}
	method.Mode = CustomOne
	if pointer, ok := first.(*types.Pointer); ok {
		method.Pointer = true
		first = pointer.Elem()
	}
	method.ResultType = first
	return method, nil
}

func underlyingStruct(typ types.Type) *types.Struct {
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = pointer.Elem()
	}
	if named, ok := typ.(*types.Named); ok {
		typ = named.Underlying()
	}
	structure, _ := typ.(*types.Struct)
	return structure
}

func isErrorType(typ types.Type) bool {
	return types.Identical(typ, types.Universe.Lookup("error").Type())
}

func isInt64Type(typ types.Type) bool {
	basic, ok := typ.(*types.Basic)
	return ok && basic.Kind() == types.Int64
}

func packagePathQualifier(pkg *types.Package) string { return pkg.Name() }

func looksLikeQuery(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "WITH") || strings.Contains(upper, " RETURNING ")
}

func sameStringSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, ok := right[value]; !ok {
			return false
		}
	}
	return true
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
