package sqlxgen

import (
	"errors"
	"testing"
)

func TestBuildWhereAndEmptyIn(t *testing.T) {
	id := NewNumericColumn[int64]("id")
	name := NewStringColumn[string]("name")
	allowed := map[string]struct{}{"id": {}, "name": {}}
	query, args, err := BuildWhere(DialectPostgreSQL, []Expr{
		id.In(),
		Or(name.Like("a%"), id.Gt(10)),
	}, allowed)
	if err != nil {
		t.Fatal(err)
	}
	if query != ` WHERE (1 = 0) AND (("name" LIKE ?) OR ("id" > ?))` {
		t.Fatalf("query = %q", query)
	}
	if len(args) != 2 || args[0] != "a%" || args[1] != int64(10) {
		t.Fatalf("args = %#v", args)
	}
}

func TestBuildAssignmentsRejectsForeignAndDuplicateColumns(t *testing.T) {
	id := NewNumericColumn[int64]("id")
	foreign := NewColumn[string]("foreign")
	allowed := map[string]struct{}{"id": {}}
	if _, _, err := BuildAssignments(DialectMySQL, []Assignment{foreign.Set("x")}, allowed); err == nil {
		t.Fatal("expected foreign column error")
	}
	if _, _, err := BuildAssignments(DialectMySQL, []Assignment{id.Set(1), id.Add(2)}, allowed); err == nil {
		t.Fatal("expected duplicate assignment error")
	}
}

func TestGeneratedColumnOwnerRejectsSameNamedForeignColumn(t *testing.T) {
	userID := NewNumericColumn[int64]("id", "users")
	orderID := NewNumericColumn[int64]("id", "orders")
	allowed := map[string]struct{}{ColumnKey("users", "id"): {}}
	if _, _, err := BuildWhere(DialectPostgreSQL, []Expr{orderID.Eq(1)}, allowed); err == nil {
		t.Fatal("expected same-named foreign column to be rejected")
	}
	if _, _, err := BuildWhere(DialectPostgreSQL, []Expr{userID.Eq(1)}, allowed); err != nil {
		t.Fatalf("owned column error = %v", err)
	}
}

func TestBuildPagination(t *testing.T) {
	offset := 10
	query, args, err := BuildPagination(DialectMySQL, nil, &offset)
	if err != nil {
		t.Fatal(err)
	}
	if query != " LIMIT 18446744073709551615 OFFSET ?" || len(args) != 1 || args[0] != 10 {
		t.Fatalf("pagination = %q %#v", query, args)
	}
	negative := -1
	if _, _, err := BuildPagination(DialectPostgreSQL, &negative, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative limit error = %v", err)
	}
}

func TestUnsafeMutationErrorIsStable(t *testing.T) {
	if !errors.Is(ErrUnsafeMutation, ErrUnsafeMutation) {
		t.Fatal("ErrUnsafeMutation must support errors.Is")
	}
}

func TestIsUnrestrictedDetectsStaticTruePredicates(t *testing.T) {
	id := NewNumericColumn[int64]("id")
	if !IsUnrestricted(nil) || !IsUnrestricted([]Expr{And()}) || !IsUnrestricted([]Expr{id.NotIn()}) {
		t.Fatal("static true predicates must remain protected")
	}
	if IsUnrestricted([]Expr{id.In()}) || IsUnrestricted([]Expr{id.Eq(1)}) || IsUnrestricted([]Expr{Or(id.In(), id.Eq(1))}) {
		t.Fatal("false or conditional predicates are safe mutations")
	}
}
