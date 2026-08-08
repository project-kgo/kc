package main

import (
	"strings"
	"testing"
)

func TestRunValidatesCommandAndPatterns(t *testing.T) {
	if err := run(nil); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("missing command error = %v", err)
	}
	if err := run([]string{"generate", "-patterns="}); err == nil || !strings.Contains(err.Error(), "patterns") {
		t.Fatalf("empty patterns error = %v", err)
	}
}

func TestParseGenerateConfigDoesNotGenerateDDLByDefault(t *testing.T) {
	config, err := parseGenerateConfig([]string{"-patterns=./model"})
	if err != nil {
		t.Fatal(err)
	}
	if config.DDLOut != "" {
		t.Fatalf("default DDL output = %q, want empty", config.DDLOut)
	}

	config, err = parseGenerateConfig([]string{"-patterns=./model", "-ddl-out=./schema/generated"})
	if err != nil {
		t.Fatal(err)
	}
	if config.DDLOut != "./schema/generated" {
		t.Fatalf("explicit DDL output = %q", config.DDLOut)
	}
}
