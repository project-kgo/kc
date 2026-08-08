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
