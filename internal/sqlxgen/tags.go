package sqlxgen

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func parseOptions(raw string) (map[string]string, error) {
	options := make(map[string]string)
	for _, item := range strings.Split(raw, ";") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key, value, found := strings.Cut(item, "=")
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("empty sqlxgen option in %q", raw)
		}
		if _, exists := options[key]; exists {
			return nil, fmt.Errorf("duplicate sqlxgen option %q", key)
		}
		if !found {
			value = "true"
		}
		options[key] = strings.TrimSpace(value)
	}
	return options, nil
}

func tagOptions(tag string) (map[string]string, error) {
	return parseOptions(reflect.StructTag(tag).Get("sqlxgen"))
}

func parsePositiveInt(options map[string]string, key string) (int, error) {
	raw := options[key]
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("sqlxgen option %s must be a positive integer", key)
	}
	return value, nil
}

func validateIdentifier(kind, value string) error {
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a safe SQL identifier", kind, value)
	}
	return nil
}

func validateQualifiedReference(value string) error {
	table, columnPart, ok := strings.Cut(value, "(")
	if !ok || !strings.HasSuffix(columnPart, ")") {
		return fmt.Errorf("reference %q must have table(column) form", value)
	}
	for _, part := range strings.Split(table, ".") {
		if err := validateIdentifier("reference table", part); err != nil {
			return err
		}
	}
	column := strings.TrimSuffix(columnPart, ")")
	return validateIdentifier("reference column", column)
}
