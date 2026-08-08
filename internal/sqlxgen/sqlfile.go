package sqlxgen

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"unicode"
)

var sqlNamePattern = regexp.MustCompile(`^\s*--\s*name:\s*([A-Za-z_][A-Za-z0-9_]*)\s*$`)

func parseSQLFile(path string) (map[string]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read SQL file %s: %w", path, err)
	}
	blocks := make(map[string]string)
	var current string
	var lines []string
	flush := func() error {
		if current == "" {
			return nil
		}
		query := strings.TrimSpace(strings.Join(lines, "\n"))
		if query == "" {
			return fmt.Errorf("SQL block %s is empty", current)
		}
		if _, exists := blocks[current]; exists {
			return fmt.Errorf("duplicate SQL block %s", current)
		}
		blocks[current] = query
		return nil
	}
	for _, line := range strings.Split(string(content), "\n") {
		match := sqlNamePattern.FindStringSubmatch(line)
		if len(match) == 2 {
			if err := flush(); err != nil {
				return nil, err
			}
			current = match[1]
			lines = nil
			continue
		}
		if current == "" {
			if strings.TrimSpace(line) != "" && !strings.HasPrefix(strings.TrimSpace(line), "--") {
				return nil, fmt.Errorf("SQL content must follow a -- name: directive")
			}
			continue
		}
		lines = append(lines, line)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return blocks, nil
}

// namedParameters 提取 SQLx :name 参数，同时跳过字符串、注释和 PostgreSQL 类型转换。
func namedParameters(query string) (map[string]struct{}, error) {
	parameters := make(map[string]struct{})
	for index := 0; index < len(query); {
		switch query[index] {
		case '\'', '"', '`':
			quote := query[index]
			index++
			closed := false
			for index < len(query) {
				if query[index] == quote {
					if index+1 < len(query) && query[index+1] == quote {
						index += 2
						continue
					}
					index++
					closed = true
					break
				}
				index++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated SQL quote")
			}
		case '-':
			if index+1 < len(query) && query[index+1] == '-' {
				index += 2
				for index < len(query) && query[index] != '\n' {
					index++
				}
				continue
			}
			index++
		case '/':
			if index+1 < len(query) && query[index+1] == '*' {
				end := strings.Index(query[index+2:], "*/")
				if end < 0 {
					return nil, fmt.Errorf("unterminated SQL block comment")
				}
				index += end + 4
				continue
			}
			index++
		case ':':
			if index+1 < len(query) && query[index+1] == ':' {
				index += 2
				continue
			}
			if index+1 >= len(query) || !isIdentifierStart(rune(query[index+1])) {
				index++
				continue
			}
			end := index + 2
			for end < len(query) && isIdentifierPart(rune(query[end])) {
				end++
			}
			parameters[query[index+1:end]] = struct{}{}
			index = end
		default:
			index++
		}
	}
	return parameters, nil
}

func isIdentifierStart(value rune) bool { return value == '_' || unicode.IsLetter(value) }
func isIdentifierPart(value rune) bool  { return isIdentifierStart(value) || unicode.IsDigit(value) }
