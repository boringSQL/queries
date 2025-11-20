package queries

import (
	"bufio"
	"strings"
	"testing"
)

func TestScannerCommentsBeforeName(t *testing.T) {
	t.Run("Comments before name directive", func(t *testing.T) {
		content := `-- comment line 1
-- comment line 2
-- name: test-query
SELECT 42;`

		scanner := &Scanner{}
		reader := strings.NewReader(content)
		bufScanner := bufio.NewScanner(reader)

		queries := scanner.Run("test.sql", bufScanner)

		if len(queries) != 1 {
			t.Errorf("Expected 1 query, got %d", len(queries))
			return
		}

		q, ok := queries["test-query"]
		if !ok {
			t.Errorf("Query 'test-query' not found")
			return
		}

		if q.Query != "SELECT 42;" {
			t.Errorf("Expected 'SELECT 42;', got %q", q.Query)
		}
	})

	t.Run("Multiple queries with comments", func(t *testing.T) {
		content := `-- comment 1
-- name: get-user
SELECT * FROM users WHERE id = :id;

-- comment 2
-- name: list-users
SELECT * FROM users;`

		scanner := &Scanner{}
		reader := strings.NewReader(content)
		bufScanner := bufio.NewScanner(reader)

		queries := scanner.Run("test.sql", bufScanner)

		if len(queries) != 2 {
			t.Errorf("Expected 2 queries, got %d", len(queries))
			return
		}

		if _, ok := queries["get-user"]; !ok {
			t.Errorf("Query 'get-user' not found")
		}

		if _, ok := queries["list-users"]; !ok {
			t.Errorf("Query 'list-users' not found")
		}
	})
}
