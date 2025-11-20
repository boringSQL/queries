package queries

import (
	"bufio"
	"strings"
	"testing"
)

func TestScannerSkipsCommentsBeforeNameDirective(t *testing.T) {
	testCases := []struct {
		name              string
		fileName          string
		content           string
		expectedQueryName string
		expectedQuery     string
	}{
		{
			name:     "Comments before name",
			fileName: "test.sql",
			content: `-- comment 1
-- comment 2
-- name: my-query
SELECT 42;`,
			expectedQueryName: "my-query",
			expectedQuery:     "SELECT 42;",
		},
		{
			name:     "Multiple comments",
			fileName: "test.sql",
			content: `-- comment 1
-- comment 2
-- comment 3
-- name: test-query
SELECT * FROM users;`,
			expectedQueryName: "test-query",
			expectedQuery:     "SELECT * FROM users;",
		},
		{
			name:     "Empty lines before name",
			fileName: "test.sql",
			content: `

-- name: test-query
SELECT 1;`,
			expectedQueryName: "test-query",
			expectedQuery:     "SELECT 1;",
		},
		{
			name:     "Mixed empty lines and comments",
			fileName: "test.sql",
			content: `-- comment 1

-- comment 2

-- name: test-query
SELECT 1;`,
			expectedQueryName: "test-query",
			expectedQuery:     "SELECT 1;",
		},
		{
			name:     "No name directive",
			fileName: "my_query.sql",
			content: `-- comment
SELECT * FROM users;`,
			expectedQueryName: "my_query",
			expectedQuery:     "SELECT * FROM users;",
		},
		{
			name:     "Metadata before name",
			fileName: "test.sql",
			content: `-- description: test
-- name: test-query
SELECT 1;`,
			expectedQueryName: "test-query",
			expectedQuery:     "SELECT 1;",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &Scanner{}
			reader := strings.NewReader(tc.content)
			bufScanner := bufio.NewScanner(reader)

			queries := scanner.Run(tc.fileName, bufScanner)

			if len(queries) != 1 {
				t.Errorf("Expected 1 query, got %d. Queries: %v", len(queries), queries)
				return
			}

			scannedQuery, ok := queries[tc.expectedQueryName]
			if !ok {
				t.Errorf("Query '%s' not found. Available queries: %v", tc.expectedQueryName, queries)
				return
			}

			if scannedQuery.Query != tc.expectedQuery {
				t.Errorf("Query content mismatch:\ngot:  %q\nwant: %q",
					scannedQuery.Query, tc.expectedQuery)
			}
		})
	}
}

func TestScannerDoesNotCreateEmptyQueries(t *testing.T) {
	t.Run("Only comments", func(t *testing.T) {
		content := `-- comment 1
-- comment 2
-- comment 3`

		scanner := &Scanner{}
		reader := strings.NewReader(content)
		bufScanner := bufio.NewScanner(reader)

		queries := scanner.Run("test.sql", bufScanner)

		if len(queries) != 0 {
			t.Errorf("Expected 0 queries, got %d", len(queries))
		}
	})

	t.Run("Only name directive", func(t *testing.T) {
		content := `-- comment
-- name: test-query`

		scanner := &Scanner{}
		reader := strings.NewReader(content)
		bufScanner := bufio.NewScanner(reader)

		queries := scanner.Run("test.sql", bufScanner)

		if len(queries) != 0 {
			t.Errorf("Expected 0 queries, got %d", len(queries))
		}
	})

	t.Run("Comments after name directive included", func(t *testing.T) {
		content := `-- comment before
-- name: test-query
-- comment after
SELECT 1;`

		scanner := &Scanner{}
		reader := strings.NewReader(content)
		bufScanner := bufio.NewScanner(reader)

		queries := scanner.Run("test.sql", bufScanner)

		if len(queries) != 1 {
			t.Errorf("Expected 1 query, got %d", len(queries))
			return
		}

		if q, ok := queries["test-query"]; ok {
			expected := "-- comment after\nSELECT 1;"
			if q.Query != expected {
				t.Errorf("Expected %q, got %q", expected, q.Query)
			}
		}
	})
}
