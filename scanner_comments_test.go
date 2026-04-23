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

func TestScannerMetadataWithoutNameDirective(t *testing.T) {
	testCases := []struct {
		name             string
		fileName         string
		content          string
		expectedName     string
		expectedQuery    string
		expectedMetadata map[string]string
	}{
		{
			name:     "Metadata before SQL without name directive",
			fileName: "users.sql",
			content: `-- description: Retrieve user by email efficiently
-- max-cost: 100
-- timeout: 50ms
SELECT id, name, email FROM users WHERE email = :email`,
			expectedName:  "users",
			expectedQuery: "SELECT id, name, email FROM users WHERE email = :email",
			expectedMetadata: map[string]string{
				"description": "Retrieve user by email efficiently",
				"max-cost":    "100",
				"timeout":     "50ms",
			},
		},
		{
			name:     "Mixed comments and metadata without name directive",
			fileName: "query.sql",
			content: `-- This is a regular comment
-- description: Test query
-- Another comment
-- author: test-team
SELECT 1;`,
			expectedName:  "query",
			expectedQuery: "SELECT 1;",
			expectedMetadata: map[string]string{
				"description": "Test query",
				"author":      "test-team",
			},
		},
		{
			name:     "Only metadata, no regular comments",
			fileName: "simple.sql",
			content: `-- version: 1.0
SELECT * FROM items;`,
			expectedName:  "simple",
			expectedQuery: "SELECT * FROM items;",
			expectedMetadata: map[string]string{
				"version": "1.0",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &Scanner{}
			reader := strings.NewReader(tc.content)
			bufScanner := bufio.NewScanner(reader)

			queries := scanner.Run(tc.fileName, bufScanner)

			if len(queries) != 1 {
				t.Errorf("Expected 1 query, got %d", len(queries))
				return
			}

			scannedQuery, ok := queries[tc.expectedName]
			if !ok {
				t.Errorf("Query '%s' not found. Available: %v", tc.expectedName, queries)
				return
			}

			if scannedQuery.Query != tc.expectedQuery {
				t.Errorf("Query mismatch:\ngot:  %q\nwant: %q", scannedQuery.Query, tc.expectedQuery)
			}

			for key, expectedValue := range tc.expectedMetadata {
				if gotValue, ok := scannedQuery.Metadata[key]; !ok {
					t.Errorf("Missing metadata key %q", key)
				} else if gotValue != expectedValue {
					t.Errorf("Metadata %q mismatch: got %q, want %q", key, gotValue, expectedValue)
				}
			}

			if len(scannedQuery.Metadata) != len(tc.expectedMetadata) {
				t.Errorf("Metadata count mismatch: got %d, want %d", len(scannedQuery.Metadata), len(tc.expectedMetadata))
			}
		})
	}
}

func TestScannerBlockCommentDirectives(t *testing.T) {
	testCases := []struct {
		name             string
		fileName         string
		content          string
		expectedName     string
		expectedQuery    string
		expectedMetadata map[string]string
	}{
		{
			name:     "Block-comment name directive",
			fileName: "test.sql",
			content: `/* name: get-user */
SELECT * FROM users WHERE id = :id`,
			expectedName:  "get-user",
			expectedQuery: "SELECT * FROM users WHERE id = :id",
		},
		{
			name:     "Block-comment metadata",
			fileName: "test.sql",
			content: `/* name: get-user */
/* description: look up a user */
/* timeout: 50ms */
SELECT 1`,
			expectedName:  "get-user",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"description": "look up a user",
				"timeout":     "50ms",
			},
		},
		{
			name:     "Mixed dash and block directives",
			fileName: "test.sql",
			content: `-- name: get-user
/* description: mixed style */
-- author: team
SELECT 1`,
			expectedName:  "get-user",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"description": "mixed style",
				"author":      "team",
			},
		},
		{
			name:     "Non-directive block comment is skipped",
			fileName: "test.sql",
			content: `/* just a note */
-- name: q
SELECT 1`,
			expectedName:  "q",
			expectedQuery: "SELECT 1",
		},
		{
			name:     "Block-comment name without dash form",
			fileName: "fallback.sql",
			content: `/* name: overridden */
SELECT 2`,
			expectedName:  "overridden",
			expectedQuery: "SELECT 2",
		},
		{
			name:     "Marginalia-style multi-pair directive",
			fileName: "test.sql",
			content: `/* name: get-user, author: team, version: 2 */
SELECT * FROM users WHERE id = :id`,
			expectedName:  "get-user",
			expectedQuery: "SELECT * FROM users WHERE id = :id",
			expectedMetadata: map[string]string{
				"author":  "team",
				"version": "2",
			},
		},
		{
			name:     "Marginalia metadata-only (no name)",
			fileName: "users.sql",
			content: `/* description: lookup, timeout: 50ms */
SELECT 1`,
			expectedName:  "users",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"description": "lookup",
				"timeout":     "50ms",
			},
		},
		{
			name:     "Marginalia pairs after dash name",
			fileName: "test.sql",
			content: `-- name: get-user
/* author: team, version: 2 */
SELECT 1`,
			expectedName:  "get-user",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"author":  "team",
				"version": "2",
			},
		},
		{
			name:     "Value containing colon is preserved",
			fileName: "test.sql",
			content: `/* name: q, timestamp: 12:30:00 */
SELECT 1`,
			expectedName:  "q",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"timestamp": "12:30:00",
			},
		},
		{
			name:     "Malformed pair falls back to skipped comment",
			fileName: "fallback.sql",
			content: `/* name: foo, bogus */
SELECT 1`,
			expectedName:  "fallback",
			expectedQuery: "SELECT 1",
		},
		{
			name:     "sqlcommenter directive",
			fileName: "test.sql",
			content: `/* name='get-user',application='billing',controller='users' */
SELECT 1`,
			expectedName:  "get-user",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"application": "billing",
				"controller":  "users",
			},
		},
		{
			name:     "sqlcommenter URL-encoded value",
			fileName: "test.sql",
			content: `/* name='q',route='GET%20%2Fusers%2F%3Aid' */
SELECT 1`,
			expectedName:  "q",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"route": "GET /users/:id",
			},
		},
		{
			name:     "sqlcommenter doubled quote un-escaped",
			fileName: "test.sql",
			content: `/* name='q',note='it%27s%20fine' */
SELECT 1`,
			expectedName:  "q",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"note": "it's fine",
			},
		},
		{
			name:     "Mixed sqlcommenter and marginalia pairs",
			fileName: "test.sql",
			content: `/* name: q, application='billing', author: team */
SELECT 1`,
			expectedName:  "q",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"application": "billing",
				"author":      "team",
			},
		},
		{
			name:     "Trailing dash name renames single-query file",
			fileName: "fallback.sql",
			content: `SELECT * FROM users WHERE id = :id
-- name: get-user`,
			expectedName:  "get-user",
			expectedQuery: "SELECT * FROM users WHERE id = :id",
		},
		{
			name:     "Trailing block name with metadata",
			fileName: "fallback.sql",
			content: `SELECT 1
/* name: q, author: team */`,
			expectedName:  "q",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"author": "team",
			},
		},
		{
			name:     "Trailing metadata after dash name",
			fileName: "test.sql",
			content: `-- name: q
SELECT 1
-- author: team`,
			expectedName:  "q",
			expectedQuery: "SELECT 1",
			expectedMetadata: map[string]string{
				"author": "team",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &Scanner{}
			reader := strings.NewReader(tc.content)
			bufScanner := bufio.NewScanner(reader)

			queries := scanner.Run(tc.fileName, bufScanner)

			sq, ok := queries[tc.expectedName]
			if !ok {
				t.Fatalf("Query %q not found. Available: %v", tc.expectedName, queries)
			}
			if sq.Query != tc.expectedQuery {
				t.Errorf("Query mismatch:\ngot:  %q\nwant: %q", sq.Query, tc.expectedQuery)
			}
			for k, v := range tc.expectedMetadata {
				if got, ok := sq.Metadata[k]; !ok || got != v {
					t.Errorf("Metadata %q = %q (ok=%v), want %q", k, got, ok, v)
				}
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
