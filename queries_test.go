package queries

import (
	"bufio"
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

func TestIsReservedName(t *testing.T) {
	testCases := []struct {
		name     string
		expected bool
	}{
		{name: "MI", expected: true},
		{name: "SS", expected: true},
		{name: "not_reserved", expected: false},
		{name: "another_not_reserved", expected: false},
		{name: "", expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isReservedName(tc.name)
			if result != tc.expected {
				t.Errorf("isReservedName(%s) = %v; expected %v", tc.name, result, tc.expected)
			}
		})
	}
}

func TestNewQuery(t *testing.T) {
	testCases := []struct {
		name        string
		inputQuery  string
		expectedRaw string
		expectedOrd string
		expectedSQL []sql.NamedArg
	}{
		{
			name:        "Test 1",
			inputQuery:  "SELECT * FROM users WHERE id = :id AND name = :name",
			expectedRaw: "SELECT * FROM users WHERE id = :id AND name = :name",
			expectedOrd: "-- name: Test 1\nSELECT * FROM users WHERE id = $1 AND name = $2",
			expectedSQL: []sql.NamedArg{
				sql.Named("id", nil),
				sql.Named("name", nil),
			},
		},
		{
			name:        "Test 2",
			inputQuery:  "INSERT INTO users (full_name, age) VALUES (:full_name, :age)",
			expectedRaw: "INSERT INTO users (full_name, age) VALUES (:full_name, :age)",
			expectedOrd: "-- name: Test 2\nINSERT INTO users (full_name, age) VALUES ($1, $2)",
			expectedSQL: []sql.NamedArg{
				sql.Named("full_name", nil),
				sql.Named("age", nil),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := NewQuery(tc.name, "test.sql", tc.inputQuery, nil)
			if q.Raw != tc.expectedRaw {
				t.Errorf("Raw: got %s, expected %s", q.Raw, tc.expectedRaw)
			}
			if q.OrdinalQuery != tc.expectedOrd {
				t.Errorf("OrdinalQuery: got %s, expected %s", q.OrdinalQuery, tc.expectedOrd)
			}
			if !reflect.DeepEqual(q.NamedArgs, tc.expectedSQL) {
				t.Errorf("Mapping: got %v, expected %v", q.NamedArgs, tc.expectedSQL)
			}
		})
	}
}

func TestScannerUsesFilenameAsQueryName(t *testing.T) {
	testCases := []struct {
		name             string
		fileName         string
		content          string
		expectedQueryName string
		expectedQuery    string
	}{
		{
			name:             "File without name directive uses filename",
			fileName:         "my_test_query.sql",
			content:          "SELECT * FROM users WHERE id = :user_id",
			expectedQueryName: "my_test_query",
			expectedQuery:    "SELECT * FROM users WHERE id = :user_id",
		},
		{
			name:             "File with name directive uses directive name",
			fileName:         "my_test_query.sql",
			content:          "-- name: other-name\nSELECT * FROM users WHERE id = :user_id",
			expectedQueryName: "other-name",
			expectedQuery:    "SELECT * FROM users WHERE id = :user_id",
		},
		{
			name:             "File with path uses basename",
			fileName:         "sql/queries/get_user.sql",
			content:          "SELECT * FROM users",
			expectedQueryName: "get_user",
			expectedQuery:    "SELECT * FROM users",
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
			}

			scannedQuery, ok := queries[tc.expectedQueryName]
			if !ok {
				t.Errorf("Query '%s' not found. Available queries: %v", tc.expectedQueryName, queries)
			}

			if scannedQuery.Query != tc.expectedQuery {
				t.Errorf("Query content: got %s, expected %s", scannedQuery.Query, tc.expectedQuery)
			}
		})
	}
}

func TestScannerParsesMetadata(t *testing.T) {
	testCases := []struct {
		name             string
		fileName         string
		content          string
		expectedMetadata map[string]string
	}{
		{
			name:     "Query with metadata",
			fileName: "get_user.sql",
			content: `-- name: get-user-by-email
-- description: Retrieve user by email efficiently
-- max-cost: 100
-- required-nodes: Index Scan
-- timeout: 50ms
SELECT id, name, email
FROM users
WHERE email = $1`,
			expectedMetadata: map[string]string{
				"description":    "Retrieve user by email efficiently",
				"max-cost":       "100",
				"required-nodes": "Index Scan",
				"timeout":        "50ms",
			},
		},
		{
			name:     "Query without metadata",
			fileName: "simple.sql",
			content: `-- name: simple-query
SELECT * FROM users`,
			expectedMetadata: map[string]string{},
		},
		{
			name:     "Query with mixed case metadata keys",
			fileName: "mixed.sql",
			content: `-- name: test-query
-- Description: This is a test
-- MAX-COST: 200
SELECT 1`,
			expectedMetadata: map[string]string{
				"description": "This is a test",
				"max-cost":    "200",
			},
		},
		{
			name:     "Query with metadata containing special chars",
			fileName: "special.sql",
			content: `-- name: special-query
-- author: John Doe <john@example.com>
-- tags: user, authentication, security
SELECT * FROM users`,
			expectedMetadata: map[string]string{
				"author": "John Doe <john@example.com>",
				"tags":   "user, authentication, security",
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

			var scannedQuery *ScannedQuery
			for _, q := range queries {
				scannedQuery = q
				break
			}

			if !reflect.DeepEqual(scannedQuery.Metadata, tc.expectedMetadata) {
				t.Errorf("Metadata mismatch:\ngot:  %v\nwant: %v", scannedQuery.Metadata, tc.expectedMetadata)
			}
		})
	}
}

func TestQueryMetadataAccess(t *testing.T) {
	metadata := map[string]string{
		"description": "Test query",
		"max-cost":    "100",
		"timeout":     "50ms",
	}

	q := NewQuery("test-query", "test.sql", "SELECT 1", metadata)

	// Test direct access to Metadata field
	if q.Metadata["description"] != "Test query" {
		t.Errorf("Direct metadata access failed: got %s", q.Metadata["description"])
	}

	// Test GetMetadata method
	if val, ok := q.GetMetadata("description"); !ok || val != "Test query" {
		t.Errorf("GetMetadata failed: got %s, %v", val, ok)
	}

	// Test GetMetadata with normalized key (case insensitive)
	if val, ok := q.GetMetadata("MAX-COST"); !ok || val != "100" {
		t.Errorf("GetMetadata case normalization failed: got %s, %v", val, ok)
	}

	// Test GetMetadata with non-existent key
	if val, ok := q.GetMetadata("nonexistent"); ok {
		t.Errorf("GetMetadata should return false for non-existent key, got %s", val)
	}

	// Test query with nil metadata
	q2 := NewQuery("test2", "test2.sql", "SELECT 2", nil)
	if q2.Metadata == nil {
		t.Error("NewQuery should initialize empty metadata map, not nil")
	}
}

func TestQueryStoreIteration(t *testing.T) {
	store := NewQueryStore()

	// Test empty store
	if len(store.QueryNames()) != 0 {
		t.Errorf("Expected empty query names, got %d items", len(store.QueryNames()))
	}

	if len(store.Queries()) != 0 {
		t.Errorf("Expected empty queries map, got %d items", len(store.Queries()))
	}

	// Add some queries
	queries := map[string]string{
		"get-user":    "SELECT * FROM users WHERE id = :id",
		"create-user": "INSERT INTO users (name) VALUES (:name)",
		"delete-user": "DELETE FROM users WHERE id = :id",
		"list-users":  "SELECT * FROM users ORDER BY name",
	}

	for name, query := range queries {
		q := NewQuery(name, "test.sql", query, nil)
		store.queries[name] = q
	}

	// Test QueryNames returns all names sorted
	names := store.QueryNames()
	expectedNames := []string{"create-user", "delete-user", "get-user", "list-users"}

	if len(names) != len(expectedNames) {
		t.Errorf("Expected %d query names, got %d", len(expectedNames), len(names))
	}

	if !reflect.DeepEqual(names, expectedNames) {
		t.Errorf("QueryNames returned incorrect names:\ngot:  %v\nwant: %v", names, expectedNames)
	}

	// Test Queries returns all queries
	allQueries := store.Queries()

	if len(allQueries) != len(queries) {
		t.Errorf("Expected %d queries, got %d", len(queries), len(allQueries))
	}

	for name := range queries {
		if _, ok := allQueries[name]; !ok {
			t.Errorf("Query '%s' not found in Queries() result", name)
		}
	}

	// Test that Queries returns a copy (modifying it shouldn't affect the store)
	allQueries["new-query"] = NewQuery("new-query", "new.sql", "SELECT 1", nil)

	if _, err := store.Query("new-query"); err == nil {
		t.Error("Modifying Queries() result should not affect the original store")
	}
}

func TestQueryPath(t *testing.T) {
	testCases := []struct {
		name         string
		queryName    string
		path         string
		query        string
		expectedPath string
	}{
		{
			name:         "Simple path",
			queryName:    "get-user",
			path:         "sql/users.sql",
			query:        "SELECT * FROM users WHERE id = :id",
			expectedPath: "sql/users.sql",
		},
		{
			name:         "Full path",
			queryName:    "create-user",
			path:         "/app/sql/queries/users/create.sql",
			query:        "INSERT INTO users (name) VALUES (:name)",
			expectedPath: "/app/sql/queries/users/create.sql",
		},
		{
			name:         "Relative path",
			queryName:    "test-query",
			path:         "test.sql",
			query:        "SELECT 1",
			expectedPath: "test.sql",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := NewQuery(tc.queryName, tc.path, tc.query, nil)

			if q.Path != tc.expectedPath {
				t.Errorf("Path mismatch: got %s, expected %s", q.Path, tc.expectedPath)
			}
		})
	}
}

func TestArgs(t *testing.T) {
	testCases := []struct {
		name         string
		query        string
		expectedArgs []string
	}{
		{
			name:         "Arguments with duplicates",
			query:        "SELECT * FROM users WHERE id = :a AND name = :a AND age = :b",
			expectedArgs: []string{"a", "a", "b"},
		},
		{
			name:         "No arguments",
			query:        "SELECT * FROM users",
			expectedArgs: []string{},
		},
		{
			name:         "Single argument",
			query:        "SELECT * FROM users WHERE id = :id",
			expectedArgs: []string{"id"},
		},
		{
			name:         "Multiple unique arguments",
			query:        "SELECT * FROM users WHERE id = :id AND name = :name AND age = :age",
			expectedArgs: []string{"id", "name", "age"},
		},
		{
			name:         "Multiple duplicates",
			query:        "SELECT * FROM users WHERE :x = :x OR :y = :y OR :x = :z",
			expectedArgs: []string{"x", "x", "y", "y", "x", "z"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := NewQuery(tc.name, "test.sql", tc.query, nil)
			args := q.Args

			if !reflect.DeepEqual(args, tc.expectedArgs) {
				t.Errorf("Args mismatch:\ngot:  %v\nwant: %v", args, tc.expectedArgs)
			}
		})
	}
}
