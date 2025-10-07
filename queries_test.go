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
			q := NewQuery(tc.name, tc.inputQuery, nil)
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

	q := NewQuery("test-query", "SELECT 1", metadata)

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
	q2 := NewQuery("test2", "SELECT 2", nil)
	if q2.Metadata == nil {
		t.Error("NewQuery should initialize empty metadata map, not nil")
	}
}
