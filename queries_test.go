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
			q := NewQuery(tc.name, tc.inputQuery)
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

			query, ok := queries[tc.expectedQueryName]
			if !ok {
				t.Errorf("Query '%s' not found. Available queries: %v", tc.expectedQueryName, queries)
			}

			if query != tc.expectedQuery {
				t.Errorf("Query content: got %s, expected %s", query, tc.expectedQuery)
			}
		})
	}
}
