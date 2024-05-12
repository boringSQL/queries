package queries

import (
	"database/sql"
	"reflect"
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
