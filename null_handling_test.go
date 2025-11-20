package queries

import (
	"testing"
)

func TestNullHandlingSQLFile(t *testing.T) {
	// Create a new query store
	store := NewQueryStore()

	// Load the SQL file
	err := store.LoadFromFile("testdata/null_handling.sql")
	if err != nil {
		t.Fatalf("Failed to load SQL file: %v", err)
	}

	// Expected queries and their parameters
	expectedQueries := map[string][]string{
		"anonymous-events":              {},
		"events-by-user-type":           {},
		"null-aggregation-counts":       {},
		"products-with-default-cost":    {},
		"campaign-status-with-nulls":    {},
		"events-with-nulls-complex":     {},
		"sessions-country-match":        {},
		"products-nullif-example":       {},
		"events-by-user-nullable":       {"user_id"},
		"products-optional-cost-filter": {"include_unknown_cost"},
	}

	// Test 1: Check that we have the correct number of queries
	t.Run("Correct number of queries", func(t *testing.T) {
		queryNames := store.QueryNames()
		if len(queryNames) != len(expectedQueries) {
			t.Errorf("Expected %d queries, got %d", len(expectedQueries), len(queryNames))
			t.Logf("Found queries: %v", queryNames)
		}
	})

	// Test 2: Check that all expected queries are present
	t.Run("All expected queries present", func(t *testing.T) {
		for expectedName := range expectedQueries {
			_, err := store.Query(expectedName)
			if err != nil {
				t.Errorf("Expected query '%s' not found: %v", expectedName, err)
			}
		}
	})

	// Test 3: Check parameters for each query
	t.Run("Query parameters", func(t *testing.T) {
		for queryName, expectedParams := range expectedQueries {
			q, err := store.Query(queryName)
			if err != nil {
				t.Errorf("Query '%s' not found: %v", queryName, err)
				continue
			}

			// Check the number of unique parameters (from Mapping)
			if len(q.Mapping) != len(expectedParams) {
				t.Errorf("Query '%s': expected %d parameters, got %d",
					queryName, len(expectedParams), len(q.Mapping))
				t.Logf("  Expected params: %v", expectedParams)
				t.Logf("  Got params: %v", q.Args)
				t.Logf("  Mapping: %v", q.Mapping)
			}

			// Check that the expected parameters are present
			for _, expectedParam := range expectedParams {
				if _, ok := q.Mapping[expectedParam]; !ok {
					t.Errorf("Query '%s': expected parameter '%s' not found in mapping",
						queryName, expectedParam)
				}
			}
		}
	})

	// Test 4: Specific query parameter tests
	t.Run("events-by-user-nullable has user_id parameter", func(t *testing.T) {
		q, err := store.Query("events-by-user-nullable")
		if err != nil {
			t.Fatalf("Query not found: %v", err)
		}

		if len(q.Args) != 1 || q.Args[0] != "user_id" {
			t.Errorf("Expected Args=['user_id'], got %v", q.Args)
		}

		if ordinal, ok := q.Mapping["user_id"]; !ok || ordinal != 1 {
			t.Errorf("Expected user_id mapped to ordinal 1, got %v (exists: %v)",
				ordinal, ok)
		}
	})

	t.Run("products-optional-cost-filter has include_unknown_cost parameter", func(t *testing.T) {
		q, err := store.Query("products-optional-cost-filter")
		if err != nil {
			t.Fatalf("Query not found: %v", err)
		}

		if len(q.Args) != 1 || q.Args[0] != "include_unknown_cost" {
			t.Errorf("Expected Args=['include_unknown_cost'], got %v", q.Args)
		}

		if ordinal, ok := q.Mapping["include_unknown_cost"]; !ok || ordinal != 1 {
			t.Errorf("Expected include_unknown_cost mapped to ordinal 1, got %v (exists: %v)",
				ordinal, ok)
		}
	})

	t.Run("non-parameterized queries have no parameters", func(t *testing.T) {
		nonParamQueries := []string{
			"anonymous-events",
			"events-by-user-type",
			"null-aggregation-counts",
			"products-with-default-cost",
			"campaign-status-with-nulls",
			"events-with-nulls-complex",
			"sessions-country-match",
			"products-nullif-example",
		}

		for _, queryName := range nonParamQueries {
			q, err := store.Query(queryName)
			if err != nil {
				t.Errorf("Query '%s' not found: %v", queryName, err)
				continue
			}

			if len(q.Args) != 0 {
				t.Errorf("Query '%s' should have no parameters, got %v", queryName, q.Args)
			}

			if len(q.Mapping) != 0 {
				t.Errorf("Query '%s' should have empty mapping, got %v", queryName, q.Mapping)
			}
		}
	})

	// Test 5: Summary report
	t.Run("Summary report", func(t *testing.T) {
		t.Logf("\n=== Query Summary ===")
		t.Logf("Total queries: %d", len(store.QueryNames()))

		queriesWithParams := 0
		queriesWithoutParams := 0

		for _, name := range store.QueryNames() {
			q, _ := store.Query(name)
			if len(q.Args) > 0 {
				queriesWithParams++
				t.Logf("  %s (%d params: %v)", name, len(q.Args), q.Args)
			} else {
				queriesWithoutParams++
			}
		}

		t.Logf("\nQueries with parameters: %d", queriesWithParams)
		t.Logf("Queries without parameters: %d", queriesWithoutParams)
	})
}
