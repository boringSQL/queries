package queries

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

func TestQueryVariableDetection(t *testing.T) {
	testQuery := `SELECT
    product_id,
    product_name,
    category,
    price,
    cost,
    COALESCE(cost, 0) as cost_safe,
    price - COALESCE(cost, 0) as margin
FROM source_products
WHERE
    CASE
        WHEN :include_unknown_cost = true THEN true  -- Include all products
        ELSE cost IS NOT NULL  -- Exclude products with NULL cost
    END
ORDER BY product_id;`

	t.Run("Detects include_unknown_cost variable", func(t *testing.T) {
		q, err := NewQuery("test-query", "test.sql", testQuery, nil)
		if err != nil {
			t.Fatalf("NewQuery() error: %v", err)
		}

		// Check that the variable was detected in Args
		expectedArgs := []string{"include_unknown_cost"}
		if !reflect.DeepEqual(q.Args, expectedArgs) {
			t.Errorf("Args mismatch:\ngot:  %v\nwant: %v", q.Args, expectedArgs)
		}

		// Check that the variable was added to NamedArgs
		expectedNamedArgs := []sql.NamedArg{
			sql.Named("include_unknown_cost", nil),
		}
		if !reflect.DeepEqual(q.NamedArgs, expectedNamedArgs) {
			t.Errorf("NamedArgs mismatch:\ngot:  %v\nwant: %v", q.NamedArgs, expectedNamedArgs)
		}

		// Check that the variable was added to Mapping
		if ordinal, ok := q.Mapping["include_unknown_cost"]; !ok {
			t.Errorf("Variable 'include_unknown_cost' not found in Mapping")
		} else if ordinal != 1 {
			t.Errorf("Variable 'include_unknown_cost' has ordinal %d, expected 1", ordinal)
		}

		// Verify that the OrdinalQuery contains the replaced parameter
		if !strings.Contains(q.OrdinalQuery, "$1") {
			t.Errorf("OrdinalQuery does not contain $1:\n%s", q.OrdinalQuery)
		}
	})

	t.Run("Prepare method works with include_unknown_cost", func(t *testing.T) {
		q, err := NewQuery("test-query", "test.sql", testQuery, nil)
		if err != nil {
			t.Fatalf("NewQuery() error: %v", err)
		}

		testCases := []struct {
			name     string
			args     map[string]interface{}
			expected interface{}
		}{
			{
				name:     "with true",
				args:     map[string]interface{}{"include_unknown_cost": true},
				expected: true,
			},
			{
				name:     "with false",
				args:     map[string]interface{}{"include_unknown_cost": false},
				expected: false,
			},
			{
				name:     "with missing argument",
				args:     map[string]interface{}{},
				expected: nil,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				preparedArgs := q.Prepare(tc.args)

				if len(preparedArgs) != 1 {
					t.Errorf("Expected 1 prepared arg, got %d", len(preparedArgs))
				}

				if preparedArgs[0] != tc.expected {
					t.Errorf("Expected prepared arg to be %v, got %v", tc.expected, preparedArgs[0])
				}
			})
		}
	})
}
