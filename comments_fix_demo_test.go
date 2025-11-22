package queries

import (
	"testing"
)

func TestCommentParameterHandling(t *testing.T) {
	t.Run("Parameters only in comments", func(t *testing.T) {
		query := `-- name: query1
-- $1 parameter
SELECT 42;`

		q, _ := NewQuery("query1", "test.sql", query, nil)

		if len(q.Args) != 0 {
			t.Errorf("Expected no parameters, got: %v", q.Args)
		}
	})

	t.Run("Real parameter with comment mentioning parameter", func(t *testing.T) {
		query := `-- :user_id and $1
SELECT * FROM users WHERE id = :user_id;`

		q, _ := NewQuery("test", "test.sql", query, nil)

		if len(q.Args) != 1 || q.Args[0] != "user_id" {
			t.Errorf("Expected [user_id], got: %v", q.Args)
		}
	})

	t.Run("Multiple parameters with inline comments", func(t *testing.T) {
		query := `-- $1, $2, $3
SELECT product_id, price - COALESCE(cost, 0) as margin
FROM products
WHERE category = :category AND price > :min_price`

		q, _ := NewQuery("test", "test.sql", query, nil)

		expectedParams := map[string]bool{"category": true, "min_price": true}
		if len(q.Args) != len(expectedParams) {
			t.Errorf("Expected %d parameters, got %d", len(expectedParams), len(q.Args))
		}

		for _, arg := range q.Args {
			if !expectedParams[arg] {
				t.Errorf("Unexpected parameter: %s", arg)
			}
		}
	})
}
