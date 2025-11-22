package queries

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestParametersInCommentsNotDetected(t *testing.T) {
	testCases := []struct {
		name         string
		query        string
		expectedArgs []string
		expectedSQL  []sql.NamedArg
	}{
		{
			name: "Positional param in comment only",
			query: `-- $1
SELECT 42;`,
			expectedArgs: []string{},
			expectedSQL:  []sql.NamedArg{},
		},
		{
			name: "Named param in comment only",
			query: `-- :user_id
SELECT 42;`,
			expectedArgs: []string{},
			expectedSQL:  []sql.NamedArg{},
		},
		{
			name: "Multiple params in comment",
			query: `-- $1, $2, :name, :email
SELECT 42;`,
			expectedArgs: []string{},
			expectedSQL:  []sql.NamedArg{},
		},
		{
			name: "Real positional param with comment",
			query: `-- $1
SELECT * FROM users WHERE id = $1;`,
			expectedArgs: []string{"arg1"},
			expectedSQL: []sql.NamedArg{
				sql.Named("arg1", nil),
			},
		},
		{
			name: "Real named param with comment",
			query: `-- :user_id
SELECT * FROM users WHERE id = :user_id;`,
			expectedArgs: []string{"user_id"},
			expectedSQL: []sql.NamedArg{
				sql.Named("user_id", nil),
			},
		},
		{
			name: "Real param different from comment",
			query: `-- $1, :fake_param
SELECT * FROM users WHERE id = :real_param;`,
			expectedArgs: []string{"real_param"},
			expectedSQL: []sql.NamedArg{
				sql.Named("real_param", nil),
			},
		},
		{
			name:         "Inline comment with positional param",
			query:        `SELECT * FROM users WHERE status = 'active'; -- $1`,
			expectedArgs: []string{},
			expectedSQL:  []sql.NamedArg{},
		},
		{
			name:         "Inline comment with named param",
			query:        `SELECT * FROM users WHERE status = 'active'; -- :user_id`,
			expectedArgs: []string{},
			expectedSQL:  []sql.NamedArg{},
		},
		{
			name:         "Real param with inline comment",
			query:        `SELECT * FROM users WHERE id = :user_id AND status = 'active'; -- :user_id`,
			expectedArgs: []string{"user_id"},
			expectedSQL: []sql.NamedArg{
				sql.Named("user_id", nil),
			},
		},
		{
			name: "Multiple real params with comments",
			query: `-- $1, $2
SELECT * FROM users WHERE name = :name AND email = :email`,
			expectedArgs: []string{"name", "email"},
			expectedSQL: []sql.NamedArg{
				sql.Named("name", nil),
				sql.Named("email", nil),
			},
		},
		{
			name: "Comment at end",
			query: `SELECT * FROM users WHERE id = :user_id;
-- $1`,
			expectedArgs: []string{"user_id"},
			expectedSQL: []sql.NamedArg{
				sql.Named("user_id", nil),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := NewQuery(tc.name, "test.sql", tc.query, nil)

			if !reflect.DeepEqual(q.Args, tc.expectedArgs) {
				t.Errorf("Args: got %v, want %v", q.Args, tc.expectedArgs)
			}

			if !reflect.DeepEqual(q.NamedArgs, tc.expectedSQL) {
				t.Errorf("NamedArgs: got %v, want %v", q.NamedArgs, tc.expectedSQL)
			}

			if len(q.Mapping) != len(tc.expectedArgs) {
				t.Errorf("Mapping size: got %d, want %d", len(q.Mapping), len(tc.expectedArgs))
			}
		})
	}
}

func TestCommentStylesWithParameters(t *testing.T) {
	t.Run("Various comment styles", func(t *testing.T) {
		queries := []string{
			`-- $1
SELECT 42;`,
			`-- :param
SELECT 42;`,
			`--$1
SELECT 42;`,
			`--:param
SELECT 42;`,
			`  -- $1
SELECT 42;`,
			`	-- :param
SELECT 42;`,
		}

		for i, query := range queries {
			q, _ := NewQuery("test", "test.sql", query, nil)
			if len(q.Args) != 0 {
				t.Errorf("Query %d: expected no parameters, got %v", i, q.Args)
			}
		}
	})

	t.Run("CASE statement with comments", func(t *testing.T) {
		query := `SELECT CASE
    WHEN :flag = true THEN true
    ELSE cost IS NOT NULL
END FROM products`

		q, _ := NewQuery("test", "test.sql", query, nil)

		expectedArgs := []string{"flag"}
		if !reflect.DeepEqual(q.Args, expectedArgs) {
			t.Errorf("Args: got %v, want %v", q.Args, expectedArgs)
		}
	})
}

func TestEdgeCasesWithComments(t *testing.T) {
	t.Run("String literals with parameter syntax", func(t *testing.T) {
		query := `SELECT * FROM users WHERE name = ':not_a_param' AND id = :real_param`
		q, _ := NewQuery("test", "test.sql", query, nil)

		foundReal := false
		for _, arg := range q.Args {
			if arg == "real_param" {
				foundReal = true
			}
		}

		if !foundReal {
			t.Errorf("Expected real_param, got: %v", q.Args)
		}
	})

	t.Run("Documentation in comments", func(t *testing.T) {
		query := `-- $1
SELECT * FROM users WHERE id = :user_id`

		q, _ := NewQuery("test", "test.sql", query, nil)

		expectedArgs := []string{"user_id"}
		if !reflect.DeepEqual(q.Args, expectedArgs) {
			t.Errorf("Args: got %v, want %v", q.Args, expectedArgs)
		}
	})

	t.Run("No parameters", func(t *testing.T) {
		query := `-- $1 and :param
SELECT * FROM users WHERE status = 'active'`

		q, _ := NewQuery("test", "test.sql", query, nil)

		if len(q.Args) != 0 {
			t.Errorf("Expected no parameters, got: %v", q.Args)
		}
	})
}
