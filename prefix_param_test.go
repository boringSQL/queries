package queries

import (
	"regexp"
	"strings"
	"testing"
)

func TestNamedParamPrefixCollision(t *testing.T) {
	const sql = `SELECT :start_date AS a, :start_date_from AS b, :start_date_to AS c`
	junk := regexp.MustCompile(`\$\d+[A-Za-z0-9_]`)

	for i := 0; i < 200; i++ {
		q, err := NewQuery("t", "t.sql", sql, nil)
		if err != nil {
			t.Fatalf("NewQuery: %v", err)
		}
		if m := junk.FindString(q.OrdinalQuery); m != "" {
			t.Fatalf("run %d: parameter bled into a longer name (%q):\n%s", i, m, q.OrdinalQuery)
		}
		for _, name := range []string{"start_date", "start_date_from", "start_date_to"} {
			if _, ok := q.Mapping[name]; !ok {
				t.Fatalf("run %d: %q missing from mapping %v", i, name, q.Mapping)
			}
		}
		if n := strings.Count(q.OrdinalQuery, "$"); n != 3 {
			t.Fatalf("run %d: expected 3 placeholders, got %d:\n%s", i, n, q.OrdinalQuery)
		}
	}
}

func TestNamedParamSubstitutionIsDeterministic(t *testing.T) {
	const sql = `SELECT :a, :ab, :abc, :abcd FROM t WHERE x = :a`
	first, err := NewQuery("t", "t.sql", sql, nil)
	if err != nil {
		t.Fatalf("NewQuery: %v", err)
	}
	for i := 0; i < 100; i++ {
		q, err := NewQuery("t", "t.sql", sql, nil)
		if err != nil {
			t.Fatalf("NewQuery: %v", err)
		}
		if q.OrdinalQuery != first.OrdinalQuery {
			t.Fatalf("run %d differs:\n first: %s\n  this: %s", i, first.OrdinalQuery, q.OrdinalQuery)
		}
	}
}
