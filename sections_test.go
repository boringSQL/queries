package queries

import (
	"reflect"
	"strings"
	"testing"
)

func TestSectionNoMarkers(t *testing.T) {
	raw := "SELECT * FROM users WHERE id = :id"
	q, _ := NewQuery("test", "test.sql", raw, nil)

	// No markers = no sections
	if q.Section("verify") != "" {
		t.Error("Section(\"verify\") should be empty")
	}
	if q.HasSection("verify") {
		t.Error("HasSection(\"verify\") should be false")
	}
	if names := q.SectionNames(); len(names) != 0 {
		t.Errorf("SectionNames() = %v, want []", names)
	}
}

func TestSectionSimpleVerify(t *testing.T) {
	raw := `INSERT INTO orders (user_id, total) VALUES (:user_id, 100);
-- @verify
SELECT * FROM orders WHERE user_id = :user_id;`

	q, _ := NewQuery("insert-order", "test.sql", raw, nil)

	wantVerify := "SELECT * FROM orders WHERE user_id = :user_id;"

	if got := q.Section("verify"); got != wantVerify {
		t.Errorf("Section(\"verify\") = %q, want %q", got, wantVerify)
	}
	if !q.HasSection("verify") {
		t.Error("HasSection(\"verify\") should be true")
	}
	if names := q.SectionNames(); !reflect.DeepEqual(names, []string{"verify"}) {
		t.Errorf("SectionNames() = %v, want [\"verify\"]", names)
	}
}

func TestSectionMultipleMarkers(t *testing.T) {
	raw := `-- @setup
DELETE FROM orders WHERE user_id = 99;
-- @execute
INSERT INTO orders (user_id, total) VALUES (99, 50);
-- @verify
SELECT * FROM orders WHERE user_id = 99;`

	q, _ := NewQuery("full-test", "test.sql", raw, nil)

	tests := []struct {
		tag  string
		want string
	}{
		{"", ""},
		{"setup", "DELETE FROM orders WHERE user_id = 99;"},
		{"execute", "INSERT INTO orders (user_id, total) VALUES (99, 50);"},
		{"verify", "SELECT * FROM orders WHERE user_id = 99;"},
	}

	for _, tt := range tests {
		if got := q.Section(tt.tag); got != tt.want {
			t.Errorf("Section(%q) = %q, want %q", tt.tag, got, tt.want)
		}
	}

	want := []string{"setup", "execute", "verify"}
	if got := q.SectionNames(); !reflect.DeepEqual(got, want) {
		t.Errorf("SectionNames() = %v, want %v", got, want)
	}
}

func TestSectionFlexibleWhitespace(t *testing.T) {
	markers := []string{
		"-- @verify",
		"--  @verify",
		"--   @verify",
		"--\t@verify",
		"-- \t @verify",
	}

	for _, marker := range markers {
		raw := "INSERT INTO foo VALUES (1);\n" + marker + "\nSELECT * FROM foo;"
		q, _ := NewQuery("test", "test.sql", raw, nil)

		if !q.HasSection("verify") {
			t.Errorf("marker %q: HasSection(\"verify\") = false", marker)
		}
		if got := q.Section("verify"); got != "SELECT * FROM foo;" {
			t.Errorf("marker %q: Section(\"verify\") = %q", marker, got)
		}
	}
}

func TestSectionCaseInsensitive(t *testing.T) {
	raw := "INSERT INTO foo VALUES (1);\n-- @VERIFY\nSELECT * FROM foo;"
	q, _ := NewQuery("test", "test.sql", raw, nil)

	if !q.HasSection("verify") {
		t.Error("HasSection should be case insensitive")
	}
}

func TestSectionEmpty(t *testing.T) {
	raw := "SELECT * FROM foo;\n-- @verify"
	q, _ := NewQuery("test", "test.sql", raw, nil)

	if q.Section("verify") != "" {
		t.Error("empty section should return empty string")
	}
	if q.HasSection("verify") {
		t.Error("HasSection should be false for empty section")
	}
}

func TestSectionQuery(t *testing.T) {
	raw := `INSERT INTO orders (user_id, total) VALUES (:user_id, :total);
-- @verify
SELECT user_id, total FROM orders WHERE user_id = :user_id;`

	q, _ := NewQuery("insert-order", "test.sql", raw, nil)

	verifyQ := q.SectionQuery("verify")
	if verifyQ == nil {
		t.Fatal("SectionQuery(\"verify\") = nil")
	}
	if verifyQ.Name != "insert-order@verify" {
		t.Errorf("verify.Name = %q", verifyQ.Name)
	}
	if !reflect.DeepEqual(verifyQ.Args, []string{"user_id"}) {
		t.Errorf("verify.Args = %v", verifyQ.Args)
	}
	if !strings.Contains(verifyQ.OrdinalQuery, "$1") {
		t.Error("verify.OrdinalQuery should contain $1")
	}
}

func TestSectionQueryCaching(t *testing.T) {
	raw := "SELECT 1;\n-- @verify\nSELECT 2;"
	q, _ := NewQuery("test", "test.sql", raw, nil)

	q1 := q.SectionQuery("verify")
	q2 := q.SectionQuery("verify")
	if q1 != q2 {
		t.Error("SectionQuery should return cached result")
	}
}

func TestSectionQueryNil(t *testing.T) {
	q, _ := NewQuery("test", "test.sql", "SELECT 1;", nil)

	if q.SectionQuery("verify") != nil {
		t.Error("non-existent section should return nil")
	}

	q2, _ := NewQuery("test", "test.sql", "SELECT 1;\n-- @verify", nil)
	if q2.SectionQuery("verify") != nil {
		t.Error("empty section should return nil")
	}
}

func TestSectionNamesImmutable(t *testing.T) {
	q, _ := NewQuery("test", "test.sql", "SELECT 1;\n-- @verify\nSELECT 2;", nil)

	names := q.SectionNames()
	names[0] = "modified"

	if q.SectionNames()[0] != "verify" {
		t.Error("SectionNames should return a copy")
	}
}

func TestBackwardCompatibility(t *testing.T) {
	raw := `INSERT INTO orders VALUES (1);
-- @verify
SELECT * FROM orders;`

	q, _ := NewQuery("test", "test.sql", raw, nil)

	if q.RawQuery() != raw {
		t.Error("RawQuery() should return full query including markers")
	}
	if !strings.Contains(q.Query(), "@verify") {
		t.Error("Query() should include @verify marker")
	}
}
