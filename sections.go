package queries

import (
	"regexp"
	"strings"
	"sync"
)

var sectionMarkerRE = regexp.MustCompile(`(?m)^--\s*@(\w+)\s*$`)

type parsedSections struct {
	once     sync.Once
	sections map[string]string
	queries  map[string]*Query
	order    []string
}

// Section returns raw content for a @tag section.
func (q *Query) Section(tag string) string {
	q.ensureSectionsParsed()
	return q.parsedSections.sections[tag]
}

// SectionQuery returns a parsed Query for a @tag section, with its own Args and OrdinalQuery.
func (q *Query) SectionQuery(tag string) *Query {
	q.ensureSectionsParsed()

	raw := q.parsedSections.sections[tag]
	if raw == "" {
		return nil
	}

	if cached := q.parsedSections.queries[tag]; cached != nil {
		return cached
	}

	sq, err := NewQuery(q.Name+"@"+tag, q.Path, raw, q.Metadata)
	if err != nil {
		return nil
	}

	q.parsedSections.queries[tag] = sq
	return sq
}

// HasSection reports whether the query contains a non-empty @tag section.
func (q *Query) HasSection(tag string) bool {
	q.ensureSectionsParsed()
	return q.parsedSections.sections[tag] != ""
}

// SectionNames returns @tag section names in order of appearance.
func (q *Query) SectionNames() []string {
	q.ensureSectionsParsed()
	if len(q.parsedSections.order) <= 1 {
		return nil
	}
	return append([]string(nil), q.parsedSections.order[1:]...)
}

func (q *Query) ensureSectionsParsed() {
	q.parsedSections.once.Do(func() {
		q.parsedSections.sections, q.parsedSections.order = parseSections(q.Raw)
		q.parsedSections.queries = make(map[string]*Query)
	})
}

func parseSections(raw string) (map[string]string, []string) {
	sections := make(map[string]string)
	order := []string{""}

	matches := sectionMarkerRE.FindAllStringSubmatchIndex(raw, -1)
	if len(matches) == 0 {
		sections[""] = strings.TrimSpace(raw)
		return sections, order
	}

	sections[""] = strings.TrimSpace(raw[:matches[0][0]])

	for i, m := range matches {
		tag := strings.ToLower(raw[m[2]:m[3]])
		order = append(order, tag)

		end := len(raw)
		if i+1 < len(matches) {
			end = matches[i+1][0]
		}
		sections[tag] = strings.TrimSpace(raw[m[1]:end])
	}

	return sections, order
}
