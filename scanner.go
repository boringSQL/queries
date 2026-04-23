// Credits to https://github.com/gchaincl/dotsql
package queries

import (
	"bufio"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

type (
	ScannedQuery struct {
		Query    string
		Metadata map[string]string
	}

	Scanner struct {
		line        string
		queries     map[string]*ScannedQuery
		defaultName string
		pending     pendingBlock
	}

	pendingBlock struct {
		name     string
		query    string
		metadata map[string]string
		started  bool
	}

	blockDirective struct {
		name     string
		metadata map[string]string
	}
)

var (
	tagDashRE      = regexp.MustCompile(`^\s*--\s*name:\s*(\S+)`)
	metaDashRE     = regexp.MustCompile(`^\s*--\s*([a-zA-Z][a-zA-Z0-9_-]*):\s*(.+?)\s*$`)
	blockInnerRE   = regexp.MustCompile(`^\s*/\*\s*(.+?)\s*\*/\s*$`)
	directiveKeyRE = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)
	blockCommentRE = regexp.MustCompile(`^\s*/\*.*\*/\s*$`)
)

func getTag(line string) string {
	if matches := tagDashRE.FindStringSubmatch(line); matches != nil {
		return matches[1]
	}
	return ""
}

func getMetadata(line string) (string, string, bool) {
	matches := metaDashRE.FindStringSubmatch(line)
	if matches == nil || matches[1] == "name" {
		return "", "", false
	}
	// Normalize key: lowercase and trim
	key := strings.ToLower(strings.TrimSpace(matches[1]))
	value := strings.TrimSpace(matches[2])
	return key, value, true
}

func parseBlockDirective(line string) (blockDirective, bool) {
	m := blockInnerRE.FindStringSubmatch(line)
	if m == nil {
		return blockDirective{}, false
	}

	parts := strings.Split(m[1], ",")
	d := blockDirective{}
	for _, p := range parts {
		sepIdx := firstSeparator(p)
		if sepIdx < 0 {
			return blockDirective{}, false
		}
		rawKey := strings.TrimSpace(p[:sepIdx])
		rawVal := strings.TrimSpace(p[sepIdx+1:])
		if !directiveKeyRE.MatchString(rawKey) {
			return blockDirective{}, false
		}
		value, ok := decodeDirectiveValue(rawVal)
		if !ok {
			return blockDirective{}, false
		}
		key := strings.ToLower(rawKey)
		if key == "name" {
			d.name = value
			continue
		}
		if d.metadata == nil {
			d.metadata = make(map[string]string)
		}
		d.metadata[key] = value
	}
	return d, true
}

// firstSeparator accepts `:` (native/Marginalia) or `=` (sqlcommenter).
func firstSeparator(s string) int {
	ci := strings.IndexByte(s, ':')
	ei := strings.IndexByte(s, '=')
	switch {
	case ci < 0:
		return ei
	case ei < 0:
		return ci
	case ci < ei:
		return ci
	default:
		return ei
	}
}

// decodeDirectiveValue unwraps sqlcommenter's quoted+URL-encoded values;
// bare values are returned unchanged.
func decodeDirectiveValue(raw string) (string, bool) {
	if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
		inner := raw[1 : len(raw)-1]
		inner = strings.ReplaceAll(inner, "''", "'")
		decoded, err := url.PathUnescape(inner)
		if err != nil {
			return "", false
		}
		return decoded, true
	}
	return raw, true
}

func (s *Scanner) processLine() {
	if tag := getTag(s.line); tag != "" {
		s.handleName(tag)
		return
	}

	if d, ok := parseBlockDirective(s.line); ok {
		if d.name != "" {
			s.handleName(d.name)
		}
		for k, v := range d.metadata {
			s.putMetadata(k, v)
		}
		return
	}

	if key, value, ok := getMetadata(s.line); ok {
		s.putMetadata(key, value)
		return
	}

	trimmed := strings.TrimSpace(s.line)
	if trimmed == "" {
		return
	}

	// Inside a query body, non-directive comments are kept verbatim.
	if !s.pending.started {
		if strings.HasPrefix(trimmed, "--") {
			return
		}
		if blockCommentRE.MatchString(s.line) {
			return
		}
	}

	line := strings.Trim(s.line, " \t")
	if line == "" {
		return
	}
	if s.pending.query != "" {
		s.pending.query += "\n"
	}
	s.pending.query += line
	s.pending.started = true
}

// handleName commits the pending block before switching names so successive
// queries don't merge.
func (s *Scanner) handleName(tag string) {
	if s.pending.query != "" && s.pending.name != "" && s.pending.name != tag {
		s.commit()
	}
	s.pending.name = tag
	s.pending.started = true
}

func (s *Scanner) putMetadata(key, value string) {
	if s.pending.metadata == nil {
		s.pending.metadata = make(map[string]string)
	}
	s.pending.metadata[key] = value
}

// commit flushes the pending block. Empty bodies are dropped; duplicate
// names concatenate.
func (s *Scanner) commit() {
	if s.pending.query == "" {
		s.pending = pendingBlock{}
		return
	}
	name := s.pending.name
	if name == "" {
		name = s.defaultName
	}
	if existing, ok := s.queries[name]; ok {
		if existing.Query != "" {
			existing.Query += "\n"
		}
		existing.Query += s.pending.query
		for k, v := range s.pending.metadata {
			existing.Metadata[k] = v
		}
	} else {
		meta := s.pending.metadata
		if meta == nil {
			meta = make(map[string]string)
		}
		s.queries[name] = &ScannedQuery{
			Query:    s.pending.query,
			Metadata: meta,
		}
	}
	s.pending = pendingBlock{}
}

func (s *Scanner) Run(fileName string, io *bufio.Scanner) map[string]*ScannedQuery {
	s.queries = make(map[string]*ScannedQuery)
	s.defaultName = filepath.Base(strings.TrimSuffix(fileName, filepath.Ext(fileName)))
	s.pending = pendingBlock{}

	for io.Scan() {
		s.line = io.Text()
		s.processLine()
	}
	s.commit()

	return s.queries
}
