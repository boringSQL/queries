// Credits to https://github.com/gchaincl/dotsql
package queries

import (
	"bufio"
	"path/filepath"
	"regexp"
	"strings"
)

type ScannedQuery struct {
	Query    string
	Metadata map[string]string
}

type Scanner struct {
	line     string
	queries  map[string]*ScannedQuery
	current  string
	metadata map[string]map[string]string
}

type stateFn func(*Scanner) stateFn

func getTag(line string) string {
	re := regexp.MustCompile("^\\s*--\\s*name:\\s*(\\S+)")
	matches := re.FindStringSubmatch(line)
	if matches == nil {
		return ""
	}
	return matches[1]
}

func getMetadata(line string) (string, string, bool) {
	re := regexp.MustCompile("^\\s*--\\s*([a-zA-Z][a-zA-Z0-9_-]*):\\s*(.+)\\s*$")
	matches := re.FindStringSubmatch(line)
	if matches == nil || matches[1] == "name" {
		return "", "", false
	}
	// Normalize key: lowercase and trim
	key := strings.ToLower(strings.TrimSpace(matches[1]))
	value := strings.TrimSpace(matches[2])
	return key, value, true
}

func initialState(s *Scanner) stateFn {
	// Check for name directive
	if tag := getTag(s.line); len(tag) > 0 {
		s.current = tag
		return queryState
	}

	// Skip empty lines and comment lines (including metadata) before first name directive
	trimmed := strings.TrimSpace(s.line)
	if trimmed == "" || strings.HasPrefix(trimmed, "--") {
		return initialState
	}

	// Found actual SQL code, use filename as query name and process this line
	s.appendQueryLine()
	return queryState
}

func queryState(s *Scanner) stateFn {
	if tag := getTag(s.line); len(tag) > 0 {
		s.current = tag
	} else if key, value, ok := getMetadata(s.line); ok {
		s.appendMetadata(key, value)
	} else {
		s.appendQueryLine()
	}
	return queryState
}

func (s *Scanner) appendQueryLine() {
	line := strings.Trim(s.line, " \t")
	if len(line) == 0 {
		return
	}

	if s.queries[s.current] == nil {
		s.queries[s.current] = &ScannedQuery{
			Query:    "",
			Metadata: make(map[string]string),
		}
	}

	current := s.queries[s.current].Query
	if len(current) > 0 {
		current = current + "\n"
	}

	current = current + line
	s.queries[s.current].Query = current
}

func (s *Scanner) appendMetadata(key, value string) {
	if s.queries[s.current] == nil {
		s.queries[s.current] = &ScannedQuery{
			Query:    "",
			Metadata: make(map[string]string),
		}
	}
	s.queries[s.current].Metadata[key] = value
}

func (s *Scanner) Run(fileName string, io *bufio.Scanner) map[string]*ScannedQuery {
	s.queries = make(map[string]*ScannedQuery)

	s.current = filepath.Base(strings.TrimSuffix(fileName, filepath.Ext(fileName)))

	for state := initialState; io.Scan(); {
		s.line = io.Text()
		state = state(s)
	}

	return s.queries
}
