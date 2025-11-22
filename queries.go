package queries

import (
	"bufio"
	"database/sql"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	positionalParamRE = `\$(\d+)`
	colonParamRE      = `[^:]:['"]?([A-Za-z][A-Za-z0-9_]*)['"]?`
	atSignParamRE     = `[^@]@['"]?([A-Za-z][A-Za-z0-9_]*)['"]?`
	namedParamRE      = `[:@]["']?%s["']?` // Template for replacement
)

var (
	reservedNames = []string{"MI", "SS"}
)

type (
	QueryStore struct {
		queries map[string]*Query
	}

	Query struct {
		Name         string
		Path         string
		Raw          string
		OrdinalQuery string
		Mapping      map[string]int
		Args         []string
		NamedArgs    []sql.NamedArg
		Metadata     map[string]string
	}
)

// NewQueryStore setups new query store
func NewQueryStore() *QueryStore {
	return &QueryStore{
		queries: make(map[string]*Query),
	}
}

// LoadFromFile loads query/queries from specified file
func (s *QueryStore) LoadFromFile(fileName string) (err error) {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	return s.loadQueriesFromFile(fileName, file)
}

func (s *QueryStore) LoadFromDir(path string) error {
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("Directory does not exist: %s", path)
	}

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(strings.ToLower(filePath), ".sql") {
			err = s.LoadFromFile(filePath)
			if err != nil {
				return fmt.Errorf("Error loading SQL file '%s': %v", filePath, err)
			}
		}

		return nil
	})

	return err
}

func (qs *QueryStore) LoadFromEmbed(sqlFS embed.FS, path string) error {
	dirEntries, err := fs.ReadDir(sqlFS, path)
	if err != nil {
		return err
	}

	for _, entry := range dirEntries {
		filePath := entry.Name()

		if !entry.IsDir() && strings.HasSuffix(strings.ToLower(filePath), ".sql") {
			file, err := sqlFS.Open(filepath.Join(path, filePath))
			if err != nil {
				return fmt.Errorf("Error opening SQL file '%s': %v", filePath, err)
			}
			defer file.Close()

			err = qs.loadQueriesFromFile(filePath, file)
			if err != nil {
				return fmt.Errorf("Error loading SQL file '%s': %v", filePath, err)
			}
		}
	}

	return nil
}

// MustHaveQuery returns query or panics on error
func (s *QueryStore) MustHaveQuery(name string) *Query {
	query, err := s.Query(name)
	if err != nil {
		panic(err)
	}

	return query
}

// Query retrieve query by given name
func (s *QueryStore) Query(name string) (*Query, error) {
	query, ok := s.queries[name]
	if !ok {
		return nil, fmt.Errorf("Query '%s' not found", name)
	}

	return query, nil
}

// QueryNames returns a sorted list of all query names in the store
func (s *QueryStore) QueryNames() []string {
	names := make([]string, 0, len(s.queries))
	for name := range s.queries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Queries returns all queries in the store as a map
func (s *QueryStore) Queries() map[string]*Query {
	// Return a copy to prevent external modification
	queriesCopy := make(map[string]*Query, len(s.queries))
	for name, query := range s.queries {
		queriesCopy[name] = query
	}
	return queriesCopy
}

func (s *QueryStore) loadQueriesFromFile(fileName string, r io.Reader) error {
	scanner := &Scanner{}
	newQueries := scanner.Run(fileName, bufio.NewScanner(r))

	for name, scannedQuery := range newQueries {
		// insert query (but check whatever it already exists)
		if _, ok := s.queries[name]; ok {
			return fmt.Errorf("Query '%s' already exists", name)
		}

		q, err := NewQuery(name, fileName, scannedQuery.Query, scannedQuery.Metadata)
		if err != nil {
			return fmt.Errorf("Error creating query '%s': %w", name, err)
		}

		s.queries[name] = q
	}

	return nil
}

// stripSQLComments removes SQL single-line comments (--) from a query string.
// It returns a copy of the query with all comment content removed, while
// preserving the structure and line breaks of the original query.
func stripSQLComments(query string) string {
	lines := strings.Split(query, "\n")
	result := make([]string, 0, len(lines))

	for _, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

func NewQuery(name, path, query string, metadata map[string]string) (*Query, error) {
	if metadata == nil {
		metadata = make(map[string]string)
	}

	q := Query{
		Name:     name,
		Path:     path,
		Raw:      query,
		Metadata: metadata,
	}

	// Strip comments to avoid detecting parameters within comment text
	cleanQuery := stripSQLComments(query)

	// Detect all parameter types
	positionalMatches := regexp.MustCompile(positionalParamRE).FindAllStringSubmatch(cleanQuery, -1)
	colonMatches := filterReservedNames(regexp.MustCompile(colonParamRE).FindAllStringSubmatch(cleanQuery, -1))
	atSignMatches := filterReservedNames(regexp.MustCompile(atSignParamRE).FindAllStringSubmatch(cleanQuery, -1))

	// Validate that only one parameter style is used
	if err := validateSingleParameterStyle(name, positionalMatches, colonMatches, atSignMatches); err != nil {
		return nil, err
	}

	// Process based on detected parameter type
	if len(positionalMatches) > 0 {
		return handlePositionalParams(&q, name, query, cleanQuery, positionalMatches), nil
	}

	// Handle named parameters (colon or at-sign style)
	return handleNamedParams(&q, name, query, cleanQuery), nil
}

func filterReservedNames(matches [][]string) [][]string {
	filtered := make([][]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 && !isReservedName(match[1]) {
			filtered = append(filtered, match)
		}
	}
	return filtered
}

func validateSingleParameterStyle(queryName string, positional, colon, atSign [][]string) error {
	styles := []string{}

	if len(positional) > 0 {
		styles = append(styles, "positional ($1, $2, ...)")
	}
	if len(colon) > 0 {
		styles = append(styles, "colon (:param)")
	}
	if len(atSign) > 0 {
		styles = append(styles, "at-sign (@param)")
	}

	if len(styles) > 1 {
		return fmt.Errorf("mixed parameter styles detected in query '%s': found %s. Use only one style per query",
			queryName, strings.Join(styles, " and "))
	}

	return nil
}

func handlePositionalParams(q *Query, name, query, cleanQuery string, matches [][]string) *Query {
	// Find the highest parameter number
	maxParam := 0

	for _, match := range matches {
		num := 0
		fmt.Sscanf(match[1], "%d", &num)
		if num > maxParam {
			maxParam = num
		}
	}

	// Create synthetic names and populate Args/NamedArgs
	mapping := make(map[string]int)
	namedArgs := []sql.NamedArg{}
	args := []string{}

	// Create args for each positional parameter in order
	for i := 1; i <= maxParam; i++ {
		syntheticName := fmt.Sprintf("arg%d", i)
		args = append(args, syntheticName)
		mapping[syntheticName] = i
		namedArgs = append(namedArgs, sql.Named(syntheticName, nil))
	}

	q.OrdinalQuery = fmt.Sprintf("-- name: %s\n%s", name, query)
	q.Mapping = mapping
	q.Args = args
	q.NamedArgs = namedArgs

	return q
}

func handleNamedParams(q *Query, name, query, cleanQuery string) *Query {
	mapping := make(map[string]int)
	namedArgs := []sql.NamedArg{}
	args := []string{}
	position := 1

	// Match both colon and at-sign parameters
	colonMatches := regexp.MustCompile(colonParamRE).FindAllStringSubmatch(cleanQuery, -1)
	atSignMatches := regexp.MustCompile(atSignParamRE).FindAllStringSubmatch(cleanQuery, -1)

	// Combine matches (only one type will have results due to validation)
	allMatches := append(colonMatches, atSignMatches...)

	for _, match := range allMatches {
		if len(match) < 2 {
			continue
		}

		variable := match[1]
		if isReservedName(variable) {
			continue
		}

		// Collect all variable occurrences (including duplicates)
		args = append(args, variable)

		if _, ok := mapping[variable]; !ok {
			mapping[variable] = position
			namedArgs = append(namedArgs, sql.Named(variable, nil))
			position++
		}
	}

	// Replace named parameters with positional markers ($1, $2, etc.)
	for paramName, ord := range mapping {
		pattern := regexp.MustCompile(fmt.Sprintf(namedParamRE, paramName))
		query = pattern.ReplaceAllLiteralString(query, fmt.Sprintf("$%d", ord))
	}

	q.OrdinalQuery = fmt.Sprintf("-- name: %s\n%s", name, query)
	q.Mapping = mapping
	q.Args = args
	q.NamedArgs = namedArgs

	return q
}

// Query returns ordinal query
func (q *Query) Query() string {
	return q.OrdinalQuery
}

func (q *Query) RawQuery() string {
	return q.Raw
}

// GetMetadata retrieves a metadata value by key
func (q *Query) GetMetadata(key string) (string, bool) {
	// Normalize the key to lowercase for consistent lookup
	key = strings.ToLower(strings.TrimSpace(key))
	value, ok := q.Metadata[key]
	return value, ok
}

// Prepare the arguments for the ordinal query. Missing arguments will
// be returned as nil
func (q *Query) Prepare(args map[string]interface{}) []interface{} {
	type kv struct {
		Name string
		Ord  int
	}

	// number of components is query and ordinal mapping count
	components := make([]interface{}, len(q.Mapping))
	var params []kv
	for k, v := range q.Mapping {
		params = append(params, kv{k, v})
	}

	sort.Slice(params, func(i, j int) bool {
		return params[i].Ord < params[j].Ord
	})

	for i, param := range params {
		components[i] = args[param.Name]
	}

	return components
}

func isReservedName(name string) bool {
	for _, res := range reservedNames {
		if name == res {
			return true
		}
	}

	return false
}
