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
	psqlVarRE = `[^:]:['"]?([A-Za-z][A-Za-z0-9_]*)['"]?`
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
		Raw          string
		OrdinalQuery string
		Mapping      map[string]int
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

		q := NewQuery(name, scannedQuery.Query, scannedQuery.Metadata)

		s.queries[name] = q
	}

	return nil
}

func NewQuery(name, query string, metadata map[string]string) *Query {
	var (
		position int = 1
	)

	if metadata == nil {
		metadata = make(map[string]string)
	}

	q := Query{
		Name:     name,
		Raw:      query,
		Metadata: metadata,
	}

	// TODO: should drop
	mapping := make(map[string]int)
	namedArgs := []sql.NamedArg{}

	r, _ := regexp.Compile(psqlVarRE)
	matches := r.FindAllStringSubmatch(query, -1)

	for _, match := range matches {
		variable := match[1]

		if isReservedName(variable) {
			continue
		}

		if _, ok := mapping[variable]; !ok {
			mapping[variable] = position
			namedArgs = append(namedArgs, sql.Named(variable, nil))
			position++
		}
	}

	// replace the variable with ordinal markers
	for name, ord := range mapping {
		r, _ := regexp.Compile(fmt.Sprintf(`:["']?%s["']?`, name))
		query = r.ReplaceAllLiteralString(query, fmt.Sprintf("$%d", ord))
	}

	q.OrdinalQuery = fmt.Sprintf("-- name: %s\n%s", name, query)
	q.Mapping = mapping
	q.NamedArgs = namedArgs

	return &q
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
