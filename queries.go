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

func (s *QueryStore) loadQueriesFromFile(fileName string, r io.Reader) error {
	scanner := &Scanner{}
	newQueries := scanner.Run(fileName, bufio.NewScanner(r))

	for name, query := range newQueries {
		// insert query (but check whatever it already exists)
		if _, ok := s.queries[name]; ok {
			return fmt.Errorf("Query '%s' already exists", name)
		}

		q := NewQuery(name, query)

		s.queries[name] = q
	}

	return nil
}

func NewQuery(name, query string) *Query {
	var (
		position int = 1
	)

	q := Query{
		Name: name,
		Raw:  query,
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

// Prepare the arguments for the ordinal query. Missing arguments will
// be returned as nil
func (q *Query) Prepare(args map[string]interface{}) []interface{} {
	type kv struct {
		Name string
		Ord  int
	}

	var components []interface{}

	// number of components is query and ordinal mapping count
	components = make([]interface{}, len(q.Mapping))
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
