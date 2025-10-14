# Queries

The `queries` library for Go implements a file based SQL query management system. Only PostgreSQL is supported. 

## Installing

```
  go get -u github.com/boringsql/queries
```

## Usage

Writing and using SQL code in Go code can be inneficient for several reasons. Not only it diminishes the advantages of a statically typed language, it also worsens editor support for syntax highlighting and indentation, and hinders query reusability with other tools. The `queries` library has been created to address these issues by providing a better way to manage and use SQL queries in Go.


With `queries` you define your SQL code in files like `sql/users.sql`

```sql
-- name: get-user-by-id
SELECT *
FROM users 
WHERE user_id = :user_id AND deleted_at is null

-- name: update-user-last-login
UPDATE users
SET last_login_at = current_timestamp
WHERE user_id = :user_id
```

Load all the respective files into QueryStore using three different ways

```go
err = queryStore.LoadFromFile("sql/users.sql")
if err != nil {
  return err
}

err = queryStore.LoadFromDir("sql/")
if err != nil {
  return err
}

//go:embed sql/*
var sqlFS embed.FS
err = queryStore.LoadFromEmbed("sql/")
if err != nil {
  return err
}

```

Once you get the query loaded you can access them by their name and prepare the named parameter mapping 


```go
getUser := queryStore.MustHaveQuery("get-user-by-id")
args := getUser.Prepare(map[string]interface{}{
  "user_id": 123,
})

err = db.Get(&user, getUser.Query(), args...)
if err != nil {
  return err
}
```

## Query format

The recommended use of the `queries` library is to switch from the default positional parameter notation ($1, $2, etc. - dollar quited sign followed by the parameter position) to [psql variable definition](https://www.postgresql.org/docs/current/app-psql.html#APP-PSQL-VARIABLES).

The benefit of the variable definition is better visual control. Other aspect is the inter-operability with other PostgreSQL tools. Notably [regresql](https://github.com/boringSQL/regresql).

If you prefer the default dolar sign positional parameters, you can skip the argument preparation (`queryStore.Prepare`) and use the `query.Raw`.

## Query Metadata

You can add metadata to your queries using `-- key: value` format. Metadata provides a way to attach additional information to queries, such as descriptions, performance requirements, or custom tags.

```sql
-- name: get-user-by-email
-- description: Retrieve user by email efficiently
-- max-cost: 100
-- required-nodes: Index Scan
-- timeout: 50ms
SELECT id, name, email
FROM users
WHERE email = :email
```

### Metadata Key Format

- Keys must start with a letter and can contain letters, numbers, underscores, and hyphens
- Keys are case-insensitive (automatically normalized to lowercase)
- The `name` key is reserved for query naming and not stored as metadata
- Values preserve their original formatting and can contain any characters

### Accessing Metadata

Once loaded, you can access metadata in two ways:

```go
query := queryStore.MustHaveQuery("get-user-by-email")

// Direct access to the Metadata map
description := query.Metadata["description"]

// Using the GetMetadata method (case-insensitive key lookup)
if timeout, ok := query.GetMetadata("timeout"); ok {
    fmt.Printf("Query timeout: %s\n", timeout)
}

// Access with any case
if cost, ok := query.GetMetadata("MAX-COST"); ok {
    fmt.Printf("Max cost: %s\n", cost)
}
```

### Common Use Cases

Metadata can be useful for:

- **Documentation**: Add descriptions and usage notes
- **Performance monitoring**: Track expected costs, timeouts, and required query plans
- **Query categorization**: Tag queries by feature, team, or purpose
- **Authorization**: Store required permissions or roles
- **Versioning**: Track query versions or migration information

Example with multiple metadata fields:

```sql
-- name: complex-analytics-query
-- description: Daily revenue calculation for reporting dashboard
-- author: analytics-team
-- version: 2.1
-- tags: reporting, revenue, daily
-- max-execution-time: 5s
-- cache-ttl: 3600
SELECT
    date_trunc('day', created_at) as day,
    sum(amount) as revenue
FROM orders
WHERE created_at >= :start_date
GROUP BY day
ORDER BY day DESC
```

## Credits

The `queries` library is heavily influenced (and in some cases re-uses part of the logic) by

* [regresql](https://github.com/dimitri/regresql)
* [yesql](https://github.com/krisajenkins/yesql)
