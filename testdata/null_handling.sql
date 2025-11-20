-- Query 2.1: NULL in WHERE clause - Find anonymous events
-- Key Learning: Use IS NULL, not = NULL
-- name: anonymous-events
SELECT
    event_id,
    session_id,
    event_type,
    event_timestamp,
    user_id  -- Will be NULL
FROM source_events
WHERE user_id IS NULL
ORDER BY event_timestamp;

-- Expected: 5 events from anonymous sessions


-- Query 2.2: NULL in JOIN conditions - Events with and without users
-- Key Learning: NULL doesn't match in joins, use LEFT JOIN to keep them
-- name: events-by-user-type
SELECT
    e.event_id,
    e.event_type,
    e.user_id,
    u.email,
    CASE
        WHEN e.user_id IS NULL THEN 'Anonymous'
        WHEN u.user_id IS NOT NULL THEN 'Identified'
        ELSE 'Unknown'  -- This case won't occur with LEFT JOIN
    END as user_type
FROM source_events e
LEFT JOIN source_users u ON e.user_id = u.user_id
ORDER BY e.event_timestamp;

-- Expected: All 23 events, 5 marked as Anonymous


-- Query 2.3: NULL in aggregations - Aggregates ignore NULLs
-- Key Learning: COUNT(*) vs COUNT(column) vs COUNT(DISTINCT column)
-- name: null-aggregation-counts
SELECT
    event_type,
    COUNT(*) as total_events,
    COUNT(user_id) as events_with_user,  -- NULLs not counted
    COUNT(DISTINCT user_id) as unique_users,  -- NULLs not counted
    COUNT(product_id) as events_with_product,
    COUNT(revenue) as events_with_revenue
FROM source_events
GROUP BY event_type
ORDER BY total_events DESC;

-- Expected: Shows difference between COUNT(*) and COUNT(column)


-- Query 2.4: COALESCE for NULL handling - Provide default values
-- Key Learning: COALESCE returns first non-NULL value
-- name: products-with-default-cost
SELECT
    product_id,
    product_name,
    category,
    price,
    cost,
    COALESCE(cost, 0) as cost_with_default,
    price - COALESCE(cost, 0) as estimated_margin,
    COALESCE(supplier_id, -1) as supplier_id_safe
FROM source_products
ORDER BY product_id;

-- Expected: NULLs replaced with defaults for cost and supplier_id


-- Query 2.5: NULL in CASE expressions
-- Key Learning: Test for NULL explicitly in CASE
-- name: campaign-status-with-nulls
SELECT
    campaign_id,
    campaign_name,
    start_date,
    end_date,
    CASE
        WHEN end_date IS NULL THEN 'Ongoing'
        WHEN end_date < CURRENT_DATE THEN 'Completed'
        WHEN end_date >= CURRENT_DATE THEN 'Active'
    END as campaign_status,
    CASE
        WHEN budget IS NULL THEN 'No Budget Set'
        WHEN budget > 20000 THEN 'High Budget'
        WHEN budget > 10000 THEN 'Medium Budget'
        ELSE 'Low Budget'
    END as budget_category
FROM marketing_campaigns
ORDER BY start_date;

-- Expected: Proper handling of NULL end_date and budget


-- Query 2.6: NULL in WHERE with AND/OR - Complex NULL logic
-- Key Learning: NULL in boolean expressions can be tricky
-- name: events-with-nulls-complex
SELECT
    e.event_id,
    e.event_type,
    e.user_id,
    e.product_id,
    e.revenue
FROM source_events e
WHERE
    -- Events that are either anonymous OR have no product
    (e.user_id IS NULL OR e.product_id IS NULL)
    -- But exclude page_view events
    AND e.event_type != 'page_view'
ORDER BY e.event_timestamp;

-- Expected: product_view and purchase events with NULLs


-- Query 2.7: NULL-safe equality with IS NOT DISTINCT FROM
-- Key Learning: Compare values that might be NULL
-- name: sessions-country-match
SELECT
    s.session_id,
    s.user_id,
    u.user_id as joined_user_id,
    s.country as session_country,
    u.country as user_country,
    CASE
        WHEN s.country IS NOT DISTINCT FROM u.country THEN 'Match'
        ELSE 'Mismatch'
    END as country_match_status
FROM dim_sessions s
LEFT JOIN source_users u ON s.user_id = u.user_id
ORDER BY s.session_start;

-- Expected: Handles NULL country comparisons correctly


-- Query 2.8: NULLIF to create NULLs conditionally
-- Key Learning: NULLIF returns NULL if two values are equal
-- name: products-nullif-example
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    NULLIF(subcategory, '') as subcategory_clean,  -- Convert empty strings to NULL
    NULLIF(category, subcategory) as category_if_different  -- NULL when same
FROM source_products
ORDER BY product_id;

-- Expected: Demonstrates conditional NULL creation


-- PARAMETERIZED VERSIONS

-- Query 2.9: Find events with optional user filter (parameterized)
-- Parameters: user_id (can be NULL to find anonymous events)
-- Key Learning: Handling NULL parameters correctly
-- name: events-by-user-nullable
SELECT
    event_id,
    event_type,
    event_timestamp,
    user_id,
    product_id
FROM source_events
WHERE user_id IS NOT DISTINCT FROM :user_id  -- Works with NULL parameter
ORDER BY event_timestamp;

-- Expected: Finds events for specified user_id or anonymous events when user_id is NULL


-- Query 2.10: Products with optional cost filter (parameterized)
-- Parameters: include_unknown_cost (boolean)
-- name: products-optional-cost-filter
SELECT
    product_id,
    product_name,
    category,
    price,
    cost,
    COALESCE(cost, 0) as cost_safe,
    price - COALESCE(cost, 0) as margin
FROM source_products
WHERE
    CASE
        WHEN :include_unknown_cost = true THEN true  -- Include all products
        ELSE cost IS NOT NULL  -- Exclude products with NULL cost
    END
ORDER BY product_id;

-- Expected: When include_unknown_cost is true, returns all products; when false, excludes products with NULL cost
