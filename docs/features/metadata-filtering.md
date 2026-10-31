# Metadata filtering

Filter search results with SQL-like predicates over per-vector metadata. Accelerated by scalar metadata indexes for common patterns.

## Attach metadata

Metadata is a JSON-shaped dict per vector:

```python
client.insert(
    "products",
    ids=[1, 2, 3],
    vectors=[...],
    metadata=[
        {"category": "electronics", "price": 199.0, "in_stock": True},
        {"category": "books", "price": 14.99, "in_stock": True},
        {"category": "electronics", "price": 899.0, "in_stock": False},
    ],
)
```

## Filter at search time

```python
results = client.search(
    "products",
    query_vector=[...],
    top_k=10,
    filter_expression="category = 'electronics' AND price < 500.0 AND in_stock = true",
    return_metadata=True,
)
```

## Supported operators

| Operator | Example |
|----------|---------|
| `=`, `!=` | `category = 'electronics'` |
| `<`, `>`, `<=`, `>=` | `price >= 100.0` |
| `IN`, `NOT IN` | `category IN ('books', 'electronics')` |
| `LIKE` | `title LIKE 'python%'` |
| `AND`, `OR`, `NOT` | `a = 1 AND (b > 2 OR NOT c = 'x')` |
| Parentheses | Arbitrary grouping |

Keywords are case-insensitive. Field names follow the metadata key rules (`[A-Za-z_][A-Za-z0-9_]*`).

## Literals and types

| Type | Syntax | Notes |
|------|--------|-------|
| String | `'text'` or `"text"` | A backslash escapes the next character: `'O\'Brien'` |
| Integer | `42`, `-7`, `+3` | 64-bit signed. Literals outside `[-2^63, 2^63-1]` are rejected |
| Float | `4.5`, `-0.25`, `1.` | Must contain a `.`; exponent notation (`1e3`) is not supported |
| Boolean | `true`, `false` | Lowercase only |

Comparisons are strictly typed: an integer literal never matches a float field and vice versa. If `price` was stored as `199.0`, write `price < 500.0`, not `price < 500`.

The whole expression must parse. Trailing text such as an unbalanced `)` is an error rather than being ignored, so a malformed fragment can never silently drop the rest of a filter.

## LIKE patterns

`%` matches any run of characters (including none), `_` matches exactly one byte, and matching is case-sensitive and anchored at both ends. To match a literal `%` or `_`, escape it with a backslash in the pattern. Because the string literal itself also consumes one backslash, the filter text needs two:

```python
filter_expression=r"discount LIKE '50\\%'"   # matches "50%", not "50 off"
```

## Limits

Parentheses and `NOT` can be nested up to 64 levels deep. `AND` / `OR` chains and `IN` lists have no length limit beyond the request size.

## Scalar metadata indexes

GVDB auto-builds per-field indexes for metadata:

- **Bitmap inverted index** for equality on strings/bools
- **Sorted numeric index** for range queries on ints/floats

This turns predicate evaluation into a posting-list intersection that runs **before** the ANN probe, shrinking the candidate set dramatically for selective filters.

## Hybrid with vector search

Metadata filters compose with all search modes:

- [Dense vector search](vector-search.md)
- [Sparse vector search](sparse-vectors.md)
- [Hybrid search](hybrid-search.md) with BM25

## Further reading

- [Python SDK — client API](../python-sdk/client.md) for method signatures
