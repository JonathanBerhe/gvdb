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
    filter_expression="category = 'electronics' AND price < 500 AND in_stock = true",
    return_metadata=True,
)
```

## Supported operators

| Operator | Example |
|----------|---------|
| `=`, `!=` | `category = 'electronics'` |
| `<`, `>`, `<=`, `>=` | `price >= 100` |
| `IN`, `NOT IN` | `category IN ('books', 'electronics')` |
| `LIKE` | `title LIKE 'python%'` |
| `AND`, `OR`, `NOT` | `a = 1 AND (b > 2 OR NOT c = 'x')` |
| Parentheses | Arbitrary grouping |

Values can be strings (single-quoted), integers, floats, or booleans.

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
- [Architecture — storage](../architecture/storage.md) for how scalar indexes are persisted
