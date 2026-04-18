# Semantic search

Use GVDB to power product search, documentation search, recommendations, image retrieval, and anomaly detection.

## Product search

Index product descriptions with a sentence-transformer model. Combine with BM25 for exact term matches (SKUs, brand names):

```python
results = client.hybrid_search(
    "products",
    query_vector=embed("cozy winter jacket for hiking"),
    text_query="winter jacket hiking",
    text_field="description",
    filter_expression="in_stock = true AND price < 300",
    top_k=20,
    return_metadata=True,
)
```

## Documentation search

Embed doc chunks. Return the raw text via metadata so you can render it inline:

```python
client.insert(
    "docs",
    ids=ids,
    vectors=dense_embeddings,
    metadata=[{"text": chunk, "url": url, "title": title} for ...],
)

hits = client.search(
    "docs",
    query_vector=embed(query),
    top_k=5,
    return_metadata=True,
)
for h in hits:
    print(h.metadata["title"], h.metadata["url"])
```

## Recommendations

**Item-to-item** recommendations are a nearest-neighbour problem over item embeddings. Store embeddings for every item; at recommendation time, search with the user's history centroid or last-viewed item:

```python
# "Users who viewed X also viewed..."
results = client.search(
    "catalog",
    query_vector=client.get("catalog", [viewed_item_id])[0].vector,
    top_k=10,
    filter_expression="category = 'electronics'",
)
```

## Image retrieval

Embed images with CLIP or a domain-specific vision model. Search by image or text (CLIP text tower):

```python
# Search by image embedding
results = client.search("images", query_vector=clip_encode_image(img), top_k=20)

# Search by text (CLIP's aligned text space)
results = client.search("images", query_vector=clip_encode_text("a dog on a beach"), top_k=20)
```

## Anomaly detection

Anomalies are points far from their nearest neighbours in embedding space. Use [range search](../python-sdk/client.md) with a radius threshold, or compute the mean distance to the top-k and flag outliers:

```python
results = client.search("transactions", query_vector=candidate, top_k=10)
mean_dist = sum(r.distance for r in results) / len(results)
if mean_dist > THRESHOLD:
    flag_anomaly()
```

## See also

- [Vector search](../features/vector-search.md) — choosing the right index
- [Metadata filtering](../features/metadata-filtering.md) — SQL-like predicates
- [Hybrid search](../features/hybrid-search.md) — dense + BM25
