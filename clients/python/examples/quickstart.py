"""GVDB Python SDK quickstart example.

Prerequisites:
    pip install gvdb

    # Start GVDB server:
    ./build/bin/gvdb-single-node --port 50051 --data-dir /tmp/gvdb
"""

import random

from gvdb import GVDBClient


def main():
    # Connect
    client = GVDBClient("localhost:50051")
    print(f"Health: {client.health_check()}")

    # Create collection
    collection_name = "quickstart_demo"
    try:
        client.drop_collection(collection_name)
    except Exception:
        pass

    client.create_collection(collection_name, dimension=128, metric="l2", index_type="hnsw")
    print(f"Created collection: {collection_name}")

    # Insert 100 vectors with metadata
    ids = list(range(1, 101))
    vectors = [[random.gauss(0, 1) for _ in range(128)] for _ in range(100)]
    metadata = [{"category": f"cat_{i % 5}", "score": random.random()} for i in range(100)]

    inserted = client.insert(collection_name, ids, vectors, metadata=metadata)
    print(f"Inserted {inserted} vectors")

    # Search
    query = [random.gauss(0, 1) for _ in range(128)]
    results = client.search(collection_name, query, top_k=5, return_metadata=True)
    print(f"\nTop 5 results:")
    for r in results:
        print(f"  ID={r.id}, distance={r.distance:.4f}, metadata={r.metadata}")

    # Search with filter
    results = client.search(
        collection_name,
        query,
        top_k=5,
        filter_expression="category = 'cat_0'",
        return_metadata=True,
    )
    print(f"\nFiltered results (category='cat_0'):")
    for r in results:
        print(f"  ID={r.id}, distance={r.distance:.4f}, metadata={r.metadata}")

    # Get by ID
    fetched = client.get(collection_name, [1, 2, 3])
    print(f"\nFetched {len(fetched)} vectors by ID")

    # Clean up
    client.drop_collection(collection_name)
    print(f"\nDropped collection: {collection_name}")
    client.close()


if __name__ == "__main__":
    main()
