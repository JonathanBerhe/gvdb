const BASE = '/api';

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || res.statusText);
  }
  return res.json();
}

export interface Stats {
  total_collections: number;
  total_vectors: number;
  total_queries: number;
  avg_query_time_ms: number;
}

export interface Collection {
  id: number;
  name: string;
  dimension: number;
  vector_count: number;
}

export interface SearchResult {
  id: number;
  distance: number;
  metadata?: Record<string, unknown>;
}

export interface SearchResponse {
  results: SearchResult[];
  query_time_ms: number;
}

export interface VectorEntry {
  id: number;
  dimension: number;
  metadata?: Record<string, unknown>;
}

export interface ListVectorsResponse {
  vectors: VectorEntry[];
  total_count: number;
  has_more: boolean;
}

export const api = {
  health: () => request<{ status: string; message: string }>('/health'),

  stats: () => request<Stats>('/stats'),

  listCollections: () => request<Collection[]>('/collections'),

  createCollection: (data: {
    name: string;
    dimension: number;
    metric: string;
    index_type: string;
  }) => request<{ collection_id: number }>('/collections', {
    method: 'POST',
    body: JSON.stringify(data),
  }),

  dropCollection: (name: string) =>
    request<{ message: string }>(`/collections/${name}`, { method: 'DELETE' }),

  listVectors: (name: string, limit = 20, offset = 0, metadata = true) =>
    request<ListVectorsResponse>(
      `/collections/${name}/vectors?limit=${limit}&offset=${offset}&metadata=${metadata}`
    ),

  search: (name: string, data: {
    query_vector: number[];
    top_k?: number;
    filter?: string;
    return_metadata?: boolean;
  }) => request<SearchResponse>(`/collections/${name}/search`, {
    method: 'POST',
    body: JSON.stringify(data),
  }),

  hybridSearch: (name: string, data: {
    query_vector?: number[];
    text_query?: string;
    top_k?: number;
    vector_weight?: number;
    text_weight?: number;
    text_field?: string;
    return_metadata?: boolean;
  }) => request<SearchResponse>(`/collections/${name}/hybrid-search`, {
    method: 'POST',
    body: JSON.stringify(data),
  }),
};
