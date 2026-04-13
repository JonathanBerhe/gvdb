# Changelog

## [0.13.0](https://github.com/JonathanBerhe/gvdb/compare/v0.12.0...v0.13.0) (2026-04-13)


### Features

* **index:** Apple Metal GPU acceleration — 16-24x speedup on Apple Silicon ([94f1271](https://github.com/JonathanBerhe/gvdb/commit/94f1271a8777d11710bc9c9bab8ce687b4c286d6))
* **index:** Apple Metal GPU acceleration — 16-24x speedup on Apple Silicon ([1e01a44](https://github.com/JonathanBerhe/gvdb/commit/1e01a441016acc13b036a8418d8295e417e9de05))

## [0.12.0](https://github.com/JonathanBerhe/gvdb/compare/v0.11.0...v0.12.0) (2026-04-12)


### Features

* **network:** structured audit logging ([4db999f](https://github.com/JonathanBerhe/gvdb/commit/4db999f2f64562912edf32ad26767822da48e3a6))
* **network:** structured audit logging — JSON interceptor, per-RPC trail, opt-in config ([3ba6917](https://github.com/JonathanBerhe/gvdb/commit/3ba6917a882dd231f460ab4f3febe439a5f6c11b))

## [0.11.0](https://github.com/JonathanBerhe/gvdb/compare/v0.10.0...v0.11.0) (2026-04-12)


### Features

* **sdk:** bulk data import — Parquet, NumPy, DataFrame, CSV, h5ad ([7b75382](https://github.com/JonathanBerhe/gvdb/commit/7b75382e6528000c09a57e2ba2c393df9aa22714))
* **sdk:** bulk data import — Parquet, NumPy, DataFrame, CSV, h5ad ([de93d0d](https://github.com/JonathanBerhe/gvdb/commit/de93d0dcd2a6b3db7a5f1104def16661078b9991))


### Bug Fixes

* **sdk:** retry safety, RESOURCE_EXHAUSTED handling, NaN checks in bulk importers ([a1993c5](https://github.com/JonathanBerhe/gvdb/commit/a1993c5193dee089360e30c60e691a6b42da3f7c))

## [0.10.0](https://github.com/JonathanBerhe/gvdb/compare/v0.9.0...v0.10.0) (2026-04-12)


### Features

* **cluster:** implement dynamic shard rebalancing execution ([73dd296](https://github.com/JonathanBerhe/gvdb/commit/73dd296abc976dbca3763aa2ce21252b128d50c9))
* **cluster:** implement dynamic shard rebalancing execution ([a3d1832](https://github.com/JonathanBerhe/gvdb/commit/a3d18323c3a9a73d0a23694769bdb8c5c5ed1062))


### Bug Fixes

* **cluster:** RAII scope guard, primary-move RemoveReplica, snapshot-consistent rebalance plan ([cd37a76](https://github.com/JonathanBerhe/gvdb/commit/cd37a7671b00977373272fe3bb4f2251498e2811))

## [0.9.0](https://github.com/JonathanBerhe/gvdb/compare/v0.8.0...v0.9.0) (2026-04-11)


### Features

* **storage:** add S3/MinIO tiered storage with ISegmentStore interface ([181b7ac](https://github.com/JonathanBerhe/gvdb/commit/181b7ac696e4b10db12a25d92f937d6e8c64679d))
* **storage:** add S3/MinIO tiered storage with ISegmentStore interface ([80891b0](https://github.com/JonathanBerhe/gvdb/commit/80891b081865ff4d609aa99bedd84f69ecca2f24))


### Bug Fixes

* **storage,network:** fix S3 tiered storage bugs and add RangeSearch distributed fan-out ([0328024](https://github.com/JonathanBerhe/gvdb/commit/03280240a086c64b9b2a63a0f624821cb05aa2ae))

## [0.8.0](https://github.com/JonathanBerhe/gvdb/compare/v0.7.0...v0.8.0) (2026-04-10)


### Features

* **auth:** add RBAC with 4 roles and per-collection scoping ([3b0097e](https://github.com/JonathanBerhe/gvdb/commit/3b0097e6a8c959f306d88640f350405436a9b969))
* **auth:** add RBAC with 4 roles and per-collection scoping ([7c998bf](https://github.com/JonathanBerhe/gvdb/commit/7c998bf511dcc2e78e34bb3ba3ec59a418cfead5))
* **sdk:** add Python SDK test suite, lint, CI, and fix search filter bug ([c6c2f9f](https://github.com/JonathanBerhe/gvdb/commit/c6c2f9f0ace7344a4b9620c1a78a609c8f3bc4a6))


### Bug Fixes

* **auth:** close thread-local leak, fail-open bypass, and substring auth bypass ([a04fccc](https://github.com/JonathanBerhe/gvdb/commit/a04fccc774bd19cceba995d3e2f8e2513a9d95ef))
* **auth:** validate RBAC config — reject unknown roles, empty keys, missing collections ([884d18f](https://github.com/JonathanBerhe/gvdb/commit/884d18fe5c101e21d8637f2024f5693c474425a4))
* **network:** add missing Upsert, RangeSearch, ListVectors to proxy ([e58c4e9](https://github.com/JonathanBerhe/gvdb/commit/e58c4e93bd8fd44d941f053ca4e92174822f48a7))
* **sdk:** fix list_collections missing auth header and update_metadata field mismatch ([c4c5c06](https://github.com/JonathanBerhe/gvdb/commit/c4c5c06606dcea27cb738bd973cece641a3b15a3))

## [0.7.0](https://github.com/JonathanBerhe/gvdb/compare/v0.6.0...v0.7.0) (2026-04-08)


### Features

* **index:** add AUTO index type with per-segment selection ([771af33](https://github.com/JonathanBerhe/gvdb/commit/771af33785e65e59f9c53b6b64010fec0fa639f6))
* **index:** add AUTO index type with per-segment selection ([a56c22a](https://github.com/JonathanBerhe/gvdb/commit/a56c22ad3d7dd3aeb20038d429272ddde7266d7b))

## [0.6.0](https://github.com/JonathanBerhe/gvdb/compare/v0.5.0...v0.6.0) (2026-04-08)


### Features

* **storage:** add per-vector TTL with background sweep and expiry filtering ([970f110](https://github.com/JonathanBerhe/gvdb/commit/970f11013562f46ba9a8000eb5f6026996c2e160))
* **storage:** per-vector TTL with background sweep and expiry filtering ([15b18d7](https://github.com/JonathanBerhe/gvdb/commit/15b18d72a64eb3a557e8be2fd414285ee6b64733))


### Bug Fixes

* **storage:** resolve TTL race conditions, perf, and correctness issues ([e0670a8](https://github.com/JonathanBerhe/gvdb/commit/e0670a8e722da092d1bbb908c12d97149cca4ae6))

## [0.5.0](https://github.com/JonathanBerhe/gvdb/compare/v0.4.0...v0.5.0) (2026-04-07)


### Features

* **index:** add sparse vector support with three-way hybrid retrieval ([22df808](https://github.com/JonathanBerhe/gvdb/commit/22df80846cf59d2d8c92fd2d3537a8f08e0560b6))
* **index:** sparse vector support with three-way hybrid retrieval ([5377528](https://github.com/JonathanBerhe/gvdb/commit/5377528a2a7f636b7c3291e12d6f146dffed03a0))

## [0.4.0](https://github.com/JonathanBerhe/gvdb/compare/v0.3.1...v0.4.0) (2026-04-06)


### Features

* **cluster:** coordinator-driven read repair ([16aab64](https://github.com/JonathanBerhe/gvdb/commit/16aab647e978a8f6846aa794ed4dbb83ce5f1217))
* **cluster:** coordinator-driven read repair with background consistency checking ([6e106cb](https://github.com/JonathanBerhe/gvdb/commit/6e106cb511027b51565edd9c0dd9e94a754071b7))

## [0.3.1](https://github.com/JonathanBerhe/gvdb/compare/v0.3.0...v0.3.1) (2026-04-06)


### Bug Fixes

* **storage,network:** iterator safety, typed shard metadata, insert error propagation ([b241984](https://github.com/JonathanBerhe/gvdb/commit/b2419847ffdd0a9752990266d0cdcf1edbc9d63b))
* **storage:** segment rotation with capacity-aware GetWritableSegment and background build loop ([f53631f](https://github.com/JonathanBerhe/gvdb/commit/f53631f8ca5ceb7589835fec8b9b574c9e062df9))
* **storage:** segment rotation, distributed build loop, and metadata type preservation ([6a2af72](https://github.com/JonathanBerhe/gvdb/commit/6a2af7269984f527396722b5f11d7d2e10c5b33d))

## [0.3.0](https://github.com/JonathanBerhe/gvdb/compare/v0.2.0...v0.3.0) (2026-04-04)


### Features

* **index:** add IVF_TURBOQUANT — IVF partitioning with TurboQuant compression ([76d8d2e](https://github.com/JonathanBerhe/gvdb/commit/76d8d2e0da90ee1d51d9eda070c483d70154a86f))
* **index:** add IVF_TURBOQUANT — IVF partitioning with TurboQuant compression ([5998f4b](https://github.com/JonathanBerhe/gvdb/commit/5998f4b8d163d8df7cf96a26ab4ab8e1db4ff0f1))

## [0.2.0](https://github.com/JonathanBerhe/gvdb/compare/v0.1.8...v0.2.0) (2026-04-04)


### Features

* **cluster:** implement data node index building and segment compaction ([a6023ec](https://github.com/JonathanBerhe/gvdb/commit/a6023ecd363e59fc239cc70455b10fedf4345c03))
* **cluster:** implement data node index building and segment compaction ([045b73c](https://github.com/JonathanBerhe/gvdb/commit/045b73cabd00936e008871283dc92d7c0fd20f32))
* **python:** add upsert() and range_search() to Python SDK ([4c34722](https://github.com/JonathanBerhe/gvdb/commit/4c34722115a5886f560ea646d3823586469539e4))
* **storage,index:** add scalar metadata indexes, upsert, and range search ([2bfb975](https://github.com/JonathanBerhe/gvdb/commit/2bfb975434280f77aca90112c1c7fc463ce4629d))
* **storage,index:** add scalar metadata indexes, upsert, and range search ([dac227e](https://github.com/JonathanBerhe/gvdb/commit/dac227e9a067dbc061b14897ec4e3ce8adbb23a9))


### Bug Fixes

* **ci:** stop demoting feat commits to patch bumps in pre-major ([30a45ba](https://github.com/JonathanBerhe/gvdb/commit/30a45ba607030c55780d61a8c2a2495195c87a22))

## [0.1.8](https://github.com/JonathanBerhe/gvdb/compare/v0.1.7...v0.1.8) (2026-04-04)


### Features

* **index:** add TurboQuant data-oblivious vector quantization ([35eadf1](https://github.com/JonathanBerhe/gvdb/commit/35eadf1227b87d3aa91a5b822acf1b042f3ba699))
* **network:** add ListVectors RPC for web UI vector browsing ([a590e43](https://github.com/JonathanBerhe/gvdb/commit/a590e4374ee44b5a712c53a93534e1d83e7ff140))
* **ui:** add build-ui/run-ui Makefile targets and update README ([61d9085](https://github.com/JonathanBerhe/gvdb/commit/61d9085be7ab7af5f9fab134d1b1512e95c55fd1))
* **ui:** add collection detail page with vector browser and search playground ([b8c936a](https://github.com/JonathanBerhe/gvdb/commit/b8c936a042ee242920ee926d4800ecd447c49670))
* **ui:** add Docker image and Helm sidecar deployment for web UI ([06a5aa1](https://github.com/JonathanBerhe/gvdb/commit/06a5aa175cede9de59f9e9a350aa7a9f06e43b83))
* **ui:** add Go REST gateway with collection/search/hybrid endpoints ([9c91305](https://github.com/JonathanBerhe/gvdb/commit/9c91305102b45035d9b0e2232772eb0eb92511a3))
* **ui:** add metrics dashboard with Recharts charts and Prometheus proxy ([272f61d](https://github.com/JonathanBerhe/gvdb/commit/272f61dc6bafdfaa80074ae88a3e91f9c4491a4f))
* **ui:** add web management UI with React SPA and Go REST gateway ([5fee951](https://github.com/JonathanBerhe/gvdb/commit/5fee9510729778564eba105682476aea534ddc87))
* **ui:** add web management UI with React SPA and Go REST gateway ([ce692b5](https://github.com/JonathanBerhe/gvdb/commit/ce692b5fff934bec3316cefbc38f1fd358e8e245))
* **ui:** redesign with pitch black dark mode, minimal palette, and theme toggle ([daaf739](https://github.com/JonathanBerhe/gvdb/commit/daaf739362d50eebedfcb248b317799df228afb2))


### Bug Fixes

* **index:** add missing #include &lt;mutex&gt; for GCC compatibility in TurboQuant ([577fe1a](https://github.com/JonathanBerhe/gvdb/commit/577fe1a60d891fdf91dfd5b84083aa492bb2dfc8))
* **ui:** handle null labels in metrics, reduce binary size with strip flags ([06870a0](https://github.com/JonathanBerhe/gvdb/commit/06870a048211c87dbc6f056b6ed231f7cde02d29))

## [0.1.7](https://github.com/JonathanBerhe/gvdb/compare/v0.1.6...v0.1.7) (2026-04-03)


### Features

* **search:** add hybrid search with BM25 + vector RRF fusion ([6a3d5ef](https://github.com/JonathanBerhe/gvdb/commit/6a3d5efa66f002d888390fcf5e86949dfa0227fe))
* **search:** add hybrid search with BM25 + vector RRF fusion ([e575f59](https://github.com/JonathanBerhe/gvdb/commit/e575f59955bc65fbb22fc3710d7584baad695083))
* **search:** add hybrid search with BM25 + vector RRF fusion ([32685d7](https://github.com/JonathanBerhe/gvdb/commit/32685d7060fe17c19a2788a0557657a2b95505ec))
* **search:** add hybrid search with BM25 + vector RRF fusion ([32e4f2a](https://github.com/JonathanBerhe/gvdb/commit/32e4f2ac63f981876e66c003a83f47411de5f75b))

## [0.1.6](https://github.com/JonathanBerhe/gvdb/compare/v0.1.5...v0.1.6) (2026-04-03)


### Features

* **compute:** add LRU query result cache ([a664216](https://github.com/JonathanBerhe/gvdb/commit/a664216a981a6e5dc3310f3f037907a8850229d0))
* **compute:** add LRU query result cache with 377x speedup on cache hits ([77b28d6](https://github.com/JonathanBerhe/gvdb/commit/77b28d6c943569ce3557eccf07f24b97bec5e990))

## [0.1.5](https://github.com/JonathanBerhe/gvdb/compare/v0.1.4...v0.1.5) (2026-04-02)


### Features

* **monitoring:** instrument all operations and add RED method Grafana dashboard ([9a486f6](https://github.com/JonathanBerhe/gvdb/commit/9a486f6144c893120ba167e23ba0f11cfb64b12e))
* **monitoring:** instrument all operations and add RED method Grafana dashboard ([91b76ff](https://github.com/JonathanBerhe/gvdb/commit/91b76ffbbfdb784fb868c93cc14aa0684462ae27))

## [0.1.4](https://github.com/JonathanBerhe/gvdb/compare/v0.1.3...v0.1.4) (2026-04-02)


### Bug Fixes

* **ci:** lowercase OCI registry names for ghcr.io ([c8c4fdd](https://github.com/JonathanBerhe/gvdb/commit/c8c4fddd4ceb15fb26b51a2ae54940f347aa96dc))
* **ci:** lowercase OCI registry names for ghcr.io compatibility ([c9b07a5](https://github.com/JonathanBerhe/gvdb/commit/c9b07a50674a56d797a7f63087f5cc852dcc4f2d))
* **ci:** skip build-and-test on push for non-code changes ([2e08896](https://github.com/JonathanBerhe/gvdb/commit/2e0889621f39f36fc6712398f74711f25e920084))

## [0.1.3](https://github.com/JonathanBerhe/gvdb/compare/v0.1.2...v0.1.3) (2026-04-02)


### Features

* **network:** add streaming inserts via client-streaming gRPC ([f973440](https://github.com/JonathanBerhe/gvdb/commit/f9734405b59dca32860dcf3005270d112bda0d93))
* **network:** add streaming inserts via client-streaming gRPC ([7ad70d3](https://github.com/JonathanBerhe/gvdb/commit/7ad70d30faee3b198b5b4cef869abc1577db286f))

## [0.1.2](https://github.com/JonathanBerhe/gvdb/compare/v0.1.1...v0.1.2) (2026-04-02)


### Features

* **ci:** add PyPI publish workflow for Python SDK ([1c4e47e](https://github.com/JonathanBerhe/gvdb/commit/1c4e47e49a697698c49737f486ffe005acc2d187))
* **sdk:** add api_key authentication to Python client ([97f2755](https://github.com/JonathanBerhe/gvdb/commit/97f27556c71cddecb5c0f19078ff0aa309bb16a0))
* **sdk:** add Python client for GVDB ([ed22e32](https://github.com/JonathanBerhe/gvdb/commit/ed22e3222490603924d173af18a27d57c48aeb21))
* **sdk:** add Python client for GVDB ([79fda95](https://github.com/JonathanBerhe/gvdb/commit/79fda956c8ec6e02793f873350228ba58e9a8725))

## [0.1.1](https://github.com/JonathanBerhe/gvdb/compare/v0.1.0...v0.1.1) (2026-04-02)


### Features

* add metadata filtering with SQL-like query support ([e1c06ce](https://github.com/JonathanBerhe/gvdb/commit/e1c06ce834c51a21b5e586f495fa18f6cf04e80f))
* add production hardening (metrics, config, monitoring) ([73502cd](https://github.com/JonathanBerhe/gvdb/commit/73502cd89bf6505b30695a5d1925f6d3969a5347))
* **ci:** add semver release pipeline with release-please ([52711ba](https://github.com/JonathanBerhe/gvdb/commit/52711baf355f955dfb869918acadf5421a4db0c9))
* **cluster:** add NodeRegistry with heartbeat protocol and multi-node binaries ([9440004](https://github.com/JonathanBerhe/gvdb/commit/94400048394c79b5d2273fba18a55aa8904337da))
* complete single-node GVDB with high-dimensional vector support ([a46a624](https://github.com/JonathanBerhe/gvdb/commit/a46a6240feb229904846d41a79f62c6742cd52ef))
* **consensus, storage:** add state maschine support ([3a0b1e7](https://github.com/JonathanBerhe/gvdb/commit/3a0b1e76e1798349f1fdf440aa3171b384006b54))
* **deploy:** add Docker, Kubernetes, and cloud-native infrastructure ([2e24176](https://github.com/JonathanBerhe/gvdb/commit/2e2417664ec1577f8fcc30801ca4fcdb408b76a8))
* **deploy:** add Helm chart for GVDB deployment ([7bcdbc8](https://github.com/JonathanBerhe/gvdb/commit/7bcdbc853d0edec144b49a64f154a457d20a523d))
* **deploy:** add Helm chart OCI release pipeline ([2bfae74](https://github.com/JonathanBerhe/gvdb/commit/2bfae7422c9b34503dae0577214cfab3026ba2e1))
* distributed mode with multi-shard, persistence, TLS, and auth ([456246e](https://github.com/JonathanBerhe/gvdb/commit/456246eecc4c78b99c10156331b2ba6bed70a2de))
* **index:** IVF_SQ ([7336977](https://github.com/JonathanBerhe/gvdb/commit/733697714cb18a225e199699d94272bf1ee1fb20))
* **storage:** implement type-safe metadata serialization and segment replication ([d854ea1](https://github.com/JonathanBerhe/gvdb/commit/d854ea1767aecdbf97787435b802e495cbff11f1))


### Bug Fixes

* add missing #include &lt;cstring&gt; for Linux GCC compatibility ([7f9dea2](https://github.com/JonathanBerhe/gvdb/commit/7f9dea2fb050803ab4c561dadf8628a6e213494e))
* add missing #include &lt;mutex&gt; for Linux GCC compatibility ([bbd85d5](https://github.com/JonathanBerhe/gvdb/commit/bbd85d5b43e2c0e194b28ff6bc67bfcb6029ad8e))
* also suppress -Wstringop-overflow GCC 13 false positive in spdlog/fmt ([9ab90ca](https://github.com/JonathanBerhe/gvdb/commit/9ab90cac2e96cc23d0aabdca8c8078caeec463e3))
* suppress GCC 13 false positive -Warray-bounds in spdlog/fmt ([bc8ee82](https://github.com/JonathanBerhe/gvdb/commit/bc8ee820542f64b2053fef77f79fffd908bf944d))
* **utils:** wrap spdlog format strings with fmt::runtime() for GCC compatibility ([7559e26](https://github.com/JonathanBerhe/gvdb/commit/7559e26568188b85f202dec4a98581edd089a4bf))
