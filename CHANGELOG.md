# Changelog

## [0.1.5](https://github.com/JonathanBerhe/gvdb/compare/v0.1.4...v0.1.5) (2026-04-02)


### Bug Fixes

* **ci:** chain Docker/Helm/PyPI releases off release-please ([72a24cb](https://github.com/JonathanBerhe/gvdb/commit/72a24cb6b6ea9ffe58ba7492eabe33c6d98927ae))
* **ci:** chain Docker/Helm/PyPI releases off release-please output ([e0d9765](https://github.com/JonathanBerhe/gvdb/commit/e0d9765f5f20b35c58a625c694383b7fb2b7ce30))

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
