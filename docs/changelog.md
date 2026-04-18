# Changelog

For the canonical, full-history changelog, see [`CHANGELOG.md`](https://github.com/JonathanBerhe/gvdb/blob/main/CHANGELOG.md) on GitHub. It's maintained automatically by [release-please](https://github.com/googleapis/release-please) from conventional commit messages on `main`.

## Release cadence

- Every merge to `main` with a `feat:` or `fix:` prefix bumps the version.
- `release-please` opens a pull request with the proposed bump + changelog entries.
- Merging the release PR creates a git tag and publishes:
  - Docker image to `ghcr.io/jonathanberhe/gvdb`
  - Helm chart to `oci://ghcr.io/jonathanberhe/charts/gvdb`
  - Python SDK to PyPI
  - Java connectors to GitHub Packages

## Versioning

Semver, governed by conventional commits:

| Commit prefix | Version bump |
|---------------|--------------|
| `feat:` | minor (0.1.0 → 0.2.0) |
| `fix:` | patch (0.1.0 → 0.1.1) |
| `feat!:` / `fix!:` (bang) | major (0.1.0 → 1.0.0) |
| `chore:` / `refactor:` / `docs:` / `style:` / `test:` | no bump |

## See also

- [Contributing](contributing.md) — conventional-commit workflow
