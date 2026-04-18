# Contributing

Thanks for your interest in GVDB. Contributions are welcome — from bug reports to typo fixes to full feature PRs.

## Repository

[github.com/JonathanBerhe/gvdb](https://github.com/JonathanBerhe/gvdb).

## Development setup

```bash
git clone https://github.com/JonathanBerhe/gvdb.git
cd gvdb
make build          # Debug build
make test           # C++ unit + integration tests (37 suites)
```

Python SDK:

```bash
cd clients/python
uv sync
make test-sdk       # from repo root
make lint-sdk
```

Java connectors:

```bash
cd connectors
./gradlew build
```

See [installation](getting-started/installation.md) for prerequisites.

## Branch workflow

- `main` is protected. All changes go through pull requests.
- Feature branches: `feature/<name>`
- Fixes: `fix/<name>`
- Chores (docs, CI, config): `chore/<name>`

## Commit messages

**Conventional commits** — parsed by release-please to compute version bumps.

```
<type>(<scope>): <subject>
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`.

Examples:

```
feat(cluster): add NodeRegistry with heartbeat protocol
fix(storage): correct segment state transitions
refactor(network): simplify gRPC service initialization
```

Subject-only commits — no body, no `Co-Authored-By` footer.

See the [changelog](changelog.md) for the version-bump rules.

## Pull requests

- Keep PRs focused. One topic per PR.
- CI runs `make build && make test` and must pass.
- Update the relevant [module CLAUDE.md](https://github.com/JonathanBerhe/gvdb/blob/main/CLAUDE.md) if you change architecture.
- Add tests for new features and bug fixes (TDD welcome).

## Coding conventions

See the top-level [`CLAUDE.md`](https://github.com/JonathanBerhe/gvdb/blob/main/CLAUDE.md) in the repo:

- C++17 minimum, C++20 preferred
- RAII + smart pointers (no raw `new`/`delete`)
- `absl::Status` / `absl::StatusOr<T>` for fallible operations
- Warnings as errors (`-Wall -Wextra -Werror`)
- Module dependency rules (see [`CLAUDE.md`](https://github.com/JonathanBerhe/gvdb/blob/main/CLAUDE.md#module-architecture-and-dependencies))

Python SDK follows `ruff` defaults. Run `make lint-sdk`.

## Documentation

This documentation site lives in `docs/` and is built with [Zensical](https://zensical.org). To preview locally:

```bash
make docs-serve
```

And to verify the strict build before committing:

```bash
make docs-build
```

Changes merged to `main` that touch `docs/`, `mkdocs.yml`, or the primary READMEs are auto-deployed to GitHub Pages.

## Reporting bugs

Open an issue on GitHub with:

- Version (`gvdb-single-node --version`)
- Deployment mode (Docker / Helm / source)
- Reproduction steps
- Logs with `--log-level debug` if possible

## Feature requests

Open an issue or discussion.

## See also

- [Changelog](changelog.md)
