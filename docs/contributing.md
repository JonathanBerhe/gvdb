# Contributing

Thanks for your interest in GVDB. Contributions are welcome, from bug reports to typo fixes to full feature PRs.

## Repository

[github.com/JonathanBerhe/gvdb](https://github.com/JonathanBerhe/gvdb).

## Development setup

```bash
git clone https://github.com/JonathanBerhe/gvdb.git
cd gvdb
make build          # Debug build with tests (preset dev)
make test           # C++ unit + integration tests
```

The `.clangd` file in the repository root points clangd at `build/dev/compile_commands.json`, so run `make build` once before you open the code in an editor that uses clangd.

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

**Conventional commits**, parsed by release-please to compute version bumps.

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

Subject-only commits: no body, no `Co-Authored-By` footer.

See the [changelog](changelog.md) for the version-bump rules.

## Pull requests

- Keep PRs focused. One topic per PR.
- CI builds and tests the `ci` preset (`cmake --preset ci`, `cmake --build --preset ci`, `ctest --preset ci`) and must pass.
- Update the relevant [module CLAUDE.md](https://github.com/JonathanBerhe/gvdb/blob/main/CLAUDE.md) if you change architecture.
- Add tests for new features and bug fixes (TDD welcome).
- Tests must be isolated, because CI runs the test binaries in parallel. Use the helpers in `test/support` for temp folders, gRPC servers on free ports and bounded waits. Do not use a fixed path, a fixed port or a fixed sleep.
- After a change to a file in `proto/`, run `make proto` and commit the regenerated stubs. CI fails when a stub differs from `proto/`.

## Coding conventions

See the top-level [`CLAUDE.md`](https://github.com/JonathanBerhe/gvdb/blob/main/CLAUDE.md) in the repo:

- C++20, set in one place (`GVDB_CXX_STANDARD` in `cmake/GvdbCompilerSettings.cmake`)
- RAII + smart pointers (no raw `new`/`delete`)
- `absl::Status` / `absl::StatusOr<T>` for fallible operations
- `-Wall -Wextra -Wpedantic` on every GVDB target, and warnings are errors in every preset
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
