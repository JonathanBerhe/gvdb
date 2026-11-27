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
refactor(service): simplify gRPC service initialization
```

Subject-only commits: no body, no `Co-Authored-By` footer.

See the [changelog](changelog.md) for the version-bump rules.

## Pull requests

- Keep PRs focused. One topic per PR.
- CI builds and tests the `ci` preset (`cmake --preset ci`, `cmake --build --preset ci`, `ctest --preset ci`) and must pass.
- If you add a module or change the modules that a module uses, update the module order under Coding conventions.
- Add tests for new features and bug fixes (TDD welcome).
- Tests must be isolated, because CI runs the test binaries in parallel. Use the helpers in `test/support` for temp folders, gRPC servers on free ports and bounded waits. Do not use a fixed path, a fixed port or a fixed sleep.
- After a change to a file in `proto/`, run `make proto` and commit the regenerated stubs. CI fails when a stub differs from `proto/`.

## Coding conventions

The C++ code follows these conventions:

- C++23, set in one place (`GVDB_CXX_STANDARD` in `cmake/GvdbCompilerSettings.cmake`). The fetched dependencies build at the same standard.
- RAII + smart pointers (no raw `new`/`delete`)
- `absl::Status` / `absl::StatusOr<T>` for fallible operations
- `-Wall -Wextra -Wpedantic` on every GVDB target, and warnings are errors in every preset
- Each module is a folder in `src/`. Its headers sit next to its sources, and code includes a header by its path below `src/`, for example `#include "storage/segment/segment.h"`. Headers use `#pragma once`.
- A module includes and links only the modules before it in this order: `core`, `common`, `auth`, `index`, `storage`, `compute`, `meta`, `rpc`, `consensus`, `cluster`, `service`, `config`, `server`. The programs in `src/main` can use all of them. `make check-layers` (`tools/check_layers.py`) finds a forbidden include, and CI runs it.
- A lower module does not read the YAML configuration. It defines its own configuration struct (for example `storage::ObjectStoreConfig`), and `config` fills it from the YAML file.
- When a header of a module includes a header of another module, the `CMakeLists.txt` of the module lists that module in `PUBLIC_DEPS`. A module that only its sources include goes in `PRIVATE_DEPS`.

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
