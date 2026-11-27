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
- Before you commit C++ changes, run `make format`. CI runs `make check-format` and fails when a file in `src/` or `test/` does not match `.clang-format`.
- `.git-blame-ignore-revs` lists the commits that only reformat code. GitHub skips them in its blame view. For a local `git blame`, run `git config blame.ignoreRevsFile .git-blame-ignore-revs` once.
- CI also runs `make tidy` with clang-tidy 22 on the `ci` build and fails on any hit. To run it locally, build a preset first, then run `make tidy PRESET=dev`. A local clang-tidy of another LLVM version can report a few hits that version 22 does not.

## Coding conventions

The C++ code follows these conventions:

- C++23, set in one place (`GVDB_CXX_STANDARD` in `cmake/GvdbCompilerSettings.cmake`). The fetched dependencies build at the same standard.
- The layout follows `.clang-format`: Google style with a limit of 100 columns. `make format` applies it with clang-format 22.1.8 from PyPI, which `uvx` downloads on first use.
- `.clang-tidy` enables the `bugprone`, `performance` and `modernize` checks and the naming check, minus the checks that it lists with a reason. `test/.clang-tidy` turns off two more checks for the tests. A `NOLINT` comment names the check and gives the reason, for example `// NOLINT(readability-identifier-naming): NuRaft name.`
- RAII + smart pointers (no raw `new`/`delete`)
- `absl::Status` and `absl::StatusOr<T>` for fallible operations, used directly. `core/status.h` includes both and adds `RETURN_IF_ERROR` and `ASSIGN_OR_RETURN`, which return an error to the caller.
- A class whose construction can fail has a private constructor and a static `Create()` that returns `absl::StatusOr` (for example `RaftNode::Create`). Constructors do not throw. Two exceptions stay: value types such as `core::Vector` throw on a precondition violation like a standard container, and the NuRaft log store throws from its overrides, because that interface has no other way to report an error.
- A class gets its collaborators in the constructor, not through setters. Required collaborators are constructor parameters. Optional ones go in an options struct that is the last parameter and defaults to `{}` (for example `service::VectorDBServiceOptions` and `cluster::CoordinatorOptions`), and callers fill it with designated initializers. Every member of an options struct has a default member initializer (`= nullptr`, `{}`), because Clang warns about a member that a designated initializer leaves out when it has none. A callback for an event, such as `ISegmentStore::SetSealCallback`, can stay a setter.
- Functions and methods have UpperCamel names (`ToProto`, `LoadFromFile`). Two kinds keep lower case: accessors that only return a member (`dimension()`, `is_negated()`), and overrides of a third-party interface, such as the NuRaft log store and state machine.
- Every source and test file starts with the two-line license header (`// Copyright 2026 jonathanberhe` and `// Licensed under the Apache License, Version 2.0`).
- `-Wall -Wextra -Wpedantic` on every GVDB target, and warnings are errors in every preset
- Each module is a folder in `src/`. Its headers sit next to its sources, and code includes a header by its path below `src/`, for example `#include "storage/segment/segment.h"`. Headers use `#pragma once`.
- A module includes and links only the modules before it in this order: `core`, `common`, `auth`, `index`, `storage`, `compute`, `meta`, `rpc`, `consensus`, `cluster`, `service`, `config`, `server`. The programs in `src/main` can use all of them. `make check-layers` (`tools/check_layers.py`) finds a forbidden include, and CI runs it.
- A program in `src/main` declares its flags with `server::FlagSet` and builds its parts with the builders in `src/server`. Code that two programs share goes in `src/server`, not in a second main.
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
