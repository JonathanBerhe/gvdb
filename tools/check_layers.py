#!/usr/bin/env python3
# Copyright 2026 jonathanberhe
# Licensed under the Apache License, Version 2.0
"""Fails when a module in src/ includes a module that comes after it.

Every GVDB module is a folder in src/, and all modules share one include root,
so the build cannot stop a forbidden include. This script does. A module can
include its own headers and the headers of the modules before it in LAYERS.
The programs in src/main can include every module.

Usage: python3 tools/check_layers.py
"""

import pathlib
import re
import sys

# Lower modules first. Keep docs/contributing.md in step with this list.
LAYERS = [
    "core",
    "common",
    "auth",
    "index",
    "storage",
    "compute",
    "meta",
    "rpc",
    "consensus",
    "cluster",
    "service",
    "config",
    "server",
]
PROGRAMS = "main"
SOURCE_SUFFIXES = {".h", ".cpp", ".mm", ".cu", ".cuh"}
INCLUDE = re.compile(r'^\s*#\s*include\s+"([^"/]+)/')


def main() -> int:
    repo = pathlib.Path(__file__).resolve().parent.parent
    src = repo / "src"
    modules = set(LAYERS) | {PROGRAMS}
    problems = []

    for folder in sorted(p for p in src.iterdir() if p.is_dir()):
        if folder.name not in modules:
            problems.append(f"src/{folder.name}: the folder is not in LAYERS in tools/check_layers.py")
            continue
        if folder.name == PROGRAMS:
            allowed = set(LAYERS)
        else:
            allowed = set(LAYERS[: LAYERS.index(folder.name) + 1])
        for path in sorted(folder.rglob("*")):
            if path.suffix not in SOURCE_SUFFIXES:
                continue
            for number, line in enumerate(path.read_text().splitlines(), start=1):
                match = INCLUDE.match(line)
                if match and match.group(1) in modules and match.group(1) not in allowed:
                    problems.append(
                        f"{path.relative_to(repo)}:{number}: {folder.name} includes "
                        f"{match.group(1)}, which comes after it in the layer order")

    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print(f"check_layers: {len(problems)} forbidden include(s). Order: {', '.join(LAYERS)}.",
              file=sys.stderr)
        return 1
    print(f"check_layers: every module includes only the modules before it ({', '.join(LAYERS)}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
