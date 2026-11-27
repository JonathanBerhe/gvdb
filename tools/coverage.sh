#!/usr/bin/env bash
# Runs every test of a coverage build and reports line coverage of GVDB's own
# sources (src/), per file and per module.
#
# Usage: tools/coverage.sh <build-dir>
#
# The build directory must come from the coverage preset (GVDB_COVERAGE=ON).
# Results go to <build-dir>/coverage:
#   report.txt    llvm-cov report, one line per file
#   modules.txt   line coverage per module (src/<module>)
#   summary.json  llvm-cov export -summary-only, for comparing two runs
#
# The script exits with the status of the test run, so a failing test still
# produces a report but fails the command.
set -euo pipefail

build_dir=${1:?usage: tools/coverage.sh <build-dir>}
repo_root=$(cd "$(dirname "$0")/.." && pwd)
build_dir=$(cd "$build_dir" && pwd)
out_dir="$build_dir/coverage"

if [[ "$(uname)" == Darwin ]]; then
  llvm_profdata=${LLVM_PROFDATA:-$(xcrun --find llvm-profdata)}
  llvm_cov=${LLVM_COV:-$(xcrun --find llvm-cov)}
else
  llvm_profdata=${LLVM_PROFDATA:-llvm-profdata}
  llvm_cov=${LLVM_COV:-llvm-cov}
fi

rm -rf "$out_dir"
mkdir -p "$out_dir/profiles"

# Each test process writes its own raw profile (%p is the process id, %m
# identifies the binary).
set +e
LLVM_PROFILE_FILE="$out_dir/profiles/%p-%m.profraw" \
  ctest --test-dir "$build_dir" --output-on-failure \
  --parallel "$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
ctest_status=$?
set -e

"$llvm_profdata" merge -sparse "$out_dir"/profiles/*.profraw -o "$out_dir/gvdb.profdata"

# The test executables, as CTest runs them.
list_tests_cmd='import json, sys
tests = json.load(sys.stdin)["tests"]
for path in sorted({t["command"][0] for t in tests if t.get("command")}):
    print(path)'
objects=()
while IFS= read -r exe; do
  if [[ ${#objects[@]} -eq 0 ]]; then
    objects+=("$exe")
  else
    objects+=("-object" "$exe")
  fi
done < <(ctest --test-dir "$build_dir" --show-only=json-v1 | python3 -c "$list_tests_cmd")

if [[ ${#objects[@]} -eq 0 ]]; then
  echo "coverage.sh: no test executables found in $build_dir" >&2
  exit 1
fi

cov_args=("${objects[@]}" "-instr-profile=$out_dir/gvdb.profdata" "$repo_root/src")

"$llvm_cov" report "${cov_args[@]}" > "$out_dir/report.txt"
"$llvm_cov" export -summary-only "${cov_args[@]}" > "$out_dir/summary.json"

modules_cmd='import collections, json, os, sys
repo_root = sys.argv[1]
totals = collections.defaultdict(lambda: [0, 0])
for entry in json.load(sys.stdin)["data"][0]["files"]:
    rel = os.path.relpath(entry["filename"], repo_root).split(os.sep)
    module = "/".join(rel[:2]) if len(rel) > 2 else rel[0]
    lines = entry["summary"]["lines"]
    totals[module][0] += lines["covered"]
    totals[module][1] += lines["count"]
totals["TOTAL"] = [sum(c for c, _ in totals.values()), sum(n for _, n in totals.values())]
row = "{:<24} {:>8} {:>8} {:>8}"
print(row.format("module", "covered", "lines", "percent"))
for module in sorted(m for m in totals if m != "TOTAL") + ["TOTAL"]:
    covered, count = totals[module]
    percent = 100.0 * covered / count if count else 100.0
    print(row.format(module, covered, count, "{:.2f}%".format(percent)))'
python3 -c "$modules_cmd" "$repo_root" < "$out_dir/summary.json" > "$out_dir/modules.txt"

cat "$out_dir/modules.txt"
echo "Per-file report: $out_dir/report.txt"
exit "$ctest_status"
