#!/usr/bin/env bash
# Generate a root ThirdPartyNotices.txt for one Go-only source scope.
# Usage: generate.sh REPO_DIR OUTPUT_DIR [GO_PACKAGE ...]
set -euo pipefail

repo=${1:?usage: generate.sh REPO_DIR OUTPUT_DIR [GO_PACKAGE ...]}
out=${2:?usage: generate.sh REPO_DIR OUTPUT_DIR [GO_PACKAGE ...]}
shift 2
packages=("$@")
if ((${#packages[@]} == 0)); then
  packages=(./...)
fi

go_licenses=${GO_LICENSES:-go-licenses}
export GOOS=${GOOS:-linux}
export GOARCH=${GOARCH:-amd64}
export GOTOOLCHAIN=${GOTOOLCHAIN:-local}
# Comma-separated package-path prefixes classified as first-party for this run.
first_party_prefixes=${FIRST_PARTY_PREFIXES:-github.com/pingcap/,github.com/tikv/}
# Optional JSON array of audited manual components. Each override needs module,
# version, license, and license_path relative to its downloaded module directory.
notice_overrides=${NOTICE_OVERRIDES:-}

cd "$repo"
mkdir -p "$out"
find "$out" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +

tool_path=$(command -v "$go_licenses")
go version -m "$tool_path" > "$out/go-licenses.version.txt"
grep -Eq '^[[:space:]]*mod[[:space:]]+github.com/google/go-licenses[[:space:]]+v1\.6\.0([[:space:]]|$)' \
  "$out/go-licenses.version.txt" || {
  echo "go-licenses must be github.com/google/go-licenses v1.6.0" >&2
  exit 1
}

# go-licenses v1.6.0 treats the standard library's intentionally absent Module
# metadata as an error. Derive those ignored package paths from this exact
# target's dependency graph instead of keeping a static allowlist.
args=(--ignore "$(go list -m -f '{{.Path}}')")
while IFS= read -r package; do
  [[ -z "$package" ]] || args+=(--ignore "$package")
done < <(go list -deps -f '{{if not .Module}}{{.ImportPath}}{{end}}' "${packages[@]}" | sort -u)
IFS=',' read -ra prefixes <<< "$first_party_prefixes"
for prefix in "${prefixes[@]}"; do
  [[ -z "$prefix" ]] || args+=(--ignore "$prefix")
done
if [[ -n "$notice_overrides" ]]; then
  while IFS= read -r module; do
    [[ -z "$module" ]] && continue
    args+=(--ignore "$module")
    # go-licenses ignores exact package paths, not an entire module prefix.
    # Exclude every in-scope package owned by an overridden module so the
    # strict collector check cannot fail before the renderer restores it.
    while IFS= read -r package; do
      [[ -z "$package" ]] || args+=(--ignore "$package")
    done < <(go list -deps -f "{{if and .Module (eq .Module.Path \"$module\")}}{{.ImportPath}}{{end}}" "${packages[@]}")
  done < <(python3 -c 'import json, sys; print("\n".join(item["module"] for item in json.load(open(sys.argv[1]))))' "$notice_overrides")
fi

"$go_licenses" report "${args[@]}" "${packages[@]}" \
  > "$out/go-licenses-report.csv" 2> "$out/go-licenses-report.stderr"
"$go_licenses" check "${args[@]}" "${packages[@]}" \
  --disallowed_types=forbidden,unknown \
  > "$out/go-licenses-check.log" 2>&1
"$go_licenses" save "${args[@]}" "${packages[@]}" \
  --save_path="$out/LICENSES" --force \
  > "$out/go-licenses-save.log" 2>&1

go version > "$out/go.version.txt"
NOTICE_GENERATOR_IMAGE=${NOTICE_GENERATOR_IMAGE:-} \
FIRST_PARTY_PREFIXES="$first_party_prefixes" \
NOTICE_OVERRIDES="$notice_overrides" \
python3 "$(dirname "$0")/render_scope.py" "$repo" "$out" "${packages[@]}"
