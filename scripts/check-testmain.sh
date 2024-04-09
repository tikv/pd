#!/usr/bin/env bash

# Find all test packages
packages=$(go list ./... | grep -v vendor | xargs go list -f '{{if .TestGoFiles}}{{.ImportPath}}{{end}}')

missing_testmain=0
for pkg in $packages; do
    # Check if the package has TestMain
    if ! grep -q "func TestMain" "$(go list -f '{{.Dir}}' "$pkg")/"*_test.go 2>/dev/null; then
        echo "Package $pkg is missing TestMain"
        missing_testmain=1
    fi
done

# Report results
if [ $missing_testmain -eq 0 ]; then
    echo "✅ All test packages have TestMain"
    exit 0
else
    echo "❌ Some test packages are missing TestMain"
    exit 1
fi
