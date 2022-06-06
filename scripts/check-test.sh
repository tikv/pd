#!/bin/bash

# TODO: remove this script after migrating all tests to the new test framework.

# Check if there are any packages foget to add `TestingT` when use "github.com/pingcap/check".

res=$(diff <(grep -rl --include=\*_test.go "github.com/pingcap/check" . | xargs -L 1 dirname | sort -u) \
     <(grep -rl --include=\*_test.go -E "^\s*(check\.)?TestingT\(" . | xargs -L 1 dirname | sort -u))

if [ "$res" ]; then
  echo "following packages may be lost TestingT:"
  echo "$res" | awk '{if(NF>1){print $2}}'
  exit 1
fi

# Check if there are duplicated `TestingT` in package.

res=$(grep -r --include=\*_test.go "TestingT(t)" . | cut -f1 | xargs -L 1 dirname | sort | uniq -d)

if [ "$res" ]; then
  echo "following packages may have duplicated TestingT:"
  echo "$res"
  exit 1
fi

# Check if there is any inefficient assert function usage in package.

res=$(grep -rn --include=\*_test.go -E "(re|suite|require)\.(True|False)\(reflect\.DeepEqual\(" . | sort -u) \

if [ "$res" ]; then
  echo "following packages use the inefficient assert function: please replace reflect.DeepEqual with require.Equal"
  echo "$res"
  exit 1
fi

res=$(grep -rn --include=\*_test.go -E "(re|suite|require)\.(True|False)\(strings\.Contains\(" . | sort -u)

if [ "$res" ]; then
  echo "following packages use the inefficient assert function: please replace strings.Contains with require.Contains"
  echo "$res"
  exit 1
fi

exit 0
