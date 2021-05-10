#!/bin/bash

# Check if there are any packages foget to add `TestingT` when use "github.com/pingcap/check".

res=$(diff <(grep -rl --include=\*_test.go "github.com/pingcap/check" . | xargs -L 1 dirname | sort -u) \
     <(grep -rl --include=\*_test.go -E "^\s*(check\.)?TestingT\(" . | xargs -L 1 dirname | sort -u))

if [ "$res" ]; then
  echo "following packages may be lost TestingT:"
  echo "$res" | awk '{if(NF>1){print $2}}'
  exit 1
fi

# Check if there are duplicated `TestingT` in package.

res=$(grep -r --include=\*_test.go "TestingT(" . | cut -f1 | xargs -L 1 dirname | sort | uniq -cd)

if [ "$res" ]; then
  echo "following packages may have duplicated TestingT:"
  echo "$res"
  exit 1
fi

exit 0
