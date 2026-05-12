#!/bin/bash

# See https://gist.github.com/howardjohn/c0f5d0bc293ef7d7fada533a2c9ffaf4
# Usage: go test -exec=true -toolexec=go-compile-without-link -vet=off ./...
# Preferably as an alias like `alias go-test-compile='go test -exec=true -toolexec=go-compile-without-link -vet=off'`
# This will compile all tests, but not link them (which is the least cacheable part)

if [[ "${2}" == "-V=full" ]]; then
  "$@"
  exit 0
fi
case "$(basename ${1})" in
  link)
    # Output a dummy file
    touch "${3}"
    ;;
  # We could skip vet as well, but it can be done with -vet=off if desired
  *)
    "$@"
esac
