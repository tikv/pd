# Copilot Instructions (TiKV PD)

## Tests must handle failpoints

This repository uses `github.com/pingcap/failpoint`. Do **not** run `go test` directly unless failpoints are enabled/disabled correctly.

Preferred (auto enable/disable failpoints):

```bash
# Full suite
make test

# Fast subset
make basic-test

# Targeted single package / single test (recommended for quick checks)
make gotest GOTEST_ARGS='./pkg/gctuner -run TestInitGCTuner -count=1'

# Submodules (each has its own go.mod)
make -C client gotest GOTEST_ARGS='./... -run TestFoo -count=1'
make -C tools gotest GOTEST_ARGS='./... -run TestFoo -count=1'
make -C tests/integrations gotest GOTEST_ARGS='./client/... -run TestFoo -count=1'
```

If you must run `go test` manually, always bracket it:

```bash
make failpoint-enable
go test ./pkg/gctuner -run TestInitGCTuner -count=1
make failpoint-disable
```

Never leave failpoints enabled; ensure `git status` is clean before committing/pushing.
