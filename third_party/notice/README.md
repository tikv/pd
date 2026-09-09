# PD third-party notice

`ThirdPartyNotices.txt` is the source-level Go notice for the profiles declared
in [`scopes.json`](scopes.json). The profile list includes the root module, the
nested `tools` and `client` modules, and the supported `SWAGGER=1` build.
Tests are intentionally excluded.

The pinned image contains Go 1.25.12 and `go-licenses` 1.6.0. Run generation
from the repository root:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace \
  us-docker.pkg.dev/pingcap-testing-account/internal/test/notice-generator@sha256:60fd803ae1ff5d74cc158cc9190d27c2b0bef4b892bcbe8d9b7de0c1ae2252d5 \
  python3 third_party/notice/generate.py
```

To verify committed outputs without changing them, append `--check`.

`components.json` records the generated component-to-profile mapping,
checksums, license evidence digests, and override provenance. The notice file
contains the retained license texts and upstream notices. Do not add an
unknown license or broaden an override without an exact-version evidence file
and review.

This is a one-time, auditable source-code notice update. It is not generated
as part of the normal build or CI workflow.
