# Dashboard SQL connections through the SPIFFE Workload API

The TiDB Dashboard embedded in PD supports SQL TLS with configured CA,
certificate, and key files. It can also obtain its MySQL client identity and
trust bundles directly from a SPIFFE Workload API. Dashboard continues to use
the entered SQL username, password, and database privileges. The SVID
authenticates the PD workload at the TLS layer; SQL account certificate
requirements still apply.

Configure each PD member that can host Dashboard:

```toml
[dashboard]
tidb-spiffe-workload-api-addr = "unix:///spire-agent-socket/spire-agent.sock"
tidb-spiffe-workload-api-timeout = "30s"
tidb-spiffe-client-id-pattern = "spiffe://example.org/ns/database/sa/pd/*"
tidb-spiffe-server-id-pattern = "spiffe://example.org/ns/database/sa/tidb/*"
```

Replace these example identities with the registered PD and TiDB workload
identities, including their actual service accounts. Make the Workload API
Unix socket available to each PD process, using a socket volume mount for a
containerized deployment. The endpoint must issue a unique SVID matching the
configured client pattern and provide the TiDB server's trust bundle. These
settings cannot be combined with Dashboard's certificate-file TLS settings.

Patterns accept exact SPIFFE IDs or whole `*` path segments. Each wildcard
matches one nonempty segment; the trust domain and path depth must match.
Restrict the trust domain and identity path to the intended workloads. URI
identity verification supports SVIDs without DNS or IP SANs.

When Dashboard starts on a PD member, it waits for initial credentials up to
the configured timeout (30 seconds by default). Missing or invalid credentials
keep Dashboard stopped, and the manager retries. PD's other services continue
running. There is no plaintext or certificate-file fallback in SPIFFE mode.

The source watches SVID and trust-bundle updates. New SQL connections use the
current credentials and verify the server identity over TLS 1.3; established
SQL sessions continue normally. The source closes after Dashboard stops and is
recreated if Dashboard moves back to that PD member. This setting affects SQL
connections only; PD cluster traffic and browser access retain their existing
configuration.

Before enabling `require_secure_transport`, verify Dashboard login and a
SQL-dependent page against TiDB with that variable enabled, including a fresh
connection after certificate and bundle rotation. An unauthorized server
identity must fail. Configure the socket and identities on every PD member
that can host Dashboard before enabling secure transport.
