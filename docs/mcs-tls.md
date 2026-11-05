# Client certificate authentication for MCS

The TSO, Scheduling, Resource Manager and Router services share a TLS listener
for their HTTP and gRPC endpoints. To require client certificates, configure
each service with:

```toml
[security]
cacert-path = "/path/to/ca.pem"
cert-path = "/path/to/server.pem"
key-path = "/path/to/server-key.pem"
client-cert-auth = true
```

With `client-cert-auth = true`, the listener rejects clients without a
certificate or with a certificate that is not signed by a configured CA.
All three file paths are required; incomplete configurations fail at listener
initialization. Set `listen-addr` and `advertise-listen-addr` to HTTPS URLs and
restart the service to apply the configuration.

To also restrict certificate common names, set `cert-allowed-cn` in the same
section. Include every component that connects to the service, including
internal callers and monitoring clients.
This list also applies to outbound TLS connections, so include the certificate
CNs of PD and any other servers the service contacts.

The default is `client-cert-auth = false`, which preserves the existing client
certificate policy. Without a CN allowlist, enabling TLS alone does not require
a client certificate on these MCS listeners. An existing `cert-allowed-cn` list
continues to require a matching client certificate. PD's embedded etcd client
and peer listeners continue to use their existing certificate authentication
policy.

For a rolling upgrade, first upgrade the MCS servers with the option omitted or
disabled. Make sure every caller, including monitoring and health probes, has
a trusted client certificate before enabling the option and restarting the
services. Older binaries do not enforce this new option, so complete the server
upgrade before relying on it for authentication.
