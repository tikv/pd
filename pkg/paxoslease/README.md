# PaxosLease

This package implements a disk-less Paxos-based lease mechanism for PD leader election, as proposed in [PaxosLease: Disk-less Paxos for Leases](https://arxiv.org/abs/1209.4187).

## Usage

1. Initialize `Paxos` with the node ID, list of all nodes, and a `Transport` implementation.
2. Call `Campaign(duration)` to try to acquire the lease.
3. Use `GetLeader()` to check who holds the current lease.
4. Handle incoming messages using `HandleMessage(msg)`.

## Integration with PD

To integrate with PD:
1. Implement the `Transport` interface using PD's existing gRPC or HTTP communication channels.
2. Add a `*Paxos` instance to `server.Member`.
3. In `server.Member.Campaign`, use `paxos.Campaign` instead of `etcd` election if configured.
4. Add HTTP/gRPC handlers to route `paxoslease.Message` to the `Paxos` instance.
