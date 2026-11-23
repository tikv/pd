#!/usr/bin/env python3
# Copyright 2026 TiKV Project Authors.
# SPDX-License-Identifier: Apache-2.0
"""Prepare an in-place Starter v1 upgrade before the first v2 PD starts.

Run after freezing v1 GC and confirming that the old default --path for
keyspaces without a meta-service group points to the destination PD etcd.
Keep each legacy group readable at its original backend until its keyspaces
have been verified, including tenants that may later be re-enabled.
Record the returned cluster IDs and revision and check them on rollout retries.
"""

import argparse
import base64
import json
import os
import subprocess
import sys


def prepare(endpoints, cluster_id):
    identity_key = "/pd/cluster_id"
    marker_key = f"/pd/{cluster_id}/gc/starter_v2_migration"
    identity = cluster_id.to_bytes(8, "big")
    # etcdctl txn parses quoted comparison operands using Go string escaping.
    quoted_identity = "".join(f"\\x{byte:02x}" for byte in identity)
    transaction = (
        f'value("{identity_key}") = "{quoted_identity}"\n'
        f'version("{marker_key}") = "0"\n\n'
        f"put {marker_key} 1\nget {marker_key}\n\n"
        f"get {identity_key}\nget {marker_key}\n\n"
    )
    env = dict(os.environ, ETCDCTL_API="3", ETCDCTL_ENDPOINTS=endpoints, ETCDCTL_WRITE_OUT="json")
    result = subprocess.run(
        ["etcdctl", "txn"], input=transaction, env=env, stdout=subprocess.PIPE, text=True, check=True
    )
    txn = json.loads(result.stdout)
    created = txn.get("succeeded", False)
    first, last = txn["responses"]
    if not created:
        rows = first["Response"]["response_range"].get("kvs", [])
        if len(rows) != 1 or base64.b64decode(rows[0]["value"], validate=True) != identity:
            raise ValueError("PD cluster ID mismatch or /pd/cluster_id missing")

    rows = last["Response"]["response_range"].get("kvs", [])
    if len(rows) != 1:
        raise ValueError("expected exactly one migration marker")
    marker = rows[0]
    if (
        base64.b64decode(marker["key"], validate=True) != marker_key.encode()
        or base64.b64decode(marker["value"], validate=True) != b"1"
    ):
        raise ValueError("unexpected migration marker key or format version")
    revision = int(marker["create_revision"])
    if (
        revision <= 0
        or int(marker["mod_revision"]) != revision
        or int(marker["version"]) != 1
        or int(marker.get("lease", 0)) != 0
    ):
        raise ValueError("expected an immutable migration marker without a lease")
    return {
        "pd_cluster_id": str(cluster_id),
        "etcd_cluster_id": str(txn["header"]["cluster_id"]),
        "revision": str(revision),
        "created": created,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pd-cluster-id", required=True, type=int,
                        help="expected logical PD cluster ID from the rollout inventory")
    parser.add_argument("--endpoints", required=True,
                        help="comma-separated client URLs of the target PD etcd")
    args = parser.parse_args()
    if not 0 < args.pd_cluster_id < 2**64:
        parser.error("--pd-cluster-id must be a positive uint64")
    if not all(item.strip() for item in args.endpoints.split(",")):
        parser.error("--endpoints contains an empty endpoint")
    try:
        print(json.dumps(prepare(args.endpoints, args.pd_cluster_id)))
    except (OSError, subprocess.CalledProcessError, ValueError, KeyError, TypeError) as err:
        sys.exit(f"GC migration preparation failed: {err}")


if __name__ == "__main__":
    main()
