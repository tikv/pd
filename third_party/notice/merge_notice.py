#!/usr/bin/env python3
"""Merge scoped Go-license evidence into one source ThirdPartyNotices file."""

import hashlib
import json
import re
import sys
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rendered_text(path, encoding="utf-8"):
    lines = []
    for line in path.read_text(encoding=encoding).splitlines():
        line = line.expandtabs(8).rstrip()
        if re.fullmatch(r"={7,}", line):
            line = "-" * len(line)
        lines.append(line)
    return "\n".join(lines).rstrip()


def component_key(component):
    return (
        component["module"],
        component["version"],
        component["license"],
        component["license_evidence_sha256"],
    )


def source_component(component, directory):
    return {"component": component, "directory": directory}


def public_component(component):
    return {
        key: component[key]
        for key in (
            "module",
            "version",
            "go_module_checksum",
            "license",
            "license_evidence_sha256",
            "license_evidence_encoding",
            "raw_upstream_license_urls",
            "override_reason",
        )
        if key in component
    }


def main(config_path, evidence_root, notice_path, components_path):
    config_path = Path(config_path).resolve()
    config = json.loads(config_path.read_text())
    evidence_root = Path(evidence_root).resolve()
    merged = {}
    commits = set()

    for profile in config["profiles"]:
        directory = evidence_root / profile["id"]
        data = json.loads((directory / "components.json").read_text())
        commits.add(data["commit"])
        for component in data["components"]:
            key = component_key(component)
            entry = merged.setdefault(
                key,
                {
                    "component": public_component(component),
                    "source": source_component(component, directory),
                    "profiles": {},
                    "upstream_notices": {},
                },
            )
            entry["profiles"].setdefault(profile["id"], set()).update(
                component["packages_in_scope"]
            )
            for notice in component["upstream_notices"]:
                upstream_path = directory / notice["path"]
                entry["upstream_notices"].setdefault(
                    (notice["sha256"], upstream_path.name), upstream_path
                )

    if len(commits) != 1:
        raise RuntimeError(f"scopes were generated from different commits: {sorted(commits)}")
    commit = commits.pop()

    components = []
    for entry in merged.values():
        component = entry["component"]
        component["profiles"] = [
            {"id": profile, "packages": sorted(packages)}
            for profile, packages in sorted(entry["profiles"].items())
        ]
        component["upstream_notices"] = [
            {"name": name, "sha256": sha}
            for sha, name in sorted(entry["upstream_notices"])
        ]
        components.append(entry)
    components.sort(
        key=lambda entry: (
            entry["component"]["module"],
            entry["component"]["version"],
            entry["component"]["license"],
        )
    )

    metadata = {
        "schema_version": config["schema_version"],
        "commit": commit,
        "generator": config["generator"],
        "target": config["target"],
        "profiles": config["profiles"],
        "components": [entry["component"] for entry in components],
    }
    components_path = Path(components_path)
    components_path.parent.mkdir(parents=True, exist_ok=True)
    components_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")

    lines = [
        "Third-Party Notices for github.com/tikv/pd",
        "",
        "This source NOTICE covers the declared Go dependency profiles in",
        "third_party/notice/scopes.json, with tests excluded. It is generated for",
        f"{config['target']['goos']}/{config['target']['goarch']} from commit {commit}.",
        "",
        "The generator, profile definitions, first-party policy, audited overrides, and",
        "component provenance are versioned under third_party/notice/. This is a one-time",
        "source-code notice update and is not generated as part of normal builds."
        "",
        "COVERED PROFILES",
        "",
    ]
    for profile in config["profiles"]:
        tags = ", ".join(profile["build_tags"]) or "default build tags"
        lines.extend(
            [
                f"- {profile['id']}: module {profile['module']}; packages {', '.join(profile['packages'])}; {tags}",
                f"  {profile['description']}",
            ]
        )

    lines.extend(["", "THIRD-PARTY COMPONENTS", ""])
    for entry in components:
        component = entry["component"]
        lines.extend(
            [
                f"- {component['module']} {component['version']} — {component['license']}",
                f"  Profiles: {', '.join(profile['id'] for profile in component['profiles'])}",
                "  Go packages in scope: "
                + ", ".join(
                    sorted(
                        {
                            package
                            for profile in component["profiles"]
                            for package in profile["packages"]
                        }
                    )
                ),
                "  License text: reproduced below",
            ]
        )

    lines.extend(["", "LICENSE TEXTS", ""])
    for entry in components:
        component = entry["component"]
        source = entry["source"]
        evidence = source["directory"] / source["component"]["license_evidence"]
        if digest(evidence) != component["license_evidence_sha256"]:
            raise RuntimeError(f"license evidence changed: {evidence}")
        lines.extend(
            [
                f"----- {component['module']} {component['version']} ({component['license']}) -----",
                rendered_text(evidence, component.get("license_evidence_encoding", "utf-8")),
                f"----- end {component['module']} license -----",
                "",
            ]
        )

    notices = []
    for entry in components:
        component = entry["component"]
        for (_, name), path in entry["upstream_notices"].items():
            notices.append((component, name, path))
    if notices:
        lines.extend(["UPSTREAM NOTICES TO RETAIN", ""])
        for component, name, path in sorted(
            notices, key=lambda item: (item[0]["module"], item[0]["version"], item[1])
        ):
            lines.extend(
                [
                    f"----- {component['module']} {component['version']} ({name}) -----",
                    rendered_text(path),
                    f"----- end {component['module']} notice -----",
                    "",
                ]
            )

    Path(notice_path).write_text("\n".join(lines).rstrip() + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit(
            "usage: merge_notice.py SCOPES_JSON EVIDENCE_DIR NOTICE_FILE COMPONENTS_FILE"
        )
    main(*sys.argv[1:])
