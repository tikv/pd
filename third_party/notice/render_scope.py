#!/usr/bin/env python3
"""Render a self-contained Go-only ThirdPartyNotices.txt from go-licenses."""

import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from urllib.parse import urlparse
from pathlib import Path


def command(*args, cwd=None, env=None):
    return subprocess.check_output(args, cwd=cwd, env=env, text=True).strip()


def repository_url(repo):
    try:
        url = command("git", "config", "--get", "remote.origin.url", cwd=repo)
    except subprocess.CalledProcessError:
        return ""
    parsed = urlparse(url)
    if parsed.username is None:
        return url
    return parsed._replace(netloc=parsed.netloc.rsplit("@", 1)[-1]).geturl()


def json_stream(text):
    decoder = json.JSONDecoder()
    offset = 0
    while offset < len(text):
        while offset < len(text) and text[offset].isspace():
            offset += 1
        if offset == len(text):
            return
        value, offset = decoder.raw_decode(text, offset)
        yield value


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rendered_text(path, encoding="utf-8"):
    return "\n".join(
        line.expandtabs(8).rstrip()
        for line in path.read_text(encoding=encoding).splitlines()
    ).rstrip()


def license_file(evidence_dir):
    matches = sorted(
        path for path in evidence_dir.iterdir()
        if path.is_file() and path.name.lower().startswith(("license", "licence", "copying", "unlicense"))
    )
    if not matches:
        matches = sorted(
            path for path in evidence_dir.iterdir()
            if path.is_file() and path.name.lower().startswith("readme")
        )
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one license evidence file for {evidence_dir}, found {matches}"
        )
    return matches[0]


def materialize_report_evidence(evidence_dir, module, source_url):
    marker = f"/blob/{module['Version']}/"
    path = urlparse(source_url).path
    if marker not in path:
        return
    source = Path(module["Dir"]) / path.split(marker, 1)[1]
    if source.is_file():
        evidence_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, evidence_dir / source.name)


def override_source(override, module, overrides_path, path_key, scope_key):
    scope = override.get(scope_key, "module")
    if scope == "module":
        root = Path(module["Dir"])
    elif scope == "override-directory":
        root = overrides_path.parent
    else:
        raise RuntimeError(f"unsupported override evidence scope: {scope}")
    source = (root / override[path_key]).resolve()
    try:
        source.relative_to(root.resolve())
    except ValueError as error:
        raise RuntimeError(f"override evidence escapes its root: {source}") from error
    if not source.is_file():
        raise RuntimeError(f"override evidence is missing: {source}")
    return source


def main(repo_arg, out_arg, package_args):
    repo = Path(repo_arg).resolve()
    out = Path(out_arg).resolve()
    env = os.environ.copy()
    goos, goarch = env["GOOS"], env["GOARCH"]
    packages = list(json_stream(command(
        "go", "list", "-deps", "-json", *package_args, cwd=repo, env=env
    )))
    modules = {}
    for package in packages:
        module = package.get("Module")
        if module and not module.get("Main"):
            modules[module["Path"]] = module

    with (out / "go-licenses-report.csv").open(newline="") as report_file:
        report_rows = list(csv.reader(report_file))

    grouped = {}
    for package, source_url, license_id in report_rows:
        candidates = [
            module for module_path, module in modules.items()
            if package == module_path or package.startswith(module_path + "/")
        ]
        if not candidates:
            raise RuntimeError(f"no Go module mapping for {package}")
        module = max(candidates, key=lambda item: len(item["Path"]))
        evidence_dir = out / "LICENSES" / package
        try:
            license_path = license_file(evidence_dir)
        except RuntimeError:
            materialize_report_evidence(evidence_dir, module, source_url)
            license_path = license_file(evidence_dir)
        evidence = str(license_path.relative_to(out))
        key = (
            module["Path"], module["Version"], license_id,
            evidence, sha256(license_path),
        )
        component = grouped.setdefault(key, {
            "module": module["Path"],
            "version": module["Version"],
            "go_module_checksum": module.get("Sum"),
            "license": license_id,
            "license_evidence": evidence,
            "license_evidence_sha256": key[-1],
            "license_evidence_encoding": "utf-8",
            "packages_in_scope": [],
            "raw_upstream_license_urls": [],
            "upstream_notices": [],
        })
        component["packages_in_scope"].append(package)
        component["raw_upstream_license_urls"].append(source_url)
        for notice_path in sorted(
            path for path in evidence_dir.iterdir()
            if path.is_file() and path.name.lower().startswith("notice")
        ):
            component["upstream_notices"].append({
                "path": str(notice_path.relative_to(out)),
                "sha256": sha256(notice_path),
            })

    overrides_path = os.environ.get("NOTICE_OVERRIDES")
    if overrides_path:
        overrides_path = Path(overrides_path).resolve()
        overrides = json.loads(overrides_path.read_text())
        override_modules = set()
        for override in overrides:
            if override["module"] in override_modules:
                raise RuntimeError(f"duplicate override module: {override['module']}")
            override_modules.add(override["module"])
            module = modules.get(override["module"])
            if not module:
                continue
            if module.get("Version") != override["version"]:
                raise RuntimeError(
                    f"override version mismatch for {override['module']}: "
                    f"expected {override['version']}, got {module.get('Version')}"
                )
            source = override_source(
                override, module, overrides_path, "license_path", "license_path_scope"
            )
            destination = out / "LICENSES" / override["module"] / source.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            evidence = str(destination.relative_to(out))
            key = (
                module["Path"], module["Version"], override["license"],
                evidence, sha256(destination),
            )
            if key in grouped:
                raise RuntimeError(f"override duplicates collected component: {override['module']}")
            notices = []
            for notice_path in override.get("notice_paths", []):
                notice_source = override_source(
                    {**override, "notice_path": notice_path}, module, overrides_path,
                    "notice_path", "notice_path_scope",
                )
                notice_destination = destination.parent / notice_source.name
                if notice_destination == destination:
                    raise RuntimeError(f"override notice duplicates license evidence: {notice_source}")
                shutil.copyfile(notice_source, notice_destination)
                notices.append({
                    "path": str(notice_destination.relative_to(out)),
                    "sha256": sha256(notice_destination),
                })
            source_urls = override.get("source_urls")
            if source_urls is None:
                source_urls = [override.get("source_url", "")]
            packages_in_scope = sorted(
                package["ImportPath"] for package in packages
                if (package.get("Module") or {}).get("Path") == override["module"]
            )
            if not packages_in_scope:
                raise RuntimeError(f"override has no Go packages in scope: {override['module']}")
            grouped[key] = {
                "module": module["Path"],
                "version": module["Version"],
                "go_module_checksum": module.get("Sum"),
                "license": override["license"],
                "license_evidence": evidence,
                "license_evidence_sha256": key[-1],
                "license_evidence_encoding": override.get("license_encoding", "utf-8"),
                "packages_in_scope": packages_in_scope,
                "raw_upstream_license_urls": source_urls,
                "upstream_notices": notices,
                "override_reason": override.get("reason", ""),
            }

    components = list(grouped.values())
    for component in components:
        component["packages_in_scope"] = sorted(set(component["packages_in_scope"]))
        component["raw_upstream_license_urls"] = sorted(
            url for url in set(component["raw_upstream_license_urls"]) if url
        )
        component["upstream_notices"] = sorted(
            {entry["path"]: entry for entry in component["upstream_notices"]}.values(),
            key=lambda entry: entry["path"],
        )
    components.sort(key=lambda item: (item["module"], item["version"], item["license_evidence"]))

    commit = os.environ.get("NOTICE_SOURCE_COMMIT") or command("git", "rev-parse", "HEAD", cwd=repo)
    root_module = command("go", "list", "-m", "-f", "{{.Path}}", cwd=repo, env=env)
    metadata = {
        "schema_version": 1,
        "repository": repository_url(repo),
        "commit": commit,
        "scope": {
            "description": "Go source packages resolved by go list -deps; tests are excluded.",
            "go_packages": package_args,
            "goos": goos,
            "goarch": goarch,
            "first_party_prefixes": [
                prefix for prefix in os.environ.get("FIRST_PARTY_PREFIXES", "").split(",")
                if prefix
            ],
            "excluded": [root_module, "Go standard library"],
        },
        "components": components,
    }
    if image := os.environ.get("NOTICE_GENERATOR_IMAGE"):
        metadata["generator_image"] = image
    (out / "components.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n"
    )

    lines = [
        f"Third-Party Notices for {root_module}",
        "",
        f"This file covers the Go source packages resolved by `go list -deps {' '.join(package_args)}`",
        f"with tests excluded, for {goos}/{goarch}. It was generated from commit {commit}.",
        "",
        "It excludes the Go standard library and the first-party modules listed in the",
        "generation evidence. The license texts and upstream notices for the listed",
        "third-party Go components are reproduced below.",
        "",
        "THIRD-PARTY COMPONENTS",
        "",
    ]
    for component in components:
        lines.extend([
            f"- {component['module']} {component['version']} — {component['license']}",
            f"  Go packages in scope: {', '.join(component['packages_in_scope'])}",
            "  License text: reproduced below",
        ])

    lines.extend(["", "LICENSE TEXTS", ""])
    for component in components:
        license_path = out / component["license_evidence"]
        lines.extend([
            f"----- {component['module']} {component['version']} ({component['license']}) -----",
            rendered_text(license_path, component["license_evidence_encoding"]),
            f"----- end {component['module']} license -----",
            "",
        ])

    notices = [
        (component, notice)
        for component in components for notice in component["upstream_notices"]
    ]
    if notices:
        lines.extend(["UPSTREAM NOTICES TO RETAIN", ""])
        for component, notice in notices:
            notice_path = out / notice["path"]
            lines.extend([
                f"----- {component['module']} {component['version']} ({notice_path.name}) -----",
                rendered_text(notice_path),
                f"----- end {component['module']} notice -----",
                "",
            ])

    (out / "ThirdPartyNotices.txt").write_text("\n".join(lines).rstrip() + "\n")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} REPO_DIR OUTPUT_DIR [GO_PACKAGE ...]")
    main(sys.argv[1], sys.argv[2], sys.argv[3:] or ["./..."])
