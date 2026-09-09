#!/usr/bin/env python3
"""Generate or verify PD's scoped Go third-party notice."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
NOTICE_DIR = Path(__file__).resolve().parent
CONFIG = NOTICE_DIR / "scopes.json"
COLLECTOR = NOTICE_DIR / "collect.sh"
MERGER = NOTICE_DIR / "merge_notice.py"


def run(command, env):
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def generate(directory):
    config = json.loads(CONFIG.read_text())
    evidence = directory / "evidence"
    for profile in config["profiles"]:
        env = os.environ.copy()
        env.update(
            {
                "GOOS": config["target"]["goos"],
                "GOARCH": config["target"]["goarch"],
                "FIRST_PARTY_PREFIXES": ",".join(config["target"]["first_party_prefixes"]),
                "NOTICE_GENERATOR_IMAGE": config["generator"]["image"],
                "NOTICE_SOURCE_COMMIT": config["source_commit"],
                "NOTICE_OVERRIDES": str(NOTICE_DIR / "overrides.json"),
                "GOFLAGS": "-tags=" + ",".join(profile["build_tags"]),
            }
        )
        if not profile["build_tags"]:
            env.pop("GOFLAGS")
        output = evidence / profile["id"]
        run([str(COLLECTOR), str(ROOT / profile["module"]), str(output), *profile["packages"]], env)
        version = (output / "go.version.txt").read_text().splitlines()[0]
        if config["generator"]["go_version"] not in version:
            raise RuntimeError(
                f"{profile['id']} used {version}, expected {config['generator']['go_version']}"
            )

    notice = directory / "ThirdPartyNotices.txt"
    components = directory / "components.json"
    run([sys.executable, str(MERGER), str(CONFIG), str(evidence), str(notice), str(components)], os.environ.copy())
    return config, notice, components


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if committed outputs are stale")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="pd-notice-") as temporary:
        config, notice, components = generate(Path(temporary))
        committed_notice = ROOT / config["notice_file"]
        committed_components = ROOT / config["components_file"]
        if args.check:
            if notice.read_bytes() != committed_notice.read_bytes() or components.read_bytes() != committed_components.read_bytes():
                raise SystemExit("Third-party notice outputs are stale; run third_party/notice/generate.py")
            return
        shutil.copyfile(notice, committed_notice)
        committed_components.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(components, committed_components)


if __name__ == "__main__":
    main()
