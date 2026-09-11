import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


NOTICE_DIR = Path(__file__).resolve().parent


def load_module(name):
    spec = importlib.util.spec_from_file_location(name, NOTICE_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


GENERATE = load_module("generate")
MERGE_NOTICE = load_module("merge_notice")
RENDER_SCOPE = load_module("render_scope")


class NoticeTest(unittest.TestCase):
    def test_renderers_preserve_license_headings(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence = Path(temporary) / "LICENSE"
            evidence.write_text("before\n=======\nafter\n")
            for renderer in (MERGE_NOTICE, RENDER_SCOPE):
                with self.subTest(renderer=renderer.__name__):
                    self.assertEqual(renderer.rendered_text(evidence), "before\n=======\nafter")

    def test_repository_url_removes_credentials_and_allows_no_origin(self):
        with patch.object(
            RENDER_SCOPE,
            "command",
            return_value="https://user:token@example.com/pd.git",
        ):
            self.assertEqual(RENDER_SCOPE.repository_url(Path(".")), "https://example.com/pd.git")
        with patch.object(
            RENDER_SCOPE,
            "command",
            side_effect=subprocess.CalledProcessError(1, ["git"]),
        ):
            self.assertEqual(RENDER_SCOPE.repository_url(Path(".")), "")

    def test_source_checkout_uses_the_pinned_commit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            root.mkdir()
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "config", "user.name", "Test"], check=True)
            subprocess.run(["git", "-C", str(root), "config", "user.email", "test@example.com"], check=True)
            source = root / "source.txt"
            source.write_text("pinned")
            subprocess.run(["git", "-C", str(root), "add", "source.txt"], check=True)
            subprocess.run(["git", "-C", str(root), "commit", "-qm", "pinned"], check=True)
            commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
            source.write_text("current")
            subprocess.run(["git", "-C", str(root), "commit", "-am", "current", "-q"], check=True)

            output = Path(temporary) / "output"
            output.mkdir()
            with patch.object(GENERATE, "ROOT", root):
                with GENERATE.source_checkout(output, commit) as checkout:
                    self.assertEqual((checkout / "source.txt").read_text(), "pinned")
            self.assertFalse((output / "source").exists())


if __name__ == "__main__":
    unittest.main()
