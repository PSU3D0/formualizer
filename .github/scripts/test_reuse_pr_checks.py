import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "reuse_pr_checks", Path(__file__).with_name("reuse-pr-checks.py")
)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class ReusePRChecks(unittest.TestCase):
    def setUp(self):
        self.pull = {
            "number": 42,
            "merged_at": "2026-11-20T07:00:00Z",
            "merge_commit_sha": "merge",
            "base": {"ref": "main"},
            "head": {"sha": "head"},
        }
        self.run = {
            "id": 123,
            "event": "pull_request",
            "head_sha": "head",
            "conclusion": "success",
            "run_attempt": 2,
        }
        self.artifact = {"name": "tested-tree-pr-42-2-tree", "expired": False}

    def api(self, endpoint):
        if endpoint == "commits/merge/pulls":
            return [self.pull]
        if endpoint.startswith("actions/workflows/wasm.yml/runs?"):
            self.assertIn("event=pull_request&head_sha=head&status=success", endpoint)
            return {"workflow_runs": [self.run]}
        if endpoint == "actions/runs/123/artifacts?name=tested-tree-pr-42-2-tree":
            return {"artifacts": [self.artifact]}
        self.fail(f"Unexpected endpoint: {endpoint}")

    def match(self):
        return gate.matching_run(self.api, "wasm.yml", "merge", "tree")

    def test_matching_run(self):
        self.assertEqual(self.match(), self.run)

    def test_requires_the_actual_pr_merge_on_main(self):
        for key, other in (
            ("merged_at", None),
            ("merge_commit_sha", "different"),
            ("base", {"ref": "develop"}),
        ):
            with self.subTest(key=key), patch.dict(self.pull, {key: other}):
                self.assertIsNone(self.match())

    def test_requires_the_successful_pr_run_on_its_head(self):
        for key, other in (
            ("event", "push"),
            ("head_sha", "different"),
            ("conclusion", "failure"),
        ):
            with self.subTest(key=key), patch.dict(self.run, {key: other}):
                self.assertIsNone(self.match())

    def test_requires_current_attempt_pr_and_tree(self):
        for artifact in (
            {"name": "tested-tree-pr-42-2-tree", "expired": True},
            {"name": "tested-tree-pr-42-1-tree", "expired": False},
            {"name": "tested-tree-pr-43-2-tree", "expired": False},
            {"name": "tested-tree-pr-42-2-other", "expired": False},
        ):
            with self.subTest(artifact=artifact), patch.dict(self.artifact, artifact):
                self.assertIsNone(self.match())

    def test_multiple_commits_or_workflow_changes_run_full_ci(self):
        cases = [
            ["merge", "some-other-parent"],
            ["merge", "before", ".github/workflows/ci.yml"],
        ]
        for commands in cases:
            with (
                self.subTest(commands=commands),
                patch.dict(gate.os.environ, {"GITHUB_SHA": "merge", "PUSH_BEFORE": "before"}),
                patch.object(gate, "git", side_effect=commands),
                patch.object(gate, "github") as api,
            ):
                self.assertIsNone(gate.reusable_run())
                api.assert_not_called()

    def test_api_failure_leaves_reuse_unset(self):
        error = subprocess.CalledProcessError(1, "gh")
        with patch.object(gate, "reusable_run", side_effect=error), patch("builtins.open") as output:
            gate.main()
            output.assert_not_called()


if __name__ == "__main__":
    unittest.main()
