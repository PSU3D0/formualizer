"""Reuse a successful PR workflow only when it tested the resulting main tree."""

import json
import os
import subprocess


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def github(endpoint):
    repository = os.environ["GITHUB_REPOSITORY"]
    return json.loads(subprocess.check_output(
        ["gh", "api", f"repos/{repository}/{endpoint}"], text=True
    ))


def matching_run(api, workflow, commit, tree):
    pulls = api(f"commits/{commit}/pulls")
    for pull in pulls:
        if not (
            pull["merged_at"]
            and pull["merge_commit_sha"] == commit
            and pull["base"]["ref"] == "main"
        ):
            continue

        head = pull["head"]["sha"]
        runs = api(
            f"actions/workflows/{workflow}/runs"
            f"?event=pull_request&head_sha={head}&status=success&per_page=30"
        )["workflow_runs"]
        for run in runs:
            if (
                run["event"] != "pull_request"
                or run["head_sha"] != head
                or run["conclusion"] != "success"
            ):
                continue
            name = f"tested-tree-pr-{pull['number']}-{run['run_attempt']}-{tree}"
            artifacts = api(f"actions/runs/{run['id']}/artifacts?name={name}")["artifacts"]
            if any(artifact["name"] == name and not artifact["expired"] for artifact in artifacts):
                return run
    return None


def reusable_run():
    commit = os.environ["GITHUB_SHA"]
    before = os.environ["PUSH_BEFORE"]
    if git("rev-parse", "HEAD") != commit or git("rev-parse", "HEAD^1") != before:
        return None
    # Require a fresh execution for changes to workflows and their helpers.
    if git("diff", "--name-only", before, commit, "--", ".github"):
        return None
    tree = git("rev-parse", "HEAD^{tree}")
    return matching_run(github, os.environ["WORKFLOW_FILE"], commit, tree)


def main():
    try:
        run = reusable_run()
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(f"Could not verify prior CI: {error}")
        return

    if run is None:
        print("No successful PR run recorded this source tree. Running full checks.")
        return

    print(f"Reusing successful PR workflow {run['html_url']}")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write("reuse=true\n")


if __name__ == "__main__":
    main()
