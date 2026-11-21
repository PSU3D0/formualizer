use anyhow::{Context, Result, ensure};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::Path, process::Command};
use walkdir::WalkDir;

const MANIFEST: &str = "docs-site/src/generated/content-dates.json";
const ROOTS: &[&str] = &[
    "docs-site/content/docs",
    "docs-site/src/app/(home)/page.tsx",
    "docs-site/src/components/home",
    "docs-site/public/home",
    "docs-site/src/app/formula-parser/page.tsx",
    "docs-site/src/app/formula-parser/page.client.tsx",
    "docs-site/src/components/playground/formula-parser-tool.tsx",
    "docs-site/src/app/(home)/evaluate-excel-formulas-python/page.tsx",
    "docs-site/src/app/(home)/spreadsheet-engine-for-ai-agents/page.tsx",
    "docs",
    "skills/formualizer-recalc",
];

#[derive(Deserialize, Serialize)]
struct SourceDate {
    date: String,
    sha256: String,
}

fn git(args: &[&str]) -> Result<String> {
    let output = Command::new("git").args(args).output()?;
    ensure!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

fn history() -> Result<BTreeMap<String, String>> {
    let mut args = vec!["log", "--format=%x1e%cs", "--name-only", "--"];
    args.extend_from_slice(ROOTS);
    let log = git(&args)?;
    let mut dates = BTreeMap::new();
    for block in log
        .split('\x1e')
        .map(str::trim)
        .filter(|block| !block.is_empty())
    {
        let mut lines = block.lines();
        let date = lines.next().unwrap();
        for file in lines.filter(|line| !line.is_empty()) {
            dates
                .entry(file.to_string())
                .or_insert_with(|| date.to_string());
        }
    }
    Ok(dates)
}

pub(super) fn update(apply: bool) -> Result<()> {
    let previous = match fs::read_to_string(MANIFEST) {
        Ok(text) => serde_json::from_str::<BTreeMap<String, SourceDate>>(&text)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && apply => BTreeMap::new(),
        Err(error) => return Err(error).context("read content date manifest"),
    };
    let today = Utc::now().date_naive().to_string();
    let mut initial_history = None;
    let mut next = BTreeMap::new();
    let mut stale = Vec::new();
    for root in ROOTS {
        for file in WalkDir::new(root) {
            let file = file?;
            if !file.file_type().is_file() {
                continue;
            }
            let path = super::path_to_repo_string(file.path())?;
            let digest = format!("{:x}", Sha256::digest(fs::read(file.path())?));
            let date = match previous.get(&path) {
                Some(existing) if existing.sha256 == digest => existing.date.clone(),
                Some(_) => today.clone(),
                None if apply => {
                    if initial_history.is_none() {
                        let mut args = vec!["diff", "--name-only", "HEAD", "--"];
                        args.extend_from_slice(ROOTS);
                        let changed = git(&args)?;
                        let mut dates = history()?;
                        for path in changed.lines() {
                            dates.remove(path);
                        }
                        initial_history = Some(dates);
                    }
                    initial_history
                        .as_ref()
                        .unwrap()
                        .get(&path)
                        .cloned()
                        .unwrap_or_else(|| today.clone())
                }
                None => today.clone(),
            };
            if previous
                .get(&path)
                .is_none_or(|existing| existing.sha256 != digest)
            {
                stale.push(path.clone());
            }
            next.insert(
                path,
                SourceDate {
                    date,
                    sha256: digest,
                },
            );
        }
    }
    stale.extend(
        previous
            .keys()
            .filter(|key| !next.contains_key(*key))
            .cloned(),
    );
    if apply {
        let updated = serde_json::to_string_pretty(&next)? + "\n";
        if fs::read_to_string(Path::new(MANIFEST)).ok().as_deref() != Some(&updated) {
            fs::write(MANIFEST, updated)?;
        }
    } else {
        ensure!(
            stale.is_empty(),
            "content date manifest is stale for {}; run cargo run -p xtask -- ref --apply --allow-dirty",
            stale.join(", ")
        );
    }
    Ok(())
}
