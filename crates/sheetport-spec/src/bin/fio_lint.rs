use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use sheetport_spec::{Manifest, ManifestIssue};

/// Validate SheetPort manifests (FIO spec) and optionally emit normalized YAML.
#[derive(Debug, Parser)]
#[command(
    name = "fio-lint",
    about = "Validate Formualizer I/O (SheetPort) manifest files."
)]
struct Args {
    /// Path to the manifest YAML file.
    path: PathBuf,

    /// Emit normalized YAML to stdout instead of a status message.
    #[arg(long)]
    normalize: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(args) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(2)
        }
    }
}

fn run(args: Args) -> anyhow::Result<ExitCode> {
    let file = File::open(&args.path)
        .map_err(|err| anyhow::anyhow!("failed to open {}: {err}", args.path.display()))?;
    let mut manifest = Manifest::from_yaml_reader(BufReader::new(file))
        .map_err(|err| anyhow::anyhow!("failed to parse {}: {err}", args.path.display()))?;

    if let Err(validation) = manifest.validate() {
        report_issues(&args.path, validation.issues());
        return Ok(ExitCode::from(1));
    }

    if args.normalize {
        manifest.normalize();
        let yaml = manifest
            .to_yaml()
            .map_err(|err| anyhow::anyhow!("failed to serialize manifest: {err}"))?;
        print!("{yaml}");
    } else {
        println!("OK: {}", args.path.display());
    }

    Ok(ExitCode::SUCCESS)
}

fn report_issues(path: &Path, issues: &[ManifestIssue]) {
    eprintln!("Manifest validation failed: {}", path.display());
    for issue in issues {
        eprintln!("  - {}: {}", issue.path, issue.message);
    }
}
