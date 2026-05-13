//! `cargo xtask <subcommand>` — workspace build automation.
//!
//! Available subcommands:
//!
//! - `check-deps`: enforce the directional dependency graph declared in
//!   `ROADMAP.md` §3. Fails fast on any back-edge.
//!
//! Future subcommands (per `ROADMAP.md` §5): `schema-check`, `gen-manifests`,
//! `bench-report`, `check-metrics`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "xtask", about = "Kamino workspace build automation")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Enforce the directional dependency graph from ROADMAP.md §3.
    CheckDeps,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::CheckDeps => match check_deps() {
            Ok(()) => {
                println!("xtask check-deps: dependency graph is acyclic and directional");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("xtask check-deps failed:\n{e}");
                ExitCode::FAILURE
            }
        },
    }
}

// ---------------------------------------------------------------------------
// Dependency-graph enforcement
// ---------------------------------------------------------------------------

/// The directional graph from ROADMAP.md §3.
///
/// Each entry: crate → set of internal crates it is permitted to depend on.
/// Membership is checked exactly; back-edges and undeclared edges fail.
fn allowed_edges() -> BTreeMap<&'static str, BTreeSet<&'static str>> {
    let edges: &[(&str, &[&str])] = &[
        ("kamino-core", &[]),
        ("kamino-storage", &["kamino-core"]),
        ("kamino-protocol", &["kamino-core"]),
        ("kamino-observability", &["kamino-core"]),
        (
            "kamino-cluster",
            &[
                "kamino-core",
                "kamino-storage",
                "kamino-protocol",
                "kamino-observability",
            ],
        ),
        (
            "kamino-client",
            &["kamino-core", "kamino-cluster", "kamino-protocol"],
        ),
        (
            "kamino-server",
            &[
                "kamino-core",
                "kamino-protocol",
                "kamino-cluster",
                "kamino-observability",
            ],
        ),
        (
            "kamino",
            &[
                "kamino-core",
                "kamino-storage",
                "kamino-protocol",
                "kamino-cluster",
                "kamino-observability",
                "kamino-client",
                "kamino-server",
            ],
        ),
        (
            "kamino-cli",
            &["kamino-core", "kamino-client", "kamino-protocol"],
        ),
    ];
    edges
        .iter()
        .map(|(name, deps)| (*name, deps.iter().copied().collect()))
        .collect()
}

fn check_deps() -> Result<(), String> {
    let workspace_root = workspace_root()?;
    let allowed = allowed_edges();
    let mut violations: Vec<String> = Vec::new();

    for (crate_name, allow_set) in &allowed {
        let manifest = workspace_root
            .join("crates")
            .join(crate_name)
            .join("Cargo.toml");
        let deps = internal_deps(&manifest, &allowed)
            .map_err(|e| format!("reading {}: {e}", manifest.display()))?;
        for dep in &deps {
            if !allow_set.contains(dep.as_str()) {
                violations.push(format!(
                    "  {crate_name} -> {dep}  (not in ROADMAP.md §3 allow-list for {crate_name})",
                ));
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} dependency violation(s):\n{}",
            violations.len(),
            violations.join("\n"),
        ))
    }
}

/// Internal crate names appearing in the `[dependencies]` table of the given
/// manifest. We use the union of allowed-edges keys as the "internal" set so
/// the check works without recursively scanning the whole workspace.
fn internal_deps(
    manifest: &Path,
    allowed: &BTreeMap<&'static str, BTreeSet<&'static str>>,
) -> Result<BTreeSet<String>, String> {
    let text = std::fs::read_to_string(manifest).map_err(|e| e.to_string())?;
    let parsed: toml::Value = text.parse().map_err(|e: toml::de::Error| e.to_string())?;
    let mut out = BTreeSet::new();
    let internal_names: BTreeSet<&str> = allowed.keys().copied().collect();

    for table_name in ["dependencies", "build-dependencies"] {
        let Some(table) = parsed.get(table_name).and_then(|v| v.as_table()) else {
            continue;
        };
        for dep_name in table.keys() {
            if internal_names.contains(dep_name.as_str()) {
                out.insert(dep_name.clone());
            }
        }
    }
    Ok(out)
}

/// Walk up from `CARGO_MANIFEST_DIR` until we find the virtual workspace.
fn workspace_root() -> Result<PathBuf, String> {
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for ancestor in here.ancestors() {
        let candidate = ancestor.join("Cargo.toml");
        if let Ok(text) = std::fs::read_to_string(&candidate) {
            if text.contains("[workspace]") {
                return Ok(ancestor.to_path_buf());
            }
        }
    }
    Err(format!("no workspace root found above {}", here.display()))
}
