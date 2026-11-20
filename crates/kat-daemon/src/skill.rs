//! `katamaran skill`: the guide that teaches coding agents to use katamaran.
//! Agents get a small stub skill that runs `kat skill show`, so the guide
//! always matches the installed `katamaran` binary.

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use clap::Subcommand;

/// What `kat skill show` prints.
pub const GUIDE: &str = include_str!("skill.md");
/// The stub skill installed into coding agents.
pub const STUB: &str = include_str!("../../../skills/katamaran/SKILL.md");

#[derive(Subcommand)]
pub enum SkillCmd {
    /// Print the guide the skill gives agents.
    Show,
    /// Install the skill for every agent found: Claude Code (~/.claude/skills),
    /// and Codex, OpenCode and Pi (~/.agents/skills).
    Setup {
        /// Only this agent (claude, agents).
        #[arg(long)]
        agent: Option<String>,
    },
    /// Remove the skill from agents.
    Remove {
        /// Only this agent (claude, agents).
        #[arg(long)]
        agent: Option<String>,
    },
    /// Show where the skill is installed and whether it is current.
    Status {
        /// Only this agent (claude, agents).
        #[arg(long)]
        agent: Option<String>,
        /// Print status as JSON.
        #[arg(long)]
        json: bool,
    },
}

struct Target {
    kind: &'static str,
    name: &'static str,
    path: PathBuf,
    present: bool,
}

fn targets(home: &Path, claude_dir: Option<PathBuf>) -> Vec<Target> {
    let claude = claude_dir.unwrap_or_else(|| home.join(".claude"));
    let agents = [".agents", ".codex", ".config/opencode", ".pi"];
    vec![
        Target {
            kind: "claude",
            name: "Claude Code",
            path: claude.join("skills/katamaran"),
            present: claude.is_dir(),
        },
        Target {
            kind: "agents",
            name: "Codex, OpenCode, Pi",
            path: home.join(".agents/skills/katamaran"),
            present: agents.iter().any(|d| home.join(d).is_dir()),
        },
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Missing,
    Current,
    Outdated,
    Other,
}

impl State {
    fn name(self) -> &'static str {
        match self {
            State::Missing => "missing",
            State::Current => "current",
            State::Outdated => "outdated",
            State::Other => "other",
        }
    }
}

fn state(path: &Path) -> State {
    if std::fs::symlink_metadata(path).is_err() {
        return State::Missing;
    }
    match std::fs::read_to_string(path.join("SKILL.md")) {
        Ok(text) if text == STUB => State::Current,
        Ok(text) if text.contains("katamaran-stub:") => State::Outdated,
        _ => State::Other,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Change {
    Unchanged,
    Changed,
}

fn install(path: &Path) -> anyhow::Result<Change> {
    match state(path) {
        State::Current => return Ok(Change::Unchanged),
        State::Other => {
            bail!(
                "{} exists and isn't katamaran's; not replacing it",
                path.display()
            );
        }
        State::Outdated if std::fs::read_link(path).is_ok() => {
            std::fs::remove_file(path)
                .with_context(|| format!("remove link {}", path.display()))?;
        }
        State::Outdated | State::Missing => {}
    }
    let file = path.join("SKILL.md");
    if std::fs::read_link(&file).is_ok() {
        std::fs::remove_file(&file).with_context(|| format!("remove link {}", file.display()))?;
    }
    std::fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))?;
    std::fs::write(&file, STUB).with_context(|| format!("write {}", file.display()))?;
    Ok(Change::Changed)
}

fn remove(path: &Path) -> anyhow::Result<Change> {
    match state(path) {
        State::Missing => Ok(Change::Unchanged),
        State::Other => bail!("{} isn't katamaran's; not removing it", path.display()),
        State::Current | State::Outdated if std::fs::read_link(path).is_ok() => {
            std::fs::remove_file(path)
                .with_context(|| format!("remove link {}", path.display()))?;
            Ok(Change::Changed)
        }
        State::Current | State::Outdated => {
            let file = path.join("SKILL.md");
            std::fs::remove_file(&file).with_context(|| format!("remove {}", file.display()))?;
            let _ = std::fs::remove_dir(path);
            Ok(Change::Changed)
        }
    }
}

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

pub fn run(cmd: SkillCmd) -> anyhow::Result<()> {
    let (action, only, json) = match cmd {
        SkillCmd::Show => {
            print!("{GUIDE}");
            return Ok(());
        }
        SkillCmd::Setup { agent } => ("setup", agent, false),
        SkillCmd::Remove { agent } => ("remove", agent, false),
        SkillCmd::Status { agent, json } => ("status", agent, json),
    };
    let only = only.map(|k| {
        if matches!(k.as_str(), "codex" | "opencode" | "pi") {
            "agents".into()
        } else {
            k
        }
    });
    let claude_dir = std::env::var_os("CLAUDE_CONFIG_DIR")
        .filter(|d| !d.is_empty())
        .map(PathBuf::from);
    let all = targets(&home(), claude_dir);
    let chosen: Vec<&Target> = match only.as_deref() {
        Some(k) => all.iter().filter(|t| t.kind == k).collect(),
        None => all
            .iter()
            .filter(|t| t.present || (json && action == "status"))
            .collect(),
    };
    if only.is_some() && chosen.is_empty() {
        bail!(
            "unknown agent {:?} (claude, agents)",
            only.unwrap_or_default()
        );
    }
    if chosen.is_empty() {
        bail!(
            "no agent found (no ~/.claude, ~/.agents, ~/.codex, ~/.config/opencode or ~/.pi); \
             run the agent once, or pass --agent claude"
        );
    }

    if action == "status" {
        let mut report = Vec::new();
        let mut all_current = true;
        for t in chosen {
            let s = state(&t.path);
            if json {
                report.push(serde_json::json!({
                    "agent": t.kind,
                    "name": t.name,
                    "state": s.name(),
                    "present": t.present,
                    "path": t.path.display().to_string(),
                }));
            } else {
                println!("{}: {} ({})", t.name, s.name(), t.path.display());
            }
            all_current &= s == State::Current;
        }
        if json {
            println!("{}", serde_json::to_string_pretty(&report)?);
            return Ok(());
        }
        if !all_current {
            bail!("skill is not current for every agent; run `kat skill setup`");
        }
        return Ok(());
    }

    let mut failed = false;
    for t in chosen {
        let done = if action == "setup" {
            install(&t.path)
        } else {
            remove(&t.path)
        };
        match done {
            Ok(Change::Unchanged) => {
                println!("{}: nothing to change ({})", t.name, t.path.display());
            }
            Ok(Change::Changed) if action == "setup" => {
                println!("{}: skill installed ({})", t.name, t.path.display());
            }
            Ok(Change::Changed) => {
                println!("{}: skill removed ({})", t.name, t.path.display());
            }
            Err(e) => {
                eprintln!("katamaran: {}: {e:#}", t.name);
                failed = true;
            }
        }
    }
    if failed {
        bail!("one or more agent skill targets failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("kat-skill-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn installs_updates_and_removes_stub() {
        let dir = scratch("lifecycle");
        let path = dir.join("skills/katamaran");

        assert_eq!(state(&path), State::Missing);
        assert_eq!(install(&path).unwrap(), Change::Changed);
        assert_eq!(state(&path), State::Current);
        assert_eq!(install(&path).unwrap(), Change::Unchanged);

        std::fs::write(
            path.join("SKILL.md"),
            "metadata:\n  katamaran-stub: \"0\"\n",
        )
        .unwrap();
        assert_eq!(state(&path), State::Outdated);
        assert_eq!(install(&path).unwrap(), Change::Changed);
        assert_eq!(state(&path), State::Current);

        assert_eq!(remove(&path).unwrap(), Change::Changed);
        assert_eq!(state(&path), State::Missing);
        assert_eq!(remove(&path).unwrap(), Change::Unchanged);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn refuses_to_overwrite_or_remove_unrelated_skill() {
        let dir = scratch("other");
        let path = dir.join("skills/katamaran");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("SKILL.md"), "# Custom skill\n").unwrap();

        assert_eq!(state(&path), State::Other);
        assert!(install(&path).is_err());
        assert!(remove(&path).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discovers_installed_agent_directories() {
        let dir = scratch("targets");
        std::fs::create_dir_all(dir.join(".claude")).unwrap();
        std::fs::create_dir_all(dir.join(".pi")).unwrap();

        let found = targets(&dir, None);
        assert_eq!(found.len(), 2);
        assert!(found[0].present);
        assert!(found[1].present);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
