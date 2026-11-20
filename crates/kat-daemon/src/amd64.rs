//! Which emulator runs x86-64 programs (amd64 images) in the guest: the
//! `amd64` config key and `run --amd64`.

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::bail;
use kat_vmm::Amd64;
use serde::{Deserialize, Serialize};

/// Where macOS installs Rosetta for Linux (`softwareupdate --install-rosetta`).
pub const ROSETTA_DIR: &str = "/Library/Apple/usr/libexec/oah/RosettaLinux";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Emulation {
    /// Rosetta when it is installed, qemu otherwise.
    #[default]
    Auto,
    Rosetta,
    Qemu,
    Off,
}

impl Emulation {
    pub const NAMES: &str = "auto, rosetta, qemu or off";

    /// The emulator for the VM, given where Rosetta for Linux would be.
    pub fn resolve(self, rosetta_dir: &Path) -> anyhow::Result<Amd64> {
        let installed = rosetta_dir.join("rosetta").is_file();
        let rosetta = || Amd64::Rosetta {
            dir: rosetta_dir.to_path_buf(),
        };
        Ok(match self {
            Emulation::Auto if installed => rosetta(),
            Emulation::Auto | Emulation::Qemu => Amd64::Qemu,
            Emulation::Rosetta if installed => rosetta(),
            Emulation::Rosetta => bail!(
                "Rosetta is not installed ({} is missing); install it with \
                 `softwareupdate --install-rosetta` or set amd64 to qemu",
                rosetta_dir.join("rosetta").display()
            ),
            Emulation::Off => Amd64::Off,
        })
    }

    pub fn resolve_installed(self) -> anyhow::Result<Amd64> {
        self.resolve(&PathBuf::from(ROSETTA_DIR))
    }
}

impl fmt::Display for Emulation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Emulation::Auto => "auto",
            Emulation::Rosetta => "rosetta",
            Emulation::Qemu => "qemu",
            Emulation::Off => "off",
        })
    }
}

impl FromStr for Emulation {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Emulation> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Emulation::Auto,
            "rosetta" => Emulation::Rosetta,
            "qemu" => Emulation::Qemu,
            "off" => Emulation::Off,
            other => bail!("amd64: {other:?}; use {}", Emulation::NAMES),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_prefers_rosetta_when_installed() {
        let dir = std::env::temp_dir().join(format!("kat-amd64-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(Emulation::Auto.resolve(&dir).unwrap(), Amd64::Qemu);
        assert!(Emulation::Rosetta.resolve(&dir).is_err());
        std::fs::write(dir.join("rosetta"), b"").unwrap();
        let rosetta = Amd64::Rosetta { dir: dir.clone() };
        assert_eq!(Emulation::Auto.resolve(&dir).unwrap(), rosetta);
        assert_eq!(Emulation::Rosetta.resolve(&dir).unwrap(), rosetta);
        assert_eq!(Emulation::Qemu.resolve(&dir).unwrap(), Amd64::Qemu);
        assert_eq!(Emulation::Off.resolve(&dir).unwrap(), Amd64::Off);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn parses_and_prints_every_mode() {
        for m in [
            Emulation::Auto,
            Emulation::Rosetta,
            Emulation::Qemu,
            Emulation::Off,
        ] {
            assert_eq!(m.to_string().parse::<Emulation>().unwrap(), m);
        }
        assert_eq!(" QEMU ".parse::<Emulation>().unwrap(), Emulation::Qemu);
        assert!("x86".parse::<Emulation>().is_err());
    }
}
