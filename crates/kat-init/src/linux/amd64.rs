//! x86-64 programs (amd64 images). The host picks the emulator with
//! `kat.amd64=` on the kernel command line:
//! - `rosetta`: Apple's Rosetta for Linux, served by the host on the
//!   read-only virtio-fs tag `rosetta`, which also answers its ioctls;
//! - `qemu`: qemu-user from the system image.
//!
//! Either is registered with binfmt_misc with the F flag, so the kernel opens
//! the interpreter now and every container can run x86-64 binaries without
//! having it in its file system.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::OnceLock;

use super::sys;

const BINFMT_MISC: &str = "/proc/sys/fs/binfmt_misc";
const ROSETTA_TAG: &str = "rosetta";
const ROSETTA_DIR: &str = "/run/rosetta";
const QEMU: &str = "/usr/bin/qemu-x86_64";

/// Set once [`setup`] has successfully registered an x86-64 binfmt_misc handler.
static REGISTERED: OnceLock<Emulator> = OnceLock::new();
/// Emulator requested on the kernel command line, recorded before setup is
/// attempted so a failed registration is never mistaken for `amd64 = "off"`.
static REQUESTED: OnceLock<Emulator> = OnceLock::new();

/// An x86-64 ELF executable: class 64-bit, little endian, `e_type` EXEC or
/// DYN, `e_machine` 0x3e. The mask ignores the OS ABI and padding bytes.
const MAGIC: &[u8] = b"\x7fELF\x02\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x3e\x00";
const MASK: &[u8] =
    b"\xff\xff\xff\xff\xff\xff\xff\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe\xff\xff\xff";

/// P keeps argv[0] (both handle AT_FLAGS_PRESERVE_ARGV0), O and C give
/// set-user-ID binaries their credentials, F opens the interpreter at
/// registration.
const FLAGS: &str = "POCF";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Emulator {
    Rosetta,
    Qemu,
}

impl Emulator {
    pub fn from_cmdline(cmdline: &str) -> Option<Emulator> {
        match cmdline
            .split_whitespace()
            .rev()
            .find_map(|a| a.strip_prefix("kat.amd64="))?
        {
            "rosetta" => Some(Emulator::Rosetta),
            "qemu" => Some(Emulator::Qemu),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Emulator::Rosetta => "rosetta",
            Emulator::Qemu => "qemu-x86_64",
        }
    }
}

/// Register the emulator the command line asks for, if any.
pub fn setup(cmdline: &str) -> io::Result<()> {
    let Some(emulator) = Emulator::from_cmdline(cmdline) else {
        return Ok(());
    };
    let _ = REQUESTED.set(emulator);
    let interpreter = match emulator {
        Emulator::Rosetta => {
            sys::mount(
                ROSETTA_TAG,
                ROSETTA_DIR,
                "virtiofs",
                libc::MS_RDONLY | libc::MS_NODEV | libc::MS_NOSUID,
                None,
            )?;
            format!("{ROSETTA_DIR}/rosetta")
        }
        Emulator::Qemu => QEMU.to_string(),
    };
    if !sys::exists(&format!("{BINFMT_MISC}/register")) {
        sys::mount("binfmt_misc", BINFMT_MISC, "binfmt_misc", 0, None)?;
    }
    fs::write(
        format!("{BINFMT_MISC}/register"),
        registration(emulator.name(), &interpreter),
    )?;
    let _ = REGISTERED.set(emulator);
    Ok(())
}

/// Live status of x86-64 `binfmt_misc` emulation in the guest kernel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmulationStatus {
    /// An emulator was registered at boot and both `binfmt_misc/status` and
    /// the registered handler entry currently report `enabled`.
    Enabled,
    /// No emulator was requested on the kernel command line (`amd64 = "off"`).
    /// This is the only state in which callers may retire emulation mirrors.
    Disabled,
    /// An emulator was requested, but it failed to register at boot, its
    /// handler is now missing or disabled, or `binfmt_misc` state could not be
    /// read; callers must not treat this as disabled or retire mirrors.
    Unknown(String),
}

/// Query whether an x86-64 emulator was registered at boot and its
/// `binfmt_misc` handler is currently enabled, distinguishing I/O errors
/// (`Unknown`) from an actually disabled or unregistered handler (`Disabled`).
pub fn status() -> EmulationStatus {
    status_from(
        REQUESTED.get().copied(),
        REGISTERED.get().copied(),
        Path::new(BINFMT_MISC),
    )
}

fn status_from(
    requested: Option<Emulator>,
    registered: Option<Emulator>,
    binfmt_dir: &Path,
) -> EmulationStatus {
    match (requested, registered) {
        (_, Some(emu)) => binfmt_handler_status(binfmt_dir, emu.name()),
        // Requested but registration failed at boot: not a user choice to turn
        // emulation off, so callers must not retire mirrors.
        (Some(req), None) => EmulationStatus::Unknown(format!(
            "amd64 emulation ({}) was requested but failed to register at boot",
            req.name()
        )),
        (None, None) => EmulationStatus::Disabled,
    }
}

/// Live state of the handler registered at boot. Only `Enabled` or `Unknown`:
/// a handler that was registered at boot and is now missing or disabled
/// (e.g. removed by a `binfmt --uninstall` / `qemu-user-static --reset`
/// container) is not the user turning emulation off in katamaran's config, so
/// callers must not treat it as `Disabled` (which retires mirrors).
fn binfmt_handler_status(binfmt_dir: &Path, name: &str) -> EmulationStatus {
    let read =
        |path: &Path| fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()));
    let status_path = binfmt_dir.join("status");
    let global = match read(&status_path) {
        Ok(s) => s,
        Err(e) => return EmulationStatus::Unknown(e),
    };
    if global.trim() != "enabled" {
        return EmulationStatus::Unknown(format!(
            "{} reports {:?} although {name} was registered at boot",
            status_path.display(),
            global.trim()
        ));
    }
    let handler_path = binfmt_dir.join(name);
    let handler = match read(&handler_path) {
        Ok(s) => s,
        Err(e) => return EmulationStatus::Unknown(e),
    };
    match handler.lines().next().map(str::trim) {
        Some("enabled") => EmulationStatus::Enabled,
        other => EmulationStatus::Unknown(format!(
            "{} reports {:?} although {name} was registered at boot",
            handler_path.display(),
            other
        )),
    }
}

/// `:name:M:offset:magic:mask:interpreter:flags`, with magic and mask
/// hex-escaped so no byte can be read as a separator.
fn registration(name: &str, interpreter: &str) -> String {
    format!(
        ":{name}:M:0:{}:{}:{interpreter}:{FLAGS}",
        escape(MAGIC),
        escape(MASK)
    )
}

fn escape(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("\\x{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_emulator_from_the_cmdline() {
        assert_eq!(Emulator::from_cmdline("console=hvc0 quiet"), None);
        assert_eq!(
            Emulator::from_cmdline("a kat.amd64=rosetta b"),
            Some(Emulator::Rosetta)
        );
        assert_eq!(
            Emulator::from_cmdline("kat.amd64=rosetta kat.amd64=qemu\n"),
            Some(Emulator::Qemu)
        );
        assert_eq!(Emulator::from_cmdline("kat.amd64=off"), None);
        let nowhere = Path::new("/nonexistent-binfmt");
        assert_eq!(status_from(None, None, nowhere), EmulationStatus::Disabled);
        assert!(matches!(
            status_from(Some(Emulator::Rosetta), None, nowhere),
            EmulationStatus::Unknown(_)
        ));
    }

    #[test]
    fn registers_x86_64_elf_with_the_fix_binary_flag() {
        let r = registration("rosetta", "/run/rosetta/rosetta");
        assert!(r.starts_with(":rosetta:M:0:\\x7f\\x45\\x4c\\x46\\x02"));
        assert!(r.ends_with(":/run/rosetta/rosetta:POCF"));
        let fields: Vec<&str> = r.split(':').collect();
        assert_eq!(fields.len(), 8);
        assert_eq!(fields[4].len(), 20 * 4);
        assert_eq!(fields[5].len(), 20 * 4);
        assert_eq!(MAGIC.len(), MASK.len());
        assert!(MAGIC.iter().zip(MASK).all(|(m, k)| m & k == *m));
    }

    #[test]
    fn checks_binfmt_handler_and_global_status() {
        let dir = std::env::temp_dir().join(format!("kat-amd64-binfmt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Missing status and handler files after registration -> Unknown.
        assert!(matches!(
            binfmt_handler_status(&dir, "rosetta"),
            EmulationStatus::Unknown(_)
        ));

        // Enabled global status and handler -> Enabled.
        fs::write(dir.join("status"), "enabled\n").unwrap();
        fs::write(
            dir.join("rosetta"),
            "enabled\ninterpreter /run/rosetta/rosetta\nflags: POCF\n",
        )
        .unwrap();
        assert_eq!(
            binfmt_handler_status(&dir, "rosetta"),
            EmulationStatus::Enabled
        );
        assert!(matches!(
            binfmt_handler_status(&dir, "qemu-x86_64"),
            EmulationStatus::Unknown(_)
        ));

        // Handler disabled after registration -> Unknown (never Disabled).
        fs::write(
            dir.join("rosetta"),
            "disabled\ninterpreter /run/rosetta/rosetta\n",
        )
        .unwrap();
        assert!(matches!(
            binfmt_handler_status(&dir, "rosetta"),
            EmulationStatus::Unknown(_)
        ));

        // Global binfmt_misc disabled -> Unknown even if handler says enabled.
        fs::write(dir.join("status"), "disabled\n").unwrap();
        fs::write(
            dir.join("rosetta"),
            "enabled\ninterpreter /run/rosetta/rosetta\n",
        )
        .unwrap();
        assert!(matches!(
            binfmt_handler_status(&dir, "rosetta"),
            EmulationStatus::Unknown(_)
        ));

        // Non-NotFound read failure (e.g. EISDIR) on handler -> Unknown (never Disabled).
        fs::write(dir.join("status"), "enabled\n").unwrap();
        fs::remove_file(dir.join("rosetta")).unwrap();
        fs::create_dir(dir.join("rosetta")).unwrap();
        assert!(matches!(
            binfmt_handler_status(&dir, "rosetta"),
            EmulationStatus::Unknown(_)
        ));

        // Non-NotFound read failure on global status -> Unknown (never Disabled).
        fs::remove_file(dir.join("status")).unwrap();
        fs::create_dir(dir.join("status")).unwrap();
        assert!(matches!(
            binfmt_handler_status(&dir, "rosetta"),
            EmulationStatus::Unknown(_)
        ));

        // Nothing registered in this test process -> Disabled.
        assert_eq!(status(), EmulationStatus::Disabled);

        let _ = fs::remove_dir_all(&dir);
    }
}
