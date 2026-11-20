//! The ioctls Rosetta for Linux makes on its own binary (`/proc/self/exe`)
//! to check that it runs in a Rosetta-enabled VM. Virtualization.framework's
//! Rosetta share answers them; a share of the RosettaLinux directory with this
//! layer answers them the same way.

use std::fs;
use std::io;
use std::path::Path;

/// `_IOC_READ`, type `'a'`: the key check (`_IOR('a', nr, char[69])`), with
/// the number varying across Rosetta versions.
const KEY_CMD_MASK: u32 = 0xffff_ff00;
const KEY_CMD: u32 = 0x8045_6100;
/// `_IOR('a', 0x23, char[128])`: where to reach the AOT cache daemon.
const AOT_CONFIG: u32 = 0x8080_6123;
/// `_IO('a', 0x24)`: Rosetta asks the host for TSO when the guest kernel
/// cannot switch the memory model itself.
const TSO_FALLBACK: u32 = 0x6124;

const AOT_CONFIG_LEN: usize = 128;
/// The key is a NUL-terminated string embedded in the Rosetta binary, which
/// compares the ioctl's answer with it.
const KEY_PREFIX: &[u8] = b"Our hard work\n";
const KEY_LEN: usize = 69;

pub struct Rosetta {
    key: Vec<u8>,
}

impl Rosetta {
    /// Reads the key from `dir/rosetta`, the binary the guest will run.
    pub fn load(dir: &Path) -> io::Result<Self> {
        Self::from_binary(&fs::read(dir.join("rosetta"))?)
    }

    fn from_binary(binary: &[u8]) -> io::Result<Self> {
        binary
            .windows(KEY_PREFIX.len())
            .position(|w| w == KEY_PREFIX)
            .and_then(|at| binary.get(at..at + KEY_LEN))
            .filter(|key| key.last() == Some(&0))
            .map(|key| Self { key: key.to_vec() })
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no Rosetta key"))
    }

    /// The answer to `cmd`, or `None` when it is not a Rosetta ioctl.
    pub fn ioctl(&self, cmd: u32, out_size: u32) -> Option<io::Result<Vec<u8>>> {
        let out_size = out_size as usize;
        match cmd {
            AOT_CONFIG => Some(Ok(vec![0; AOT_CONFIG_LEN.min(out_size)])),
            TSO_FALLBACK => Some(Ok(Vec::new())),
            c if c & KEY_CMD_MASK == KEY_CMD => Some(if out_size <= self.key.len() {
                Ok(self.key[..out_size].to_vec())
            } else {
                Err(io::Error::from_raw_os_error(libc::ENOTTY))
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binary() -> Vec<u8> {
        let mut b = b"\x7fELF padding ".to_vec();
        b.extend_from_slice(KEY_PREFIX);
        b.extend(std::iter::repeat_n(b'k', KEY_LEN - KEY_PREFIX.len() - 1));
        b.push(0);
        b.extend_from_slice(b"trailing");
        b
    }

    #[test]
    fn finds_the_key_in_the_binary() {
        let r = Rosetta::from_binary(&binary()).unwrap();
        assert_eq!(r.key.len(), KEY_LEN);
        assert!(r.key.starts_with(KEY_PREFIX));
        assert_eq!(r.key[KEY_LEN - 1], 0);
    }

    #[test]
    fn rejects_a_binary_without_a_terminated_key() {
        assert!(Rosetta::from_binary(b"no key here").is_err());
        let mut b = binary();
        let at = b.iter().rposition(|&c| c == 0).unwrap();
        b[at] = b'x';
        assert!(Rosetta::from_binary(&b).is_err());
    }

    #[test]
    fn answers_rosetta_ioctls_only() {
        let r = Rosetta::from_binary(&binary()).unwrap();
        for nr in [0x22, 0x25] {
            let got = r.ioctl(KEY_CMD | nr, KEY_LEN as u32).unwrap().unwrap();
            assert_eq!(got, r.key);
        }
        assert!(r.ioctl(KEY_CMD | 0x22, 4096).unwrap().is_err());
        assert_eq!(
            r.ioctl(AOT_CONFIG, 128).unwrap().unwrap(),
            vec![0; AOT_CONFIG_LEN]
        );
        assert!(r.ioctl(TSO_FALLBACK, 0).unwrap().unwrap().is_empty());
        assert!(r.ioctl(0x7602, 0).is_none());
        assert!(r.ioctl(0x8045_6200, KEY_LEN as u32).is_none());
    }
}
