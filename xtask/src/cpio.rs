//! Minimal writer for uncompressed `newc` cpio archives, the format the Linux
//! kernel unpacks as an initramfs.

const MAGIC: &str = "070701";
const S_IFDIR: u32 = 0o040000;
const S_IFREG: u32 = 0o100000;
const S_IFCHR: u32 = 0o020000;

pub struct Writer {
    buf: Vec<u8>,
    ino: u32,
}

impl Writer {
    pub fn new() -> Self {
        Writer {
            buf: Vec::new(),
            ino: 1,
        }
    }

    pub fn dir(&mut self, path: &str, perm: u32) {
        self.entry(path, S_IFDIR | perm, 2, (0, 0), &[]);
    }

    pub fn file(&mut self, path: &str, perm: u32, data: &[u8]) {
        self.entry(path, S_IFREG | perm, 1, (0, 0), data);
    }

    pub fn char_device(&mut self, path: &str, perm: u32, major: u32, minor: u32) {
        self.entry(path, S_IFCHR | perm, 1, (major, minor), &[]);
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.entry("TRAILER!!!", 0, 1, (0, 0), &[]);
        self.buf
    }

    fn entry(&mut self, path: &str, mode: u32, nlink: u32, rdev: (u32, u32), data: &[u8]) {
        let ino = if path == "TRAILER!!!" { 0 } else { self.ino };
        self.ino += 1;
        let name_len = path.len() + 1;
        let fields = [
            ino,
            mode,
            0,
            0,
            nlink,
            0,
            data.len() as u32,
            0,
            0,
            rdev.0,
            rdev.1,
            name_len as u32,
            0,
        ];
        self.buf.extend_from_slice(MAGIC.as_bytes());
        for f in fields {
            self.buf.extend_from_slice(format!("{f:08x}").as_bytes());
        }
        self.buf.extend_from_slice(path.as_bytes());
        self.buf.push(0);
        self.pad();
        self.buf.extend_from_slice(data);
        self.pad();
    }

    fn pad(&mut self) {
        while !self.buf.len().is_multiple_of(4) {
            self.buf.push(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(header: &[u8], i: usize) -> u32 {
        let s = std::str::from_utf8(&header[6 + i * 8..6 + (i + 1) * 8]).unwrap();
        u32::from_str_radix(s, 16).unwrap()
    }

    #[test]
    fn entries_are_aligned_and_ends_with_trailer() {
        let mut w = Writer::new();
        w.dir("dev", 0o755);
        w.file("init", 0o755, b"abc");
        let out = w.finish();
        assert_eq!(out.len() % 4, 0);
        assert!(out.starts_with(b"070701"));
        let trailer = b"TRAILER!!!\0";
        assert!(out.windows(trailer.len()).any(|w| w == trailer));
    }

    #[test]
    fn file_header_records_mode_size_and_name() {
        let mut w = Writer::new();
        w.file("init", 0o755, b"hello");
        let out = w.finish();
        assert_eq!(field(&out, 1), S_IFREG | 0o755);
        assert_eq!(field(&out, 6), 5);
        assert_eq!(field(&out, 11), 5);
        assert_eq!(&out[110..114], b"init");
        let data_at = (110 + 5usize).div_ceil(4) * 4;
        assert_eq!(&out[data_at..data_at + 5], b"hello");
    }

    #[test]
    fn char_device_records_rdev() {
        let mut w = Writer::new();
        w.char_device("dev/console", 0o600, 5, 1);
        let out = w.finish();
        assert_eq!(field(&out, 1), S_IFCHR | 0o600);
        assert_eq!((field(&out, 9), field(&out, 10)), (5, 1));
    }
}
