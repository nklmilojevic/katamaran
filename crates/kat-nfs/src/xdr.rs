//! XDR (RFC 4506): big-endian 32-bit units, opaque data padded to 4 bytes.

/// The arguments did not decode.
#[derive(Debug, PartialEq, Eq)]
pub struct Garbage;

pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Garbage> {
        if self.buf.len() < n {
            return Err(Garbage);
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    pub fn u32(&mut self) -> Result<u32, Garbage> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Result<u64, Garbage> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn bool(&mut self) -> Result<bool, Garbage> {
        match self.u32()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Garbage),
        }
    }

    pub fn fixed(&mut self, n: usize) -> Result<&'a [u8], Garbage> {
        let data = self.take(n)?;
        self.take(pad(n))?;
        Ok(data)
    }

    /// Variable-length opaque or string of at most `max` bytes.
    pub fn opaque(&mut self, max: usize) -> Result<&'a [u8], Garbage> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(Garbage);
        }
        self.fixed(n)
    }

    pub fn remaining(&self) -> usize {
        self.buf.len()
    }
}

fn pad(n: usize) -> usize {
    (4 - n % 4) % 4
}

#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn with_capacity(n: usize) -> Self {
        Writer {
            buf: Vec::with_capacity(n),
        }
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u32(v as u32)
    }

    pub fn fixed(&mut self, data: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(data);
        self.buf.extend_from_slice(&[0u8; 3][..pad(data.len())]);
        self
    }

    pub fn opaque(&mut self, data: &[u8]) -> &mut Self {
        self.u32(data.len() as u32);
        self.fixed(data)
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_pads() {
        let mut w = Writer::default();
        w.u32(7)
            .u64(u64::MAX - 1)
            .bool(true)
            .opaque(b"abcde")
            .fixed(b"xy");
        assert_eq!(w.len(), 4 + 8 + 4 + (4 + 8) + 4);
        let mut r = Reader::new(&w.buf);
        assert_eq!(r.u32(), Ok(7));
        assert_eq!(r.u64(), Ok(u64::MAX - 1));
        assert_eq!(r.bool(), Ok(true));
        assert_eq!(r.opaque(16), Ok(&b"abcde"[..]));
        assert_eq!(r.fixed(2), Ok(&b"xy"[..]));
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn rejects_short_oversized_and_bad_bools() {
        assert_eq!(Reader::new(&[0, 0, 1]).u32(), Err(Garbage));
        let mut w = Writer::default();
        w.opaque(b"too long");
        assert_eq!(Reader::new(&w.buf).opaque(4), Err(Garbage));
        assert_eq!(Reader::new(&[0, 0, 0, 2]).bool(), Err(Garbage));
        // Length says 5 but the padding is missing.
        assert_eq!(
            Reader::new(&[0, 0, 0, 5, 1, 2, 3, 4, 5]).opaque(8),
            Err(Garbage)
        );
    }
}
