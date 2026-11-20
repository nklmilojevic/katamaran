//! The guest's wall clock follows the host's `SetTime`.
//!
//! The guest's clocks run on the hardware counter, which stops while the Mac
//! sleeps, so after a wake the guest is behind by the time asleep. The host
//! sends `SetTime` at connect, on wake, when it detects drift and every few
//! minutes. Large offsets are stepped; small ones are slewed, so the clock
//! never jumps back by a few milliseconds under a running program.

use std::io;

/// Offsets at least this large are stepped.
const STEP_NS: i64 = 50_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Correction {
    None,
    /// Slew by this many microseconds (`adjtime`).
    Slew(i64),
    Step,
}

pub fn correction(offset_ns: i64) -> Correction {
    if offset_ns.unsigned_abs() >= STEP_NS as u64 {
        Correction::Step
    } else if offset_ns.abs() >= 1_000 {
        Correction::Slew(offset_ns / 1_000)
    } else {
        Correction::None
    }
}

/// Bring `CLOCK_REALTIME` to `unix_ns`. Returns the offset that was
/// corrected (host minus guest).
pub fn sync(unix_ns: u64) -> io::Result<i64> {
    let offset = unix_ns as i128 - now_ns() as i128;
    let offset = offset.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
    match correction(offset) {
        Correction::None => {}
        Correction::Slew(us) => slew(us)?,
        Correction::Step => {
            super::sys::set_realtime(unix_ns)?;
            if offset.unsigned_abs() >= 1_000_000_000 {
                super::log(&format!("clock stepped by {:.3} s", offset as f64 / 1e9));
            }
        }
    }
    Ok(offset)
}

fn now_ns() -> u64 {
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn slew(us: i64) -> io::Result<()> {
    let mut tx: libc::timex = unsafe { std::mem::zeroed() };
    tx.modes = libc::ADJ_OFFSET_SINGLESHOT;
    tx.offset = us as _;
    if unsafe { libc::adjtimex(&mut tx) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_large_offsets_and_slews_small_ones() {
        assert_eq!(correction(0), Correction::None);
        assert_eq!(correction(999), Correction::None);
        assert_eq!(correction(-2_500_000), Correction::Slew(-2_500));
        assert_eq!(correction(49_999_999), Correction::Slew(49_999));
        assert_eq!(correction(50_000_000), Correction::Step);
        assert_eq!(correction(-3_600_000_000_000), Correction::Step);
        assert_eq!(correction(i64::MIN), Correction::Step);
    }
}
