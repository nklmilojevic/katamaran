//! The VM lifecycle as the daemon sees it.
//!
//! ```text
//!   stopped ──start──▶ starting ──ready──▶ running
//!      ▲                  │                  │
//!      │                stop               stop
//!      │                  ▼                  ▼
//!      └──exited ok── stopping ◀─────────────┘
//!   error ◀── exited with an error, from starting, running or stopping
//!   error ──start──▶ starting
//! ```

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VmState {
    Stopped,
    Starting,
    Running,
    Stopping,
    /// The last run failed; the VM is not running.
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Start,
    /// Docker is up (or failed for good while the VM stays up).
    Ready,
    Stop,
    /// The VM's supervisor returned: `ok` after a requested shutdown.
    Exited {
        ok: bool,
    },
    /// The VM died and is being started again.
    Restarting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transition {
    To(VmState),
    /// Already where the trigger leads (start while running, stop while stopped).
    Unchanged,
    Invalid(String),
}

impl VmState {
    pub fn on(self, t: Trigger) -> Transition {
        use Transition::*;
        use VmState::*;
        match (self, t) {
            (Stopped | Error, Trigger::Start) => To(Starting),
            (Starting | Running, Trigger::Start) => Unchanged,
            (Stopping, Trigger::Start) => Invalid("the VM is stopping; try again shortly".into()),
            (Starting, Trigger::Ready) => To(Running),
            (Running, Trigger::Ready) => Unchanged,
            (Starting | Running, Trigger::Restarting) => To(Starting),
            // A stop is already on its way; the restart will not happen.
            (Stopping, Trigger::Restarting) => Unchanged,
            (Starting | Running, Trigger::Stop) => To(Stopping),
            (Stopping | Stopped | Error, Trigger::Stop) => Unchanged,
            (Starting | Running | Stopping, Trigger::Exited { ok: true }) => To(Stopped),
            (Starting | Running | Stopping, Trigger::Exited { ok: false }) => To(Error),
            (s, t) => Invalid(format!("{t:?} while {}", s.as_str())),
        }
    }

    /// The VM process exists in these states.
    pub fn is_active(self) -> bool {
        matches!(
            self,
            VmState::Starting | VmState::Running | VmState::Stopping
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            VmState::Stopped => "stopped",
            VmState::Starting => "starting",
            VmState::Running => "running",
            VmState::Stopping => "stopping",
            VmState::Error => "error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Transition::*;
    use super::VmState::*;
    use super::*;

    #[test]
    fn normal_lifecycle() {
        assert_eq!(Stopped.on(Trigger::Start), To(Starting));
        assert_eq!(Starting.on(Trigger::Ready), To(Running));
        assert_eq!(Running.on(Trigger::Stop), To(Stopping));
        assert_eq!(Stopping.on(Trigger::Exited { ok: true }), To(Stopped));
    }

    #[test]
    fn a_restarting_vm_is_starting_again() {
        assert_eq!(Running.on(Trigger::Restarting), To(Starting));
        assert_eq!(Starting.on(Trigger::Restarting), To(Starting));
        assert_eq!(Stopping.on(Trigger::Restarting), Unchanged);
        assert!(matches!(Stopped.on(Trigger::Restarting), Invalid(_)));
    }

    #[test]
    fn repeated_requests_are_no_ops() {
        assert_eq!(Starting.on(Trigger::Start), Unchanged);
        assert_eq!(Running.on(Trigger::Start), Unchanged);
        assert_eq!(Stopped.on(Trigger::Stop), Unchanged);
        assert_eq!(Stopping.on(Trigger::Stop), Unchanged);
        assert_eq!(Error.on(Trigger::Stop), Unchanged);
    }

    #[test]
    fn failures_and_recovery() {
        assert_eq!(Starting.on(Trigger::Exited { ok: false }), To(Error));
        assert_eq!(Running.on(Trigger::Exited { ok: false }), To(Error));
        assert_eq!(Stopping.on(Trigger::Exited { ok: false }), To(Error));
        assert_eq!(Error.on(Trigger::Start), To(Starting));
        // Stop during boot is honoured once the guest is up.
        assert_eq!(Starting.on(Trigger::Stop), To(Stopping));
    }

    #[test]
    fn invalid_transitions() {
        assert!(matches!(Stopping.on(Trigger::Start), Invalid(_)));
        assert!(matches!(Stopped.on(Trigger::Ready), Invalid(_)));
        assert!(matches!(
            Stopped.on(Trigger::Exited { ok: true }),
            Invalid(_)
        ));
        assert!(matches!(Stopping.on(Trigger::Ready), Invalid(_)));
    }

    #[test]
    fn serializes_lowercase() {
        assert_eq!(serde_json::to_string(&Running).unwrap(), "\"running\"");
        assert_eq!(serde_json::from_str::<VmState>("\"error\"").unwrap(), Error);
        for s in [Stopped, Starting, Running, Stopping, Error] {
            assert_eq!(
                serde_json::to_string(&s).unwrap(),
                format!("\"{}\"", s.as_str())
            );
            assert_eq!(s.is_active(), matches!(s, Starting | Running | Stopping));
        }
    }
}
