//! macOS sleep and wake notifications (IOKit `IORegisterForSystemPower`),
//! which work from a process without a GUI or an NSApplication.
//!
//! The notifications arrive on a CFRunLoop on a thread of their own. Sleep
//! waits for the handler: the system goes to sleep only after
//! `IOAllowPowerChange`, which is called once the handler returns, so the
//! handler can pause the VM first. Constants and signatures are from the SDK
//! headers `IOKit/IOMessage.h` and `IOKit/pwr_mgt/IOPMLib.h`.

use std::ffi::c_void;
use std::sync::mpsc;
use std::thread;

use anyhow::bail;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerEvent {
    /// The Mac is about to sleep.
    WillSleep,
    /// The Mac woke and is running again.
    Woke,
}

// iokit_common_msg(x) = sys_iokit (0x38 << 26) | sub_iokit_common (0) | x
const IOKIT_COMMON: u32 = 0x38 << 26;
const CAN_SYSTEM_SLEEP: u32 = IOKIT_COMMON | 0x270;
const SYSTEM_WILL_SLEEP: u32 = IOKIT_COMMON | 0x280;
const SYSTEM_HAS_POWERED_ON: u32 = IOKIT_COMMON | 0x300;

/// What a power message means for the VM, and whether IOKit waits for an
/// acknowledgement.
fn classify(message: u32) -> (Option<PowerEvent>, bool) {
    match message {
        // Idle sleep may still be vetoed; nothing to do yet.
        CAN_SYSTEM_SLEEP => (None, true),
        SYSTEM_WILL_SLEEP => (Some(PowerEvent::WillSleep), true),
        SYSTEM_HAS_POWERED_ON => (Some(PowerEvent::Woke), false),
        _ => (None, false),
    }
}

type IoConnect = u32;
type IoObject = u32;
type Callback = extern "C" fn(*mut c_void, IoObject, u32, *mut c_void);

#[link(name = "IOKit", kind = "framework")]
unsafe extern "C" {
    fn IORegisterForSystemPower(
        refcon: *mut c_void,
        port: *mut *mut c_void,
        callback: Callback,
        notifier: *mut IoObject,
    ) -> IoConnect;
    fn IOAllowPowerChange(kernel_port: IoConnect, notification_id: isize) -> i32;
    fn IONotificationPortGetRunLoopSource(port: *mut c_void) -> *mut c_void;
}

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    fn CFRunLoopGetCurrent() -> *mut c_void;
    fn CFRunLoopAddSource(rl: *mut c_void, source: *mut c_void, mode: *const c_void);
    fn CFRunLoopRun();
    static kCFRunLoopDefaultMode: *const c_void;
}

struct Context {
    root_port: IoConnect,
    handler: Box<dyn Fn(PowerEvent) + Send>,
}

extern "C" fn on_message(refcon: *mut c_void, _service: IoObject, message: u32, arg: *mut c_void) {
    // SAFETY: refcon is the Context leaked in `watch`, alive for the process.
    let ctx = unsafe { &*(refcon as *const Context) };
    let (event, ack) = classify(message);
    if let Some(event) = event {
        (ctx.handler)(event);
    }
    if ack {
        // SAFETY: the notification id is the message argument, as documented.
        unsafe { IOAllowPowerChange(ctx.root_port, arg as isize) };
    }
}

/// Call `handler` on every sleep and wake, from a dedicated thread, for the
/// life of the process. Sleep proceeds when the handler returns.
pub fn watch(handler: impl Fn(PowerEvent) + Send + 'static) -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel();
    let handler: Box<dyn Fn(PowerEvent) + Send> = Box::new(handler);
    thread::Builder::new().name("power".into()).spawn(move || {
        let ctx = Box::into_raw(Box::new(Context {
            root_port: 0,
            handler,
        }));
        let mut port = std::ptr::null_mut();
        let mut notifier = 0;
        // SAFETY: out-pointers are valid; ctx stays alive (leaked) for
        // the callback, which only runs on this thread's run loop.
        let root =
            unsafe { IORegisterForSystemPower(ctx.cast(), &mut port, on_message, &mut notifier) };
        if root == 0 {
            let _ = tx.send(false);
            return;
        }
        unsafe {
            (*ctx).root_port = root;
            CFRunLoopAddSource(
                CFRunLoopGetCurrent(),
                IONotificationPortGetRunLoopSource(port),
                kCFRunLoopDefaultMode,
            );
        }
        let _ = tx.send(true);
        unsafe { CFRunLoopRun() };
    })?;
    if !rx.recv().unwrap_or(false) {
        bail!("IORegisterForSystemPower failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_values_match_iokit() {
        assert_eq!(CAN_SYSTEM_SLEEP, 0xE000_0270);
        assert_eq!(SYSTEM_WILL_SLEEP, 0xE000_0280);
        assert_eq!(SYSTEM_HAS_POWERED_ON, 0xE000_0300);
    }

    #[test]
    fn sleep_needs_an_ack_and_wake_does_not() {
        assert_eq!(classify(CAN_SYSTEM_SLEEP), (None, true));
        assert_eq!(
            classify(SYSTEM_WILL_SLEEP),
            (Some(PowerEvent::WillSleep), true)
        );
        assert_eq!(
            classify(SYSTEM_HAS_POWERED_ON),
            (Some(PowerEvent::Woke), false)
        );
        // kIOMessageSystemWillPowerOn: too early, devices are still off.
        assert_eq!(classify(IOKIT_COMMON | 0x320), (None, false));
    }

    #[test]
    fn registers_for_notifications() {
        watch(|_| {}).unwrap();
    }
}
