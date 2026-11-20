//! Vendor-specific hypervisor calls (SMCCC fast calls, owner 6), dispatched on
//! the calling vCPU thread to handlers that devices register.

use std::sync::{Arc, RwLock};

/// SMCCC return value for an unknown function ID.
pub const NOT_SUPPORTED: u64 = u64::MAX;

type Handler = Arc<dyn Fn([u64; 3]) -> u64 + Send + Sync>;

static HANDLERS: RwLock<Vec<(u64, Handler)>> = RwLock::new(Vec::new());

/// Register `handler` for `function_id`, replacing any previous handler.
pub fn register(function_id: u64, handler: impl Fn([u64; 3]) -> u64 + Send + Sync + 'static) {
    let mut handlers = HANDLERS.write().unwrap();
    handlers.retain(|(id, _)| *id != function_id);
    handlers.push((function_id, Arc::new(handler)));
}

pub fn unregister(function_id: u64) {
    HANDLERS
        .write()
        .unwrap()
        .retain(|(id, _)| *id != function_id);
}

/// Run the handler for `function_id` with the call's x1..x3 arguments.
pub fn dispatch(function_id: u64, args: [u64; 3]) -> Option<u64> {
    let handler = HANDLERS
        .read()
        .unwrap()
        .iter()
        .find(|(id, _)| *id == function_id)
        .map(|(_, h)| h.clone())?;
    Some(handler(args))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatches_registered_handlers_only() {
        register(0xC600_4BF0, |a| a[0] + a[1]);
        assert_eq!(dispatch(0xC600_4BF0, [2, 3, 0]), Some(5));
        register(0xC600_4BF0, |_| 7);
        assert_eq!(dispatch(0xC600_4BF0, [2, 3, 0]), Some(7));
        unregister(0xC600_4BF0);
        assert_eq!(dispatch(0xC600_4BF0, [0; 3]), None);
    }
}
