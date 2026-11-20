//! Per-task memory model. x86 code translated by Rosetta assumes total store
//! ordering; Apple cores provide it through ACTLR_EL1.EnTSO, which
//! Hypervisor.framework exposes per vCPU. A guest kernel that tracks the model
//! per task makes this vendor call on the vCPU it switches, so only Rosetta's
//! threads pay for TSO.

use std::env;
use std::sync::LazyLock;

use crate::bindings::{HV_SUCCESS, hv_sys_reg_t_HV_SYS_REG_ACTLR_EL1, hv_vcpu_set_sys_reg};
use crate::vendor_call::NOT_SUPPORTED;

/// `x1` = 1 selects TSO, 0 the default arm64 model. Returns 0, or
/// [`NOT_SUPPORTED`] when the host cannot switch.
pub const SET_MEM_MODEL: u64 = 0xC600_4B10;

const ACTLR_EL1_EN_TSO: u64 = 1 << 1;

static ENABLED: LazyLock<bool> = LazyLock::new(|| env::var("KATAMARAN_TSO").as_deref() != Ok("0"));

/// Must run on the thread that owns `vcpuid`.
pub fn set_mem_model(vcpuid: u64, model: u64) -> u64 {
    if !*ENABLED || model > 1 {
        return NOT_SUPPORTED;
    }
    let actlr = if model == 1 { ACTLR_EL1_EN_TSO } else { 0 };
    // SAFETY: plain register write on the calling thread's own vCPU.
    let ret = unsafe { hv_vcpu_set_sys_reg(vcpuid, hv_sys_reg_t_HV_SYS_REG_ACTLR_EL1, actlr) };
    if ret == HV_SUCCESS { 0 } else { NOT_SUPPORTED }
}
