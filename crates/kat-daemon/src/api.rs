//! The daemon's control API: JSON-RPC 2.0 over `<home>/run/daemon.sock`.
//!
//! Framing: one JSON object per line (`\n`-terminated UTF-8, at most
//! [`MAX_LINE`] bytes) in each direction. A client may send any number of
//! requests on one connection; each gets one response with the same `id`,
//! in order. `logs` with `"follow": true` is the exception: after its
//! response the connection carries only `logs.data` notifications (no `id`)
//! until the client closes it.
//!
//! | method            | params                                                     | result         |
//! |-------------------|------------------------------------------------------------|----------------|
//! | `status`          | –                                                          | [`Status`]     |
//! | `start`           | –                                                          | [`Status`]     |
//! | `stop`            | `{"wait": bool}` (default false)                           | [`Status`]     |
//! | `restart`         | –                                                          | [`Status`]     |
//! | `logs`            | [`LogsParams`]                                             | [`LogChunk`]   |
//! | `config.get`      | –                                                          | [`ConfigView`] |
//! | `config.set`      | `{"values": {"cpus": 2, "mount": false, ...}}`             | [`ConfigView`] |
//! | `stats`           | –                                                          | [`StatsResult`]|
//! | `version`         | –                                                          | [`Versions`]   |
//! | `daemon.shutdown` | –                                                          | `null`, then the daemon stops the VM and exits |
//! | `machine.list`    | –                                                          | `[Machine]`    |
//! | `machine.info`    | `{"name": "dev"}` (empty or absent: the default machine)   | `Machine`      |
//! | `machine.create`  | [`MachineCreate`]                                          | `Machine`, after `machine.progress` notifications |
//! | `machine.start`, `machine.stop`, `machine.delete`, `machine.default` | `{"name": "dev"}` | `null` |
//!
//! Machines need the VM running; `machine.create` sends `machine.progress`
//! notifications (`{"message": "..."}`) on its connection before the
//! response. A `Machine` is [`crate::machines::Machine`].
//!
//! `start` returns as soon as the VM is starting; poll `status` until
//! `docker.state` is `ready`. `stop` with `wait` returns once the VM is gone.
//! Errors use the JSON-RPC codes below; [`FAILED`] means the request was
//! understood but cannot be carried out (for example `start` while stopping).
//!
//! ```text
//! → {"jsonrpc":"2.0","id":1,"method":"status"}
//! ← {"jsonrpc":"2.0","id":1,"result":{"state":"running",...}}
//! ```

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{Config, WireConfig};
use crate::state::VmState;

pub const SOCKET_NAME: &str = "daemon.sock";
pub const MAX_LINE: usize = 1 << 20;
pub const VERSION: &str = "2.0";

pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;
pub const FAILED: i64 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> RpcError {
        RpcError {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RpcError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notification {
    pub jsonrpc: String,
    pub method: String,
    pub params: Value,
}

impl Response {
    pub fn ok(id: Value, result: Value) -> Response {
        Response {
            jsonrpc: VERSION.into(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: Value, error: RpcError) -> Response {
        Response {
            jsonrpc: VERSION.into(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

impl Request {
    pub fn new(id: u64, call: &Call) -> Request {
        Request {
            jsonrpc: VERSION.into(),
            id: Some(id.into()),
            method: call.method().into(),
            params: call.params(),
        }
    }
}

/// A parsed request.
#[derive(Debug, Clone, PartialEq)]
pub enum Call {
    Status,
    Start,
    Stop { wait: bool },
    Restart,
    Logs(LogsParams),
    ConfigGet,
    ConfigSet(BTreeMap<String, String>),
    Stats,
    Version,
    Shutdown,
    MachineList,
    MachineInfo { name: String },
    MachineCreate(MachineCreate),
    MachineStart { name: String },
    MachineStop { name: String },
    MachineDelete { name: String },
    MachineDefault { name: String },
    K8sStart,
    K8sStop,
    K8sReset { images: bool, progress: bool },
    K8sKubeconfig,
    K8sSyncImages,
    K8sMarkVersion { version: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCreate {
    pub name: String,
    /// ubuntu, debian, alpine, fedora, arch, … (images.linuxcontainers.org).
    pub distro: String,
    /// The distro's release (`noble`, `3.24`); default: a current one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<String>,
}

impl Call {
    pub fn method(&self) -> &'static str {
        match self {
            Call::Status => "status",
            Call::Start => "start",
            Call::Stop { .. } => "stop",
            Call::Restart => "restart",
            Call::Logs(_) => "logs",
            Call::ConfigGet => "config.get",
            Call::ConfigSet(_) => "config.set",
            Call::Stats => "stats",
            Call::Version => "version",
            Call::Shutdown => "daemon.shutdown",
            Call::MachineList => "machine.list",
            Call::MachineInfo { .. } => "machine.info",
            Call::MachineCreate(_) => "machine.create",
            Call::MachineStart { .. } => "machine.start",
            Call::MachineStop { .. } => "machine.stop",
            Call::MachineDelete { .. } => "machine.delete",
            Call::MachineDefault { .. } => "machine.default",
            Call::K8sStart => "k8s.start",
            Call::K8sStop => "k8s.stop",
            Call::K8sReset { images: false, .. } => "k8s.reset",
            Call::K8sReset { images: true, .. } => "k8s.reset_images",
            Call::K8sKubeconfig => "k8s.kubeconfig",
            Call::K8sSyncImages => "k8s.sync_images",
            Call::K8sMarkVersion { .. } => "k8s.mark_version",
        }
    }

    pub fn params(&self) -> Value {
        match self {
            Call::Stop { wait } => serde_json::json!({ "wait": wait }),
            Call::Logs(p) => serde_json::to_value(p).unwrap_or(Value::Null),
            Call::ConfigSet(values) => serde_json::json!({ "values": values }),
            Call::MachineCreate(c) => serde_json::to_value(c).unwrap_or(Value::Null),
            Call::MachineInfo { name }
            | Call::MachineStart { name }
            | Call::MachineStop { name }
            | Call::MachineDelete { name }
            | Call::MachineDefault { name } => serde_json::json!({ "name": name }),
            Call::K8sMarkVersion { version } => serde_json::json!({ "version": version }),
            Call::K8sReset {
                images: false,
                progress: true,
            } => serde_json::json!({ "progress": true }),
            _ => Value::Null,
        }
    }

    pub fn parse(req: &Request) -> Result<Call, RpcError> {
        if req.jsonrpc != VERSION {
            return Err(RpcError::new(INVALID_REQUEST, "jsonrpc must be \"2.0\""));
        }
        let p = &req.params;
        let bad = |e: serde_json::Error| RpcError::new(INVALID_PARAMS, e.to_string());
        Ok(match req.method.as_str() {
            "status" => Call::Status,
            "start" => Call::Start,
            "stop" => {
                #[derive(Deserialize, Default)]
                #[serde(default, deny_unknown_fields)]
                struct P {
                    wait: bool,
                }
                let P { wait } = if p.is_null() {
                    P::default()
                } else {
                    serde_json::from_value(p.clone()).map_err(bad)?
                };
                Call::Stop { wait }
            }
            "restart" => Call::Restart,
            "logs" => Call::Logs(if p.is_null() {
                LogsParams::default()
            } else {
                serde_json::from_value(p.clone()).map_err(bad)?
            }),
            "config.get" => Call::ConfigGet,
            "config.set" => {
                let values = p
                    .get("values")
                    .and_then(Value::as_object)
                    .ok_or_else(|| RpcError::new(INVALID_PARAMS, "expected {\"values\": {...}}"))?;
                let mut out = BTreeMap::new();
                for (k, v) in values {
                    let text = match v {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        Value::Bool(b) => b.to_string(),
                        Value::Null => String::new(),
                        Value::Array(items) if k == "forward_addresses" => {
                            if items.is_empty() {
                                return Err(RpcError::new(
                                    INVALID_PARAMS,
                                    format!("{k}: expected a non-empty array of IPv4 addresses"),
                                ));
                            }
                            let mut parts = Vec::with_capacity(items.len());
                            for item in items {
                                match item {
                                    Value::String(s) => {
                                        let addr: Ipv4Addr = s.trim().parse().map_err(|_| {
                                            RpcError::new(
                                                INVALID_PARAMS,
                                                format!("{k}: {s:?} is not an IPv4 address"),
                                            )
                                        })?;
                                        parts.push(addr.to_string());
                                    }
                                    _ => {
                                        return Err(RpcError::new(
                                            INVALID_PARAMS,
                                            format!("{k}: expected an array of IPv4 addresses"),
                                        ));
                                    }
                                }
                            }
                            parts.join(",")
                        }
                        _ => {
                            return Err(RpcError::new(
                                INVALID_PARAMS,
                                format!("{k}: expected a string, number or bool"),
                            ));
                        }
                    };
                    out.insert(k.clone(), text);
                }
                if out.is_empty() {
                    return Err(RpcError::new(INVALID_PARAMS, "no values to set"));
                }
                Call::ConfigSet(out)
            }
            "stats" => Call::Stats,
            "version" => Call::Version,
            "daemon.shutdown" => Call::Shutdown,
            "machine.list" => Call::MachineList,
            "machine.create" => {
                Call::MachineCreate(serde_json::from_value(p.clone()).map_err(bad)?)
            }
            m @ ("machine.info" | "machine.start" | "machine.stop" | "machine.delete"
            | "machine.default") => {
                #[derive(Deserialize, Default)]
                #[serde(default, deny_unknown_fields)]
                struct P {
                    name: String,
                }
                let P { name } = if p.is_null() {
                    P::default()
                } else {
                    serde_json::from_value(p.clone()).map_err(bad)?
                };
                match m {
                    "machine.info" => Call::MachineInfo { name },
                    "machine.start" => Call::MachineStart { name },
                    "machine.stop" => Call::MachineStop { name },
                    "machine.delete" => Call::MachineDelete { name },
                    _ => Call::MachineDefault { name },
                }
            }
            "k8s.start" => Call::K8sStart,
            "k8s.stop" => Call::K8sStop,
            "k8s.reset" => {
                #[derive(Deserialize, Default)]
                #[serde(default, deny_unknown_fields)]
                struct P {
                    progress: bool,
                }
                let P { progress } = if p.is_null() {
                    P::default()
                } else {
                    serde_json::from_value(p.clone()).map_err(bad)?
                };
                Call::K8sReset {
                    images: false,
                    progress,
                }
            }
            "k8s.reset_images" => {
                #[derive(Deserialize, Default)]
                #[serde(default, deny_unknown_fields)]
                struct Empty {}
                if !p.is_null() {
                    let _: Empty = serde_json::from_value(p.clone()).map_err(bad)?;
                }
                Call::K8sReset {
                    images: true,
                    progress: true,
                }
            }
            "k8s.kubeconfig" => Call::K8sKubeconfig,
            "k8s.sync_images" => Call::K8sSyncImages,
            "k8s.mark_version" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct P {
                    version: String,
                }
                let P { version } = serde_json::from_value(p.clone()).map_err(bad)?;
                Call::K8sMarkVersion { version }
            }
            other => {
                return Err(RpcError::new(
                    METHOD_NOT_FOUND,
                    format!("unknown method {other}"),
                ));
            }
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogSource {
    /// The daemon's own log (`<home>/logs/daemon.log`).
    #[default]
    Daemon,
    /// The guest's serial console (`<home>/logs/console.log`), new at every start.
    Console,
    /// The VM process (`<home>/run/vm.log`).
    Vm,
    /// The network stack (`<home>/logs/gvproxy.log`).
    Gvproxy,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogsParams {
    pub source: LogSource,
    /// Return the last `lines` lines (default 200). Ignored with `offset`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lines: Option<usize>,
    /// Return everything from this byte offset (for polling: pass back the
    /// `offset` of the previous chunk). A file that shrank starts over at 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    pub follow: bool,
}

/// A piece of a log: `data` ends at byte `offset` of `path`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogChunk {
    pub data: String,
    pub offset: u64,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigView {
    pub config: WireConfig,
    /// Saved data-disk ceiling in GiB (`64` when unset). Carried on the
    /// enclosing response rather than inside `config` so older CLIs (whose
    /// `Config` uses `deny_unknown_fields`) still decode `config.get` /
    /// `config.set` replies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_gib: Option<u32>,
    /// Explicitly configured `disk_gib` target from `config.toml`, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_target_gib: Option<u32>,
    pub path: PathBuf,
    /// The running VM was started with different settings.
    pub restart_required: bool,
    /// Whether this `config.set` call changed `kubernetes` (triggering an
    /// immediate start or stop on a running VM).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub k8s_toggled: bool,
}

impl ConfigView {
    /// Reconstruct the full [`Config`], combining `config` with `disk_target_gib`
    /// (falling back to `disk_gib` for replies from daemons without `disk_target_gib`).
    pub fn resolved_config(&self) -> Config {
        let explicit = self.disk_target_gib.or_else(|| {
            self.disk_gib
                .filter(|&d| d != crate::config::DEFAULT_DISK_GIB)
        });
        self.config.clone().into_config(explicit)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Versions {
    pub katamaran: String,
    pub protocol: u32,
    /// The guest kernel, while a VM is up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kernel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub state: VmState,
    /// Why the last run failed, or why Docker or the mount did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Unix time in ms when `state` was entered.
    pub since_ms: u64,
    /// Seconds since the guest came up, while it is up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uptime_s: Option<f64>,
    pub daemon: DaemonInfo,
    pub versions: Versions,
    /// The saved settings, used at the next start (legacy wire shape without `disk_gib`).
    pub config: WireConfig,
    /// Saved data-disk ceiling in GiB (`64` when unset). Carried on the
    /// enclosing response rather than inside `config` so older CLIs (whose
    /// `Config` uses `deny_unknown_fields`) still decode `status` replies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_gib: Option<u32>,
    /// Explicitly configured `disk_gib` target from `config.toml`, when set.
    /// Distinguishes an explicit `disk_gib = 64` growth request from an
    /// omitted default when the attached image is smaller than 64 GiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_target_gib: Option<u32>,
    /// The running VM's settings and timings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm: Option<VmInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Sample>,
    pub docker: DockerInfo,
    #[serde(default)]
    pub k8s: K8sStatus,
    pub mount: MountInfo,
    pub ports: Vec<Port>,
    /// Ports the guest tried to expose that could not be bound on macOS.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub port_errors: Vec<PortError>,
}

impl Status {
    /// Saved data-disk ceiling in GiB, falling back to the default for older daemons.
    pub fn saved_disk_gib(&self) -> u32 {
        self.disk_gib.unwrap_or(crate::config::DEFAULT_DISK_GIB)
    }

    /// Explicitly configured growth target in GiB, if any.
    pub fn explicit_disk_target_gib(&self) -> Option<u32> {
        self.disk_target_gib.or_else(|| {
            self.disk_gib
                .filter(|&d| d > crate::config::DEFAULT_DISK_GIB)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub pid: u32,
    pub home: PathBuf,
    pub started_ms: u64,
    /// The LaunchAgent label when launchd runs the daemon.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launchd_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VmInfo {
    pub cpus: u8,
    pub memory_mib: u32,
    /// Attached data-disk size in GiB (rounded up for display; may exceed the
    /// saved target if `data.img` was already grown larger).
    #[serde(default = "crate::config::default_disk_gib")]
    pub disk_gib: u32,
    /// Exact attached data-disk size in bytes, used for pending-growth checks
    /// so fractional images (such as 63.5 GiB -> 64 GiB) are not masked by
    /// GiB rounding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_bytes: Option<u64>,
    /// Requested `disk_gib` target from config when this VM started, kept
    /// separate from `disk_gib` so a retained larger image does not appear
    /// as perpetual pending work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_disk_gib: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Start request until the guest's hello.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_ms: Option<f64>,
    /// Start request until Docker answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_ready_ms: Option<f64>,
}

impl VmInfo {
    /// Exact attached data-disk size in bytes, falling back to `disk_gib` for
    /// older daemons that omit `disk_bytes`.
    pub fn attached_disk_bytes(&self) -> u64 {
        self.disk_bytes
            .unwrap_or_else(|| u64::from(self.disk_gib) << 30)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DockerState {
    Stopped,
    Starting,
    Ready,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DockerInfo {
    pub state: DockerState,
    pub socket: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct K8sStatus {
    pub enabled: bool,
    /// `stopped`, `starting`, `running`, `stopping`, or `failed`.
    pub state: String,
    pub installed: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_port: Option<u16>,
    /// Plain-HTTP Kubernetes API socket, while the cluster is running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_socket: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub context: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kubeconfig: Option<PathBuf>,
    #[serde(default)]
    pub dns_domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dns_port: Option<u16>,
    #[serde(default)]
    pub dns_installed: bool,
    #[serde(default)]
    pub dns_isolated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Default for K8sStatus {
    fn default() -> Self {
        K8sStatus {
            enabled: false,
            state: "stopped".into(),
            installed: false,
            version: String::new(),
            api_port: None,
            proxy_socket: None,
            context: String::new(),
            kubeconfig: None,
            dns_domain: String::new(),
            dns_port: None,
            dns_installed: false,
            dns_isolated: false,
            error: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MountInfo {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    pub mounted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Port {
    pub proto: String,
    pub port: u16,
    /// The Mac IPv4 addresses this port is actively bound to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<Ipv4Addr>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PortError {
    pub proto: String,
    pub port: u16,
    pub error: String,
}

/// One resource sample: the guest's own view plus the VM process on the Mac.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub t_ms: u64,
    /// Busy share of all guest CPUs, 0–100.
    pub guest_cpu_percent: f64,
    pub guest_mem_used_mib: u64,
    pub guest_mem_total_mib: u64,
    /// CPU time of the VM process, in percent of one Mac core.
    pub host_cpu_percent: f64,
    /// The VM process's physical footprint (what Activity Monitor shows).
    pub host_footprint_mib: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatsResult {
    pub interval_ms: u64,
    pub samples: Vec<Sample>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(method: &str, params: Value) -> Request {
        Request {
            jsonrpc: VERSION.into(),
            id: Some(json!(1)),
            method: method.into(),
            params,
        }
    }

    #[test]
    fn every_call_round_trips_through_a_request() {
        let mut values = BTreeMap::new();
        values.insert("cpus".to_string(), "2".to_string());
        let calls = [
            Call::Status,
            Call::Start,
            Call::Stop { wait: true },
            Call::Stop { wait: false },
            Call::Restart,
            Call::Logs(LogsParams {
                source: LogSource::Console,
                lines: Some(10),
                offset: None,
                follow: true,
            }),
            Call::ConfigGet,
            Call::ConfigSet(values),
            Call::Stats,
            Call::Version,
            Call::Shutdown,
            Call::MachineList,
            Call::MachineInfo { name: "".into() },
            Call::MachineCreate(MachineCreate {
                name: "dev".into(),
                distro: "alpine".into(),
                release: Some("3.24".into()),
            }),
            Call::MachineCreate(MachineCreate {
                name: "u".into(),
                distro: "ubuntu".into(),
                release: None,
            }),
            Call::MachineStart { name: "dev".into() },
            Call::MachineStop { name: "dev".into() },
            Call::MachineDelete { name: "dev".into() },
            Call::MachineDefault { name: "dev".into() },
            Call::K8sStart,
            Call::K8sStop,
            Call::K8sReset {
                images: false,
                progress: false,
            },
            Call::K8sReset {
                images: false,
                progress: true,
            },
            Call::K8sReset {
                images: true,
                progress: true,
            },
            Call::K8sKubeconfig,
            Call::K8sSyncImages,
            Call::K8sMarkVersion {
                version: "v1.37.2+k3s1".into(),
            },
        ];
        for (i, call) in calls.iter().enumerate() {
            let line = serde_json::to_string(&Request::new(i as u64, call)).unwrap();
            assert!(!line.contains('\n'));
            let back: Request = serde_json::from_str(&line).unwrap();
            assert_eq!(back.id, Some(json!(i)));
            assert_eq!(&Call::parse(&back).unwrap(), call, "{line}");
        }
    }

    #[test]
    fn parses_hand_written_requests() {
        let r: Request =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":"a","method":"status"}"#).unwrap();
        assert_eq!(Call::parse(&r).unwrap(), Call::Status);
        let r = req("logs", json!({"source": "console", "offset": 42}));
        assert_eq!(
            Call::parse(&r).unwrap(),
            Call::Logs(LogsParams {
                source: LogSource::Console,
                lines: None,
                offset: Some(42),
                follow: false,
            })
        );
        let r = req(
            "config.set",
            json!({"values": {
                "cpus": 2,
                "mount": false,
                "mount_path": "/tmp/x",
                "forward_addresses": ["127.0.0.1", "172.16.1.200"],
            }}),
        );
        let Call::ConfigSet(v) = Call::parse(&r).unwrap() else {
            panic!()
        };
        assert_eq!(v["cpus"], "2");
        assert_eq!(v["mount"], "false");
        assert_eq!(v["mount_path"], "/tmp/x");
        assert_eq!(v["forward_addresses"], "127.0.0.1,172.16.1.200");
        assert_eq!(
            Call::parse(&req("stop", Value::Null)).unwrap(),
            Call::Stop { wait: false }
        );
        assert_eq!(
            Call::parse(&req("k8s.reset", Value::Null)).unwrap(),
            Call::K8sReset {
                images: false,
                progress: false,
            }
        );
        assert_eq!(
            Call::parse(&req("k8s.reset", json!({}))).unwrap(),
            Call::K8sReset {
                images: false,
                progress: false,
            }
        );
        assert_eq!(
            Call::parse(&req("k8s.reset", json!({"progress": true}))).unwrap(),
            Call::K8sReset {
                images: false,
                progress: true,
            }
        );
        assert_eq!(
            Call::parse(&req("k8s.reset_images", Value::Null)).unwrap(),
            Call::K8sReset {
                images: true,
                progress: true,
            }
        );
        assert_eq!(
            Call::parse(&req("k8s.reset_images", json!({}))).unwrap(),
            Call::K8sReset {
                images: true,
                progress: true,
            }
        );
        assert_eq!(
            Request::new(
                1,
                &Call::K8sReset {
                    images: false,
                    progress: false,
                }
            )
            .method,
            "k8s.reset"
        );
        assert_eq!(
            Request::new(
                1,
                &Call::K8sReset {
                    images: true,
                    progress: true,
                }
            )
            .method,
            "k8s.reset_images"
        );
    }

    #[test]
    fn rejects_bad_requests() {
        let code = |r: Request| Call::parse(&r).unwrap_err().code;
        assert_eq!(code(req("reboot", Value::Null)), METHOD_NOT_FOUND);
        assert_eq!(code(req("stop", json!({"wait": "yes"}))), INVALID_PARAMS);
        assert_eq!(code(req("stop", json!({"force": true}))), INVALID_PARAMS);
        assert_eq!(
            code(req("k8s.reset", json!({"images": true}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("k8s.reset", json!({"progress": "yes"}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("k8s.reset", json!({"force": true}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("k8s.reset_images", json!({"progress": true}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("k8s.reset_images", json!({"force": true}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("logs", json!({"source": "kernel"}))),
            INVALID_PARAMS
        );
        assert_eq!(code(req("config.set", json!({"cpus": 2}))), INVALID_PARAMS);
        assert_eq!(
            code(req("config.set", json!({"values": {}}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("config.set", json!({"values": {"cpus": [1]}}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("config.set", json!({"values": {"cpus": ["2"]}}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("config.set", json!({"values": {"mount": ["false"]}}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req(
                "config.set",
                json!({"values": {"forward_addresses": []}})
            )),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req(
                "config.set",
                json!({"values": {"forward_addresses": [""]}})
            )),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req(
                "config.set",
                json!({"values": {"forward_addresses": [","]}})
            )),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req(
                "config.set",
                json!({"values": {"forward_addresses": ["127.0.0.1,172.16.1.200"]}})
            )),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req(
                "config.set",
                json!({"values": {"forward_addresses": ["127.0.0.1 172.16.1.200"]}})
            )),
            INVALID_PARAMS
        );
        assert_eq!(code(req("machine.create", Value::Null)), INVALID_PARAMS);
        assert_eq!(
            code(req("machine.create", json!({"name": "a"}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("machine.start", json!({"name": 1}))),
            INVALID_PARAMS
        );
        assert_eq!(
            code(req("machine.stop", json!({"name": "a", "force": true}))),
            INVALID_PARAMS
        );
        assert_eq!(
            Call::parse(&req("machine.info", Value::Null)).unwrap(),
            Call::MachineInfo { name: "".into() }
        );
        assert_eq!(code(req("k8s.mark_version", Value::Null)), INVALID_PARAMS);
        assert_eq!(
            code(req(
                "k8s.mark_version",
                json!({"version": "v1.37.2+k3s1", "extra": true})
            )),
            INVALID_PARAMS
        );
        let mut r = req("status", Value::Null);
        r.jsonrpc = "1.0".into();
        assert_eq!(code(r), INVALID_REQUEST);
    }

    #[test]
    fn responses_carry_either_result_or_error() {
        let ok = serde_json::to_string(&Response::ok(json!(3), json!({"a": 1}))).unwrap();
        assert_eq!(ok, r#"{"jsonrpc":"2.0","id":3,"result":{"a":1}}"#);
        let err =
            serde_json::to_string(&Response::err(json!(4), RpcError::new(FAILED, "no"))).unwrap();
        assert_eq!(
            err,
            r#"{"jsonrpc":"2.0","id":4,"error":{"code":1,"message":"no"}}"#
        );
        let back: Response = serde_json::from_str(&err).unwrap();
        assert_eq!(back.error.unwrap().message, "no");
        assert!(back.result.is_none());
    }

    #[test]
    fn status_round_trips() {
        let status = Status {
            state: VmState::Running,
            error: None,
            since_ms: 1,
            uptime_s: Some(2.5),
            daemon: DaemonInfo {
                pid: 7,
                home: "/tmp/h".into(),
                started_ms: 0,
                launchd_label: Some("dev.katamaran.daemon".into()),
            },
            versions: Versions {
                katamaran: "0.1.0".into(),
                protocol: 2,
                kernel: Some("6.18".into()),
            },
            config: WireConfig::default(),
            disk_gib: Some(crate::config::DEFAULT_DISK_GIB),
            disk_target_gib: None,
            vm: Some(VmInfo {
                cpus: 4,
                memory_mib: 4096,
                disk_gib: 64,
                disk_bytes: Some(64u64 << 30),
                requested_disk_gib: Some(64),
                pid: Some(9),
                boot_ms: Some(120.0),
                docker_ready_ms: Some(900.0),
            }),
            usage: Some(Sample::default()),
            docker: DockerInfo {
                state: DockerState::Ready,
                socket: "/tmp/h/run/docker.sock".into(),
            },
            k8s: K8sStatus::default(),
            mount: MountInfo {
                enabled: false,
                path: None,
                mounted: false,
                error: None,
            },
            ports: vec![Port {
                proto: "tcp".into(),
                port: 8080,
                addresses: vec![Ipv4Addr::LOCALHOST],
            }],
            port_errors: vec![],
        };
        let v = serde_json::to_value(&status).unwrap();
        assert_eq!(v["state"], "running");
        assert_eq!(v["docker"]["state"], "ready");
        assert!(v.get("error").is_none());
        assert!(v.get("port_errors").is_none());
        let back: Status = serde_json::from_value(v).unwrap();
        assert_eq!(back, status);

        let mut with_err = status;
        with_err.port_errors.push(PortError {
            proto: "tcp".into(),
            port: 80,
            error: "address already in use".into(),
        });
        let v = serde_json::to_value(&with_err).unwrap();
        assert_eq!(v["port_errors"][0]["port"], 80);
        let back: Status = serde_json::from_value(v).unwrap();
        assert_eq!(back, with_err);

        // Older daemons omit `dns_isolated`; serde must default it to false.
        let legacy: K8sStatus = serde_json::from_value(json!({
            "enabled": true,
            "state": "running",
            "installed": true,
            "dns_domain": "k8s.kat.local",
            "dns_port": 15353,
            "dns_installed": true
        }))
        .unwrap();
        assert!(!legacy.dns_isolated);

        // Older daemons omit disk_gib and disk_bytes on VmInfo; it defaults to 64 GiB.
        let legacy: VmInfo =
            serde_json::from_value(json!({"cpus": 4, "memory_mib": 4096})).unwrap();
        assert_eq!(legacy.disk_gib, crate::config::DEFAULT_DISK_GIB);
        assert_eq!(legacy.disk_bytes, None);
        assert_eq!(
            legacy.attached_disk_bytes(),
            u64::from(crate::config::DEFAULT_DISK_GIB) << 30
        );
        assert_eq!(legacy.requested_disk_gib, None);
    }

    #[test]
    fn pre_disk_cli_decodes_status_and_config_view_replies() {
        #[derive(Debug, PartialEq, Eq, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct PreDiskConfig {
            cpus: u8,
            memory_mib: u32,
            mount: bool,
            mount_path: Option<PathBuf>,
            launch_at_login: bool,
            pause_on_sleep: bool,
            auto_forward: bool,
            amd64: crate::amd64::Emulation,
            kubernetes: bool,
        }
        impl Default for PreDiskConfig {
            fn default() -> Self {
                Self {
                    cpus: 1,
                    memory_mib: 1024,
                    mount: true,
                    mount_path: None,
                    launch_at_login: true,
                    pause_on_sleep: false,
                    auto_forward: true,
                    amd64: crate::amd64::Emulation::default(),
                    kubernetes: false,
                }
            }
        }
        #[derive(Deserialize)]
        struct PreDiskStatus {
            state: VmState,
            versions: Versions,
            config: PreDiskConfig,
        }
        #[derive(Deserialize)]
        struct PreDiskConfigView {
            config: PreDiskConfig,
            restart_required: bool,
        }

        for explicit_target in [None, Some(crate::config::DEFAULT_DISK_GIB), Some(500)] {
            let cfg = Config {
                disk_gib: explicit_target,
                ..Config::default()
            };
            let expected_legacy = PreDiskConfig {
                cpus: cfg.cpus,
                memory_mib: cfg.memory_mib,
                mount: cfg.mount,
                mount_path: cfg.mount_path.clone(),
                launch_at_login: cfg.launch_at_login,
                pause_on_sleep: cfg.pause_on_sleep,
                auto_forward: cfg.auto_forward,
                amd64: cfg.amd64,
                kubernetes: cfg.kubernetes,
            };
            let display_gib = cfg.disk_gib();
            let status = Status {
                state: VmState::Running,
                error: None,
                since_ms: 1,
                uptime_s: Some(2.5),
                daemon: DaemonInfo {
                    pid: 7,
                    home: "/tmp/h".into(),
                    started_ms: 0,
                    launchd_label: None,
                },
                versions: Versions {
                    katamaran: "0.11.0".into(),
                    protocol: 3,
                    kernel: Some("6.18".into()),
                },
                config: WireConfig::from(&cfg),
                disk_gib: Some(display_gib),
                disk_target_gib: cfg.disk_gib,
                vm: Some(VmInfo {
                    cpus: cfg.cpus,
                    memory_mib: cfg.memory_mib,
                    disk_gib: display_gib,
                    disk_bytes: Some(u64::from(display_gib) << 30),
                    requested_disk_gib: Some(display_gib),
                    pid: Some(9),
                    boot_ms: Some(120.0),
                    docker_ready_ms: Some(900.0),
                }),
                usage: None,
                docker: DockerInfo {
                    state: DockerState::Ready,
                    socket: "/tmp/h/run/docker.sock".into(),
                },
                k8s: K8sStatus::default(),
                mount: MountInfo {
                    enabled: false,
                    path: None,
                    mounted: false,
                    error: None,
                },
                ports: vec![],
                port_errors: vec![],
            };
            let status_json = serde_json::to_value(&status).unwrap();
            assert!(status_json["config"].get("disk_gib").is_none());
            assert_eq!(status_json["disk_gib"], display_gib);
            let old_status: PreDiskStatus = serde_json::from_value(status_json).unwrap();
            assert_eq!(old_status.state, VmState::Running);
            assert_eq!(old_status.versions.katamaran, "0.11.0");
            assert_eq!(old_status.config, expected_legacy);

            let view = ConfigView {
                config: WireConfig::from(&cfg),
                disk_gib: Some(display_gib),
                disk_target_gib: cfg.disk_gib,
                path: "/tmp/h/config.toml".into(),
                restart_required: explicit_target == Some(500),
            };
            let view_json = serde_json::to_value(&view).unwrap();
            assert!(view_json["config"].get("disk_gib").is_none());
            assert_eq!(view_json["disk_gib"], display_gib);
            let old_view: PreDiskConfigView = serde_json::from_value(view_json).unwrap();
            assert_eq!(old_view.config, expected_legacy);
            assert_eq!(old_view.restart_required, view.restart_required);
            assert_eq!(view.resolved_config().disk_gib, explicit_target);
            assert_eq!(view.resolved_config().disk_gib(), display_gib);
        }
    }
}
