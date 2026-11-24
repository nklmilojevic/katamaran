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
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::Config;
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
    K8sReset,
    K8sKubeconfig,
    K8sSyncImages,
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
            Call::K8sReset => "k8s.reset",
            Call::K8sKubeconfig => "k8s.kubeconfig",
            Call::K8sSyncImages => "k8s.sync_images",
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
            "k8s.reset" => Call::K8sReset,
            "k8s.kubeconfig" => Call::K8sKubeconfig,
            "k8s.sync_images" => Call::K8sSyncImages,
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
    pub config: Config,
    pub path: PathBuf,
    /// The running VM was started with different settings.
    pub restart_required: bool,
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
    /// The saved settings, used at the next start.
    pub config: Config,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Start request until the guest's hello.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_ms: Option<f64>,
    /// Start request until Docker answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_ready_ms: Option<f64>,
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
            Call::K8sReset,
            Call::K8sKubeconfig,
            Call::K8sSyncImages,
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
            json!({"values": {"cpus": 2, "mount": false, "mount_path": "/tmp/x"}}),
        );
        let Call::ConfigSet(v) = Call::parse(&r).unwrap() else {
            panic!()
        };
        assert_eq!(v["cpus"], "2");
        assert_eq!(v["mount"], "false");
        assert_eq!(v["mount_path"], "/tmp/x");
        assert_eq!(
            Call::parse(&req("stop", Value::Null)).unwrap(),
            Call::Stop { wait: false }
        );
    }

    #[test]
    fn rejects_bad_requests() {
        let code = |r: Request| Call::parse(&r).unwrap_err().code;
        assert_eq!(code(req("reboot", Value::Null)), METHOD_NOT_FOUND);
        assert_eq!(code(req("stop", json!({"wait": "yes"}))), INVALID_PARAMS);
        assert_eq!(code(req("stop", json!({"force": true}))), INVALID_PARAMS);
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
            config: Config::default(),
            vm: Some(VmInfo {
                cpus: 4,
                memory_mib: 4096,
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
    }
}
