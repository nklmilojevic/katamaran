//! Kubernetes (k3s) management protocol, spoken on [`crate::K8S_PORT`].
//!
//! The host opens one connection per request and sends one request frame
//! (same framing as [`crate::Msg`], kinds 0x801–0x81f):
//!
//! | request          | then                                       | guest answers                               |
//! |------------------|--------------------------------------------|---------------------------------------------|
//! | `Status`         | –                                          | `StatusInfo`, or `Error`                    |
//! | `Install`        | the `k3s` binary as `Data`..., `DataEnd`   | `Progress`…, then `Done` / `Error`          |
//! | `Start`          | optional `extra_args`, `config_yaml`       | `Progress`…, then `StatusInfo` / `Error`    |
//! | `Stop`           | –                                          | `Progress`…, then `Done` / `Error`          |
//! | `Reset`          | –                                          | `Progress`…, then `Done` / `Error`          |
//! | `Kubeconfig`     | –                                          | `KubeconfigData`, or `Error`                |
//! | `Connect`        | `Data`… (`DataEnd` closes the write side)  | `Done` then `Data`…, `DataEnd`; or `Error`  |
//! | `SyncImages`     | –                                          | `Done`, or `Error`                          |
//! | `Proxy`          | `Data`… (`DataEnd` closes the write side)  | `Done` then `Data`…, `DataEnd`; or `Error`  |
//! | `MarkDatastoreVersion` | –                                    | `Done`, or `Error`                          |
//! | `Capabilities`   | –                                          | `CapabilitiesInfo`, or `Error`              |

use std::io::{self, Read, Write};

use crate::{Cursor, HEADER, MAX_FRAME, ProtoError, put_str, put_u32, read_len};

/// Payload bytes per `Data` frame.
pub const CHUNK: usize = 64 << 10;

/// Capability bit advertised in [`K8sMsg::CapabilitiesInfo`] when the guest
/// supports non-empty `extra_args` and `config_yaml` payloads on [`K8sMsg::Start`].
pub const CAP_START_CONFIG: u32 = 1 << 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum K8sMsg {
    Status,
    Install {
        version: String,
    },
    Start {
        /// Extra arguments appended to `k3s server`.
        extra_args: Vec<String>,
        /// Optional contents of Katamaran's managed guest k3s YAML config
        /// (`/var/lib/katamaran/k3s/config.yaml`, passed via `K3S_CONFIG_FILE`).
        config_yaml: Option<String>,
    },
    Stop,
    Reset,
    Kubeconfig,
    /// Dial an arbitrary IPv4/IPv6 address and TCP port inside the guest
    /// (used for direct Pod IP `10.42.x.x` and Service ClusterIP `10.43.x.x`
    /// connections from macOS).
    Connect {
        addr: String,
        port: u16,
    },
    SyncImages,
    /// Connect to the guest's root-only `kubectl proxy` Unix socket for
    /// authenticated plain-HTTP Kubernetes API access from the host app.
    Proxy,
    /// Record an independently verified k3s release tag for an existing
    /// unmarked datastore while Kubernetes is stopped. Refuses to overwrite
    /// an existing marker.
    MarkDatastoreVersion {
        version: String,
    },
    /// Query guest Kubernetes service capabilities (see `CAP_*`).
    Capabilities,
    Data(Vec<u8>),
    DataEnd,
    StatusInfo(K8sInfo),
    KubeconfigData(String),
    CapabilitiesInfo(u32),
    Done,
    Error(String),
    Progress(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct K8sInfo {
    /// `stopped`, `starting`, `running`, `stopping`, or `failed`.
    pub state: String,
    /// Whether the `k3s` binary is installed on the data disk.
    pub installed: bool,
    /// Installed k3s version string (empty if not installed).
    pub version: String,
    /// Whether the Kubernetes API server is accepting connections.
    pub api_ready: bool,
    /// Last error, if any.
    pub error: String,
}

mod kind {
    pub const STATUS: u16 = 0x801;
    pub const INSTALL: u16 = 0x802;
    pub const START: u16 = 0x803;
    pub const STOP: u16 = 0x804;
    pub const RESET: u16 = 0x805;
    pub const KUBECONFIG: u16 = 0x806;
    pub const CONNECT: u16 = 0x807;
    pub const SYNC_IMAGES: u16 = 0x808;
    pub const PROXY: u16 = 0x809;
    pub const DATA: u16 = 0x80a;
    pub const DATA_END: u16 = 0x80b;
    pub const STATUS_INFO: u16 = 0x80c;
    pub const KUBECONFIG_DATA: u16 = 0x80d;
    pub const DONE: u16 = 0x80e;
    pub const ERROR: u16 = 0x80f;
    pub const PROGRESS: u16 = 0x810;
    pub const MARK_DATASTORE_VERSION: u16 = 0x811;
    pub const CAPABILITIES: u16 = 0x812;
    pub const CAPABILITIES_INFO: u16 = 0x813;
}

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

impl K8sMsg {
    /// True when this is a [`K8sMsg::Start`] carrying non-empty `extra_args` or
    /// `config_yaml`, which requires guest [`CAP_START_CONFIG`] support.
    pub fn is_extended_start(&self) -> bool {
        matches!(
            self,
            K8sMsg::Start {
                extra_args,
                config_yaml,
            } if !extra_args.is_empty() || config_yaml.is_some()
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER];
        let b = &mut buf;
        let k = match self {
            K8sMsg::Status => kind::STATUS,
            K8sMsg::Install { version } => {
                put_str(b, version);
                kind::INSTALL
            }
            K8sMsg::Start {
                extra_args,
                config_yaml,
            } => {
                if !extra_args.is_empty() || config_yaml.is_some() {
                    put_u32(b, extra_args.len() as u32);
                    for arg in extra_args {
                        put_str(b, arg);
                    }
                    match config_yaml {
                        Some(yaml) => {
                            b.push(1);
                            put_str(b, yaml);
                        }
                        None => b.push(0),
                    }
                }
                kind::START
            }
            K8sMsg::Stop => kind::STOP,
            K8sMsg::Reset => kind::RESET,
            K8sMsg::Kubeconfig => kind::KUBECONFIG,
            K8sMsg::Connect { addr, port } => {
                put_str(b, addr);
                b.extend_from_slice(&port.to_le_bytes());
                kind::CONNECT
            }
            K8sMsg::SyncImages => kind::SYNC_IMAGES,
            K8sMsg::Proxy => kind::PROXY,
            K8sMsg::MarkDatastoreVersion { version } => {
                put_str(b, version);
                kind::MARK_DATASTORE_VERSION
            }
            K8sMsg::Capabilities => kind::CAPABILITIES,
            K8sMsg::Data(d) => {
                put_bytes(b, d);
                kind::DATA
            }
            K8sMsg::DataEnd => kind::DATA_END,
            K8sMsg::StatusInfo(info) => {
                put_str(b, &info.state);
                b.push(u8::from(info.installed));
                put_str(b, &info.version);
                b.push(u8::from(info.api_ready));
                put_str(b, &info.error);
                kind::STATUS_INFO
            }
            K8sMsg::KubeconfigData(yaml) => {
                put_str(b, yaml);
                kind::KUBECONFIG_DATA
            }
            K8sMsg::CapabilitiesInfo(flags) => {
                put_u32(b, *flags);
                kind::CAPABILITIES_INFO
            }
            K8sMsg::Done => kind::DONE,
            K8sMsg::Error(e) => {
                put_str(b, e);
                kind::ERROR
            }
            K8sMsg::Progress(p) => {
                put_str(b, p);
                kind::PROGRESS
            }
        };
        let len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf[4..6].copy_from_slice(&k.to_le_bytes());
        buf
    }

    pub fn decode(kind: u16, payload: &[u8]) -> Result<K8sMsg, ProtoError> {
        let mut c = Cursor { buf: payload };
        let flag = |c: &mut Cursor<'_>, what: &'static str| -> Result<bool, ProtoError> {
            match c.u8()? {
                0 => Ok(false),
                1 => Ok(true),
                v => Err(ProtoError::InvalidValue(what, v.into())),
            }
        };
        let msg = match kind {
            kind::STATUS => K8sMsg::Status,
            kind::INSTALL => K8sMsg::Install {
                version: c.string()?,
            },
            kind::START => {
                if c.buf.is_empty() {
                    K8sMsg::Start {
                        extra_args: Vec::new(),
                        config_yaml: None,
                    }
                } else {
                    let count = c.u32()? as usize;
                    if count > c.buf.len() / 4 {
                        return Err(ProtoError::Truncated);
                    }
                    let mut extra_args = Vec::with_capacity(count);
                    for _ in 0..count {
                        extra_args.push(c.string()?);
                    }
                    let config_yaml = if flag(&mut c, "k8s has_config_yaml")? {
                        Some(c.string()?)
                    } else {
                        None
                    };
                    K8sMsg::Start {
                        extra_args,
                        config_yaml,
                    }
                }
            }
            kind::STOP => K8sMsg::Stop,
            kind::RESET => K8sMsg::Reset,
            kind::KUBECONFIG => K8sMsg::Kubeconfig,
            kind::CONNECT => K8sMsg::Connect {
                addr: c.string()?,
                port: c.u16()?,
            },
            kind::SYNC_IMAGES => K8sMsg::SyncImages,
            kind::PROXY => K8sMsg::Proxy,
            kind::MARK_DATASTORE_VERSION => K8sMsg::MarkDatastoreVersion {
                version: c.string()?,
            },
            kind::CAPABILITIES => K8sMsg::Capabilities,
            kind::DATA => {
                let n = c.u32()? as usize;
                K8sMsg::Data(c.take(n)?.to_vec())
            }
            kind::DATA_END => K8sMsg::DataEnd,
            kind::STATUS_INFO => K8sMsg::StatusInfo(K8sInfo {
                state: c.string()?,
                installed: flag(&mut c, "k8s installed")?,
                version: c.string()?,
                api_ready: flag(&mut c, "k8s api_ready")?,
                error: c.string()?,
            }),
            kind::KUBECONFIG_DATA => K8sMsg::KubeconfigData(c.string()?),
            kind::CAPABILITIES_INFO => K8sMsg::CapabilitiesInfo(c.u32()?),
            kind::DONE => K8sMsg::Done,
            kind::ERROR => K8sMsg::Error(c.string()?),
            kind::PROGRESS => K8sMsg::Progress(c.string()?),
            other => return Err(ProtoError::UnknownKind(other)),
        };
        if !c.buf.is_empty() {
            return Err(ProtoError::TrailingBytes(c.buf.len()));
        }
        Ok(msg)
    }
}

pub fn write<W: Write>(mut w: W, msg: &K8sMsg) -> io::Result<()> {
    let frame = msg.encode();
    let len = frame.len() - 4;
    if len > MAX_FRAME {
        return Err(ProtoError::FrameTooLarge(len).into());
    }
    w.write_all(&frame)?;
    w.flush()
}

pub fn read<R: Read>(mut r: R) -> io::Result<Option<K8sMsg>> {
    let Some(len) = read_len(&mut r)? else {
        return Ok(None);
    };
    if len < 2 {
        return Err(ProtoError::Truncated.into());
    }
    if len > MAX_FRAME {
        return Err(ProtoError::FrameTooLarge(len).into());
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    let kind = u16::from_le_bytes([body[0], body[1]]);
    Ok(Some(K8sMsg::decode(kind, &body[2..])?))
}

/// Parse one Go `flag` / `urfave/cli` style token (`-flag`, `--flag`,
/// `-flag=val`, `--flag=val`) into `(name, inline value)`.
fn parse_k3s_flag(arg: &str) -> Option<(&str, Option<&str>)> {
    let stripped = arg.strip_prefix("--").or_else(|| arg.strip_prefix('-'))?;
    if stripped.is_empty() || stripped.starts_with('-') {
        return None;
    }
    match stripped.split_once('=') {
        Some((name, val)) => Some((name, Some(val))),
        None => Some((stripped, None)),
    }
}

/// `k3s server` flags (k3s v1.31–v1.37 names) accepted in `k8s_args` and
/// `k3s-config.yaml`: logging, component arguments, labels/taints/SANs,
/// optional components and features. Everything else is rejected, because
/// the guest sets or depends on it (data dir, kubeconfig, API listener and
/// ports, node identity, networking, container runtime, control-plane
/// components, joining other servers) or because it is not useful in the
/// single-node guest. An allowlist, so new or obscure k3s flags fail closed.
const ALLOWED_K3S_FLAGS: &[&str] = &[
    "debug",
    "v",
    "vmodule",
    "log",
    "alsologtostderr",
    "disable",
    "disable-cloud-controller",
    "disable-helm-controller",
    "disable-network-policy",
    "disable-default-registry-endpoint",
    "kube-apiserver-arg",
    "kube-scheduler-arg",
    "kube-controller-manager-arg",
    "kube-controller-arg",
    "kube-cloud-controller-manager-arg",
    "kube-cloud-controller-arg",
    "kubelet-arg",
    "kube-proxy-arg",
    "helm-controller-arg",
    "node-label",
    "node-taint",
    "tls-san",
    "tls-san-security",
    "service-node-port-range",
    "cluster-domain",
    "default-local-storage-path",
    "secrets-encryption",
    "secrets-encryption-provider",
    "embedded-registry",
    "system-default-registry",
    "private-registry",
    "pause-image",
    "egress-selector-mode",
    "servicelb-namespace",
    "protect-kernel-defaults",
    "enable-pprof",
    "supervisor-metrics",
    "image-credential-provider-bin-dir",
    "image-credential-provider-config",
    "helm-job-image",
    "nonroot-devices",
    "token-file",
    "agent-token-file",
    "vpn-auth-file",
];

/// Datastore and snapshot settings, also accepted (in `k8s_args` only when
/// not in [`CREDENTIAL_K3S_FLAGS`]). They change where cluster state lives;
/// `kat k8s reset` only wipes the local data directory.
const DATASTORE_K3S_FLAGS: &[&str] = &[
    "cluster-init",
    "datastore-endpoint",
    "datastore-cafile",
    "datastore-certfile",
    "datastore-keyfile",
    "etcd-disable-snapshots",
    "etcd-expose-metrics",
    "etcd-arg",
    "etcd-snapshot-compress",
    "etcd-snapshot-dir",
    "etcd-snapshot-name",
    "etcd-snapshot-reconcile-interval",
    "etcd-snapshot-retention",
    "etcd-snapshot-schedule-cron",
    "etcd-s3",
    "etcd-s3-bucket",
    "etcd-s3-bucket-lookup-type",
    "etcd-s3-config-secret",
    "etcd-s3-endpoint",
    "etcd-s3-endpoint-ca",
    "etcd-s3-folder",
    "etcd-s3-insecure",
    "etcd-s3-region",
    "etcd-s3-retention",
    "etcd-s3-skip-ssl-verify",
    "etcd-s3-timeout",
    "etcd-s3-access-key",
    "etcd-s3-secret-key",
    "etcd-s3-session-token",
    "etcd-s3-proxy",
    "token",
    "agent-token",
    "vpn-auth",
];

/// Component settings that would override something katamaran manages,
/// directly or through the component's own config file: the kubelet root
/// that stop/reset unmount, swap tolerance (the guest runs zram swap), the
/// kubelet and API server credentials k3s generates, the shared containerd,
/// the node name, the pod and
/// service CIDRs and cluster DNS, kube-proxy's nftables mode (stop/reset only
/// delete its nftables table), and the API server address the relay uses.
/// Other component arguments pass through unchecked.
const MANAGED_COMPONENT_ARGS: &[(&str, &[&str])] = &[
    (
        "kubelet-arg",
        &[
            "root-dir",
            "container-runtime-endpoint",
            "hostname-override",
            "cluster-dns",
            "fail-swap-on",
            "kubeconfig",
            "bootstrap-kubeconfig",
            "cert-dir",
            "config",
            "config-dir",
        ],
    ),
    (
        "kube-proxy-arg",
        &[
            "proxy-mode",
            "cluster-cidr",
            "hostname-override",
            "config",
            "write-config-to",
        ],
    ),
    (
        "kube-apiserver-arg",
        &[
            "secure-port",
            "bind-address",
            "advertise-address",
            "service-cluster-ip-range",
            "tls-cert-file",
            "tls-private-key-file",
            "client-ca-file",
        ],
    ),
    (
        "kube-controller-manager-arg",
        &[
            "cluster-cidr",
            "service-cluster-ip-range",
            "allocate-node-cidrs",
        ],
    ),
];

/// Settings whose value can carry credentials: accepted only in
/// `k3s-config.yaml`. `k8s_args` end up on the k3s process command line,
/// which `k3s --debug` also writes to its log.
const CREDENTIAL_K3S_FLAGS: &[&str] = &[
    "token",
    "agent-token",
    "datastore-endpoint",
    "etcd-s3-access-key",
    "etcd-s3-secret-key",
    "etcd-s3-session-token",
    "etcd-s3-proxy",
    "vpn-auth",
];

/// The setting a component argument such as `root-dir=/x`, `--root_dir=/x`
/// or `root-dir+=/x` addresses, normalized the way kubelet/kube-proxy
/// (`_` == `-`) and k3s (`+=`/`-=` suffixes) read it.
fn component_arg_key(value: &str) -> String {
    let setting = value.trim_start_matches('-');
    let key = setting.split_once('=').map_or(setting, |(k, _)| k);
    key.trim_end_matches(['+', '-']).replace('_', "-")
}

/// Check one k3s setting (`name` without dashes; `values` are the values it
/// is given) against the allowlist and the reserved component settings.
/// Errors name the flag but never echo values.
fn check_k3s_setting(name: &str, values: &[&str], in_args: bool) -> Result<(), String> {
    let allowed = ALLOWED_K3S_FLAGS.contains(&name) || DATASTORE_K3S_FLAGS.contains(&name);
    if !allowed {
        return Err(format!(
            "--{name} is not supported: katamaran only passes k3s settings that cannot conflict with how it runs the guest cluster (see the Kubernetes docs for the list)"
        ));
    }
    if in_args && CREDENTIAL_K3S_FLAGS.contains(&name) {
        return Err(format!(
            "--{name} can carry credentials and would appear in the k3s process command line and, with --debug, its log; set `{name}:` in ~/.katamaran/k3s-config.yaml (or use the matching *-file flag) instead"
        ));
    }
    let component = match name {
        "kube-controller-arg" => "kube-controller-manager-arg",
        "kube-cloud-controller-arg" => "kube-cloud-controller-manager-arg",
        other => other,
    };
    if let Some((_, managed)) = MANAGED_COMPONENT_ARGS.iter().find(|(f, _)| *f == component) {
        for v in values {
            let key = component_arg_key(v);
            if managed.contains(&key.as_str()) {
                return Err(format!(
                    "--{name} cannot set {key}: katamaran manages it for the guest cluster"
                ));
            }
        }
    }
    Ok(())
}

/// Allowlisted k3s flags that are booleans (k3s `cli.BoolFlag`): they never
/// consume the next argument as their value.
const BOOL_K3S_FLAGS: &[&str] = &[
    "debug",
    "alsologtostderr",
    "disable-cloud-controller",
    "disable-helm-controller",
    "disable-network-policy",
    "disable-default-registry-endpoint",
    "secrets-encryption",
    "embedded-registry",
    "protect-kernel-defaults",
    "enable-pprof",
    "supervisor-metrics",
    "nonroot-devices",
    "tls-san-security",
    "cluster-init",
    "etcd-disable-snapshots",
    "etcd-expose-metrics",
    "etcd-snapshot-compress",
    "etcd-s3",
    "etcd-s3-insecure",
    "etcd-s3-skip-ssl-verify",
];

/// Check one `k3s-config.yaml` key and its values (see [`check_k3s_setting`]).
pub fn check_k3s_config_setting(name: &str, values: &[&str]) -> Result<(), String> {
    check_k3s_setting(name, values, false)
}

/// Validate `k8s_args` before they are saved (host) and before `k3s server`
/// starts (guest). Errors never echo argument values.
pub fn validate_k3s_extra_args(args: &[String]) -> Result<(), String> {
    if args.iter().any(String::is_empty) {
        return Err("k8s_args must not contain empty arguments".into());
    }
    if args.iter().any(|a| a.contains('\0')) {
        return Err("k8s_args must not contain NUL bytes".into());
    }
    let mut i = 0;
    while i < args.len() {
        let Some((name, inline)) = parse_k3s_flag(&args[i]) else {
            i += 1;
            continue;
        };
        // Like Go's flag parser, a non-boolean flag without `=value` takes
        // the next argument as its value even if it starts with `-`.
        let takes_next = inline.is_none() && !BOOL_K3S_FLAGS.contains(&name);
        let value = if takes_next {
            args.get(i + 1).map(String::as_str)
        } else {
            inline
        };
        check_k3s_setting(name, value.as_slice(), true).map_err(|e| format!("k8s_args: {e}"))?;
        i += if takes_next { 2 } else { 1 };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn validate_k3s_extra_args_rejects_managed_and_credential_flags() {
        let v = |a: &[&str]| {
            super::validate_k3s_extra_args(&a.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };
        v(&[]).unwrap();
        v(&["--disable=traefik", "--debug", "--kubelet-arg=max-pods=200"]).unwrap();
        v(&["--kubelet-arg", "max-pods=200", "--node-label", "a=b"]).unwrap();
        v(&["--kube-proxy-arg=metrics-bind-address=0.0.0.0:10249"]).unwrap();
        v(&[
            "--kube-apiserver-arg=audit-log-path=-",
            "--kubelet-arg=max-pods=200",
        ])
        .unwrap();
        // Positional values that merely look like names are not flags.
        v(&["--node-label", "token=x"]).unwrap();
        // A split value may start with a dash; it is checked as the value.
        v(&["--kubelet-arg", "--max-pods=200"]).unwrap();
        v(&["--kubelet-arg", "--root-dir=/x"]).unwrap_err();
        // A boolean flag does not consume the next argument.
        v(&["--debug", "--bind-address=0.0.0.0"]).unwrap_err();

        assert!(v(&[""]).is_err());
        assert!(v(&["--foo=\0bar"]).is_err());

        let marker = "placeholder-marker-3310";
        for flag in [
            // managed by the guest / conflicts with how it runs the cluster
            "data-dir",
            "d",
            "write-kubeconfig",
            "o",
            "write-kubeconfig-mode",
            "https-listen-port",
            "container-runtime-endpoint",
            "node-name",
            "flannel-backend",
            "cluster-cidr",
            "service-cidr",
            "cluster-dns",
            "config",
            "c",
            "disable-kube-proxy",
            "bind-address",
            "with-node-id",
            "disable-apiserver",
            "advertise-port",
            "supervisor-port",
            "server",
            "snapshotter",
            "node-ip",
            "disable-agent",
            "resolv-conf",
            "made-up-flag",
            // credentials
            "token",
            "t",
            "agent-token",
            "datastore-endpoint",
            "etcd-s3-access-key",
            "etcd-s3-secret-key",
            "etcd-s3-session-token",
            "etcd-s3-proxy",
            "vpn-auth",
        ] {
            for args in [
                vec![format!("--{flag}={marker}")],
                vec![format!("-{flag}={marker}")],
                vec![format!("--{flag}"), marker.to_string()],
                vec![format!("-{flag}")],
            ] {
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                let err = v(&args).expect_err(&format!("{args:?}"));
                assert!(!err.contains(marker), "{args:?} echoed its value: {err}");
            }
        }
        // Credentials are fine in k3s-config.yaml; unknown/managed keys are not.
        super::check_k3s_config_setting("token", &["x"]).unwrap();
        super::check_k3s_config_setting("etcd-s3-proxy", &["none"]).unwrap();
        super::check_k3s_config_setting("bind-address", &["x"]).unwrap_err();
        super::check_k3s_config_setting("kube-controller-arg", &["cluster-cidr=x"]).unwrap_err();
        for kubelet in [
            vec!["--kubelet-arg=root-dir=/x"],
            vec!["--kubelet-arg=--root-dir=/x"],
            vec!["--kubelet-arg", "root-dir=/x"],
            vec!["--kubelet-arg=root_dir=/x"],
            vec!["--kubelet-arg=root-dir+=/x"],
            vec!["--kubelet-arg=root-dir-=/x"],
            vec!["-kubelet-arg=container-runtime-endpoint=unix:///x"],
            vec!["--kubelet-arg=container_runtime_endpoint=unix:///x"],
            vec!["--kubelet-arg=root-dir"],
            vec!["--kube-proxy-arg=proxy-mode=iptables"],
            vec!["--kube-proxy-arg", "proxy_mode=ipvs"],
            vec!["--kube-proxy-arg=config=/var/lib/kp.yaml"],
            vec!["--kube-proxy-arg=write-config-to=/tmp/x"],
            vec!["--kubelet-arg=config=/etc/kubelet.yaml"],
            vec!["--kubelet-arg=config-dir=/etc/kubelet.d"],
            vec!["--kubelet-arg=hostname-override=x"],
            vec!["--kube-apiserver-arg=secure-port=7443"],
            vec!["--kube-apiserver-arg=service-cluster-ip-range=10.9.0.0/16"],
            vec!["--kube-controller-manager-arg=cluster-cidr=10.8.0.0/16"],
            vec!["--kube-controller-manager-arg=allocate_node_cidrs=false"],
            vec!["--kube-controller-arg=cluster-cidr=10.8.0.0/16"],
            vec!["--kubelet-arg=fail-swap-on=true"],
            vec!["--kubelet-arg=fail_swap_on=true"],
            vec!["--kubelet-arg=cert-dir=/x"],
            vec!["--kube-apiserver-arg=tls-cert-file=/x"],
            vec!["--kube-apiserver-arg=advertise-address=10.0.0.1"],
        ] {
            assert!(v(&kubelet).is_err(), "{kubelet:?}");
        }
    }

    use super::*;

    #[test]
    fn every_message_round_trips() {
        let msgs = [
            K8sMsg::Status,
            K8sMsg::Install {
                version: "v1.37.2+k3s1".into(),
            },
            K8sMsg::Start {
                extra_args: vec![],
                config_yaml: None,
            },
            K8sMsg::Start {
                extra_args: vec!["--disable=traefik".into(), "--debug".into()],
                config_yaml: None,
            },
            K8sMsg::Start {
                extra_args: vec!["--disable=traefik".into()],
                config_yaml: Some("disable:\n  - servicelb\n".into()),
            },
            K8sMsg::Start {
                extra_args: vec![],
                config_yaml: Some("write-kubeconfig-mode: '0600'\n".into()),
            },
            K8sMsg::Stop,
            K8sMsg::Reset,
            K8sMsg::Kubeconfig,
            K8sMsg::Connect {
                addr: "10.43.0.1".into(),
                port: 443,
            },
            K8sMsg::SyncImages,
            K8sMsg::Proxy,
            K8sMsg::MarkDatastoreVersion {
                version: "v1.37.2+k3s1".into(),
            },
            K8sMsg::Capabilities,
            K8sMsg::Data(vec![1, 2, 3, 4]),
            K8sMsg::DataEnd,
            K8sMsg::StatusInfo(K8sInfo {
                state: "running".into(),
                installed: true,
                version: "v1.37.2+k3s1".into(),
                api_ready: true,
                error: String::new(),
            }),
            K8sMsg::KubeconfigData("apiVersion: v1\n".into()),
            K8sMsg::CapabilitiesInfo(CAP_START_CONFIG),
            K8sMsg::Done,
            K8sMsg::Error("failed".into()),
            K8sMsg::Progress("starting k3s".into()),
        ];
        let mut wire = Vec::new();
        for m in &msgs {
            write(&mut wire, m).unwrap();
        }
        let mut slice = wire.as_slice();
        for want in &msgs {
            let got = read(&mut slice).unwrap().unwrap();
            assert_eq!(&got, want);
        }
        assert!(read(&mut slice).unwrap().is_none());
    }

    #[test]
    fn kinds_stay_in_the_assigned_range() {
        for m in [
            K8sMsg::Status,
            K8sMsg::Install {
                version: String::new(),
            },
            K8sMsg::Start {
                extra_args: vec![],
                config_yaml: None,
            },
            K8sMsg::Start {
                extra_args: vec!["--disable=traefik".into(), "--debug".into()],
                config_yaml: None,
            },
            K8sMsg::Start {
                extra_args: vec!["--disable=traefik".into()],
                config_yaml: Some("disable:\n  - servicelb\n".into()),
            },
            K8sMsg::Start {
                extra_args: vec![],
                config_yaml: Some("write-kubeconfig-mode: '0600'\n".into()),
            },
            K8sMsg::Stop,
            K8sMsg::Reset,
            K8sMsg::Kubeconfig,
            K8sMsg::Connect {
                addr: String::new(),
                port: 0,
            },
            K8sMsg::SyncImages,
            K8sMsg::Proxy,
            K8sMsg::MarkDatastoreVersion {
                version: String::new(),
            },
            K8sMsg::Capabilities,
            K8sMsg::Data(vec![]),
            K8sMsg::DataEnd,
            K8sMsg::StatusInfo(K8sInfo::default()),
            K8sMsg::KubeconfigData(String::new()),
            K8sMsg::CapabilitiesInfo(CAP_START_CONFIG),
            K8sMsg::Done,
            K8sMsg::Error(String::new()),
            K8sMsg::Progress(String::new()),
        ] {
            let f = m.encode();
            let k = u16::from_le_bytes([f[4], f[5]]);
            assert!((0x801..=0x81f).contains(&k), "kind {k:#x} out of range");
        }
    }

    #[test]
    fn truncated_length_header_and_oversized_write() {
        assert_eq!(read([].as_slice()).unwrap(), None);
        for n in 1..=3 {
            let bytes = [1u8, 2, 3];
            let err = read(&bytes[..n]).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "len {n}");
        }
        let mut out = Vec::new();
        let err = write(&mut out, &K8sMsg::Error("e".repeat(MAX_FRAME))).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(out.is_empty());
    }

    #[test]
    fn empty_start_keeps_minimal_wire_format_and_rejects_bogus_count() {
        let empty = K8sMsg::Start {
            extra_args: vec![],
            config_yaml: None,
        };
        assert!(!empty.is_extended_start());
        assert!(
            K8sMsg::Start {
                extra_args: vec!["--debug".into()],
                config_yaml: None,
            }
            .is_extended_start()
        );
        assert!(
            K8sMsg::Start {
                extra_args: vec![],
                config_yaml: Some("debug: true\n".into()),
            }
            .is_extended_start()
        );
        let frame = empty.encode();
        assert_eq!(frame.len(), HEADER);
        assert_eq!(u32::from_le_bytes(frame[0..4].try_into().unwrap()), 2);
        assert_eq!(
            u16::from_le_bytes(frame[4..6].try_into().unwrap()),
            kind::START
        );
        assert_eq!(read(frame.as_slice()).unwrap(), Some(empty));

        // Corrupt count that claims millions of args in a 4-byte payload.
        let bogus = u32::MAX.to_le_bytes();
        assert!(matches!(
            K8sMsg::decode(kind::START, &bogus),
            Err(ProtoError::Truncated)
        ));
    }
}
