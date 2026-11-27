//! Kubernetes (k3s) management protocol, spoken on [`crate::K8S_PORT`].
//!
//! The host opens one connection per request and sends one request frame
//! (same framing as [`crate::Msg`], kinds 0x801–0x81f):
//!
//! | request                | then                                                                        | guest answers                                                                                                                   |
//! |------------------------|-----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
//! | `Status`               | –                                                                           | `StatusInfo`, or `Error`                                                                                                        |
//! | `Install`              | the `k3s` binary as `Data`..., `DataEnd`                                    | `Progress`…, then `Done` / `Error`                                                                                              |
//! | `Start`                | optional `extra_args`, `config_yaml`                                        | `Progress`…, then `StatusInfo` / `Error`                                                                                        |
//! | `Stop`                 | –                                                                           | `Progress`…, then `Done` / `Error`                                                                                              |
//! | `Reset`                | No follow-up; `Reset` may carry one `images` byte (`0` or `1`, default `0`) | `Progress`…, `ResetDataCleared`, `Progress`…, then `Done` / `Error` (with `images` = `1`: starts with `ResetAccepted`) |
//! | `Kubeconfig`           | –                                                                           | `KubeconfigData`, or `Error`                                                                                                    |
//! | `Connect`              | `Data`… (`DataEnd` closes the write side)                                   | `Done` then `Data`…, `DataEnd`; or `Error`                                                                                      |
//! | `SyncImages`           | –                                                                           | `Done`, or `Error`                                                                                                              |
//! | `Proxy`                | `Data`… (`DataEnd` closes the write side)                                   | `Done` then `Data`…, `DataEnd`; or `Error`                                                                                      |
//! | `MarkDatastoreVersion` | –                                                                           | `Done`, or `Error`                                                                                                              |
//! | `Capabilities`         | –                                                                           | `CapabilitiesInfo`, or `Error`                                                                                                  |

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
    Reset {
        /// Also remove all images in containerd's `k8s.io` namespace.
        images: bool,
    },
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
    /// Emitted by compatible guests before mutating state on `Reset { images: true }`
    /// so the host can distinguish an accepted reset from an older guest closing
    /// the connection after rejecting the flag byte.
    ResetAccepted,
    /// Emitted by compatible guests as soon as active cluster data has been
    /// removed or atomically detached, before deferred disk reclamation or
    /// image cleanup runs, so the host disables Kubernetes auto-start and
    /// reports a partial outcome even if reclamation, image purge, or the
    /// guest connection fails afterwards.
    ResetDataCleared,
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
    pub const RESET_ACCEPTED: u16 = 0x814;
    pub const RESET_DATA_CLEARED: u16 = 0x815;
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
            K8sMsg::Reset { images } => {
                // Omit payload when `images` is false so older guests still decode default resets;
                // send `1` when true so older guests reject `--images` rather than silently keeping images.
                if *images {
                    b.push(1);
                }
                kind::RESET
            }
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
            K8sMsg::ResetAccepted => kind::RESET_ACCEPTED,
            K8sMsg::ResetDataCleared => kind::RESET_DATA_CLEARED,
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
            kind::RESET => K8sMsg::Reset {
                images: if c.buf.is_empty() {
                    false
                } else {
                    flag(&mut c, "k8s reset images")?
                },
            },
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
            kind::RESET_ACCEPTED => K8sMsg::ResetAccepted,
            kind::RESET_DATA_CLEARED => K8sMsg::ResetDataCleared,
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
/// components, joining other servers, external datastores, experimental VPN)
/// or because it is not useful in the single-node guest. An allowlist, so new
/// or obscure k3s flags fail closed.
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
];

/// Embedded etcd, snapshot and token settings, also accepted (in `k8s_args`
/// only when not in [`CREDENTIAL_K3S_FLAGS`]). External datastores
/// (`datastore-endpoint` and its TLS flags) and experimental VPN (`vpn-auth*`)
/// are intentionally excluded: Kine and k3s can log malformed DSNs or URLs
/// with embedded credentials to `/var/log/k3s.log`, and Katamaran's reset and
/// minor-version downgrade guard assume cluster state lives under the managed
/// local data directory.
const DATASTORE_K3S_FLAGS: &[&str] = &[
    "cluster-init",
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
];

/// Component settings that would override something katamaran manages,
/// directly or through the component's own config file: the kubelet root
/// that stop/reset unmount, swap tolerance (the guest runs zram swap), the
/// embedded etcd data/WAL directories that reset deletes, the node IP and
/// resolver config, the component kubeconfigs and TLS credentials k3s
/// generates, the shared containerd, the node name, the pod and service CIDRs
/// and cluster DNS, kube-proxy's nftables mode (stop/reset only delete its
/// nftables table), and internal control-plane ports/addresses.
/// Other component arguments pass through unchecked.
const MANAGED_COMPONENT_ARGS: &[(&str, &[&str])] = &[
    (
        "kubelet-arg",
        &[
            "root-dir",
            "container-runtime-endpoint",
            "image-service-endpoint",
            "hostname-override",
            "cluster-dns",
            "fail-swap-on",
            "node-ip",
            "resolv-conf",
            "kubeconfig",
            "bootstrap-kubeconfig",
            "cert-dir",
            "tls-cert-file",
            "tls-private-key-file",
            "client-ca-file",
            "address",
            "port",
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
            "kubeconfig",
            "master",
            "healthz-bind-address",
            "healthz-port",
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
            "advertise-port",
            "service-cluster-ip-range",
            "cert-dir",
            "tls-cert-file",
            "tls-private-key-file",
            "client-ca-file",
            "storage-backend",
            "etcd-servers",
            "etcd-servers-overrides",
            "etcd-cafile",
            "etcd-certfile",
            "etcd-keyfile",
            "kubelet-client-certificate",
            "kubelet-client-key",
            "kubelet-certificate-authority",
            "proxy-client-cert-file",
            "proxy-client-key-file",
            "requestheader-client-ca-file",
            "service-account-key-file",
            "service-account-signing-key-file",
            "service-account-issuer",
        ],
    ),
    (
        "kube-controller-manager-arg",
        &[
            "cluster-cidr",
            "service-cluster-ip-range",
            "allocate-node-cidrs",
            "kubeconfig",
            "authorization-kubeconfig",
            "authentication-kubeconfig",
            "master",
            "bind-address",
            "secure-port",
            "tls-cert-file",
            "tls-private-key-file",
            "root-ca-file",
            "service-account-private-key-file",
        ],
    ),
    (
        "kube-scheduler-arg",
        &[
            "kubeconfig",
            "authorization-kubeconfig",
            "authentication-kubeconfig",
            "master",
            "bind-address",
            "secure-port",
            "tls-cert-file",
            "tls-private-key-file",
            "config",
            "write-config-to",
        ],
    ),
    (
        "kube-cloud-controller-manager-arg",
        &[
            "kubeconfig",
            "authorization-kubeconfig",
            "authentication-kubeconfig",
            "master",
            "cluster-cidr",
            "allocate-node-cidrs",
            "bind-address",
            "secure-port",
        ],
    ),
    (
        "helm-controller-arg",
        &["kubeconfig", "master-url", "node-name"],
    ),
    (
        "etcd-arg",
        &[
            "data-dir",
            "wal-dir",
            "config-file",
            "name",
            "listen-client-urls",
            "listen-client-http-urls",
            "listen-peer-urls",
            "advertise-client-urls",
            "initial-advertise-peer-urls",
            "initial-cluster",
            "initial-cluster-state",
            "initial-cluster-token",
            "client-transport-security",
            "peer-transport-security",
            "cert-file",
            "key-file",
            "trusted-ca-file",
            "client-cert-auth",
            "peer-cert-file",
            "peer-key-file",
            "peer-trusted-ca-file",
            "peer-client-cert-auth",
        ],
    ),
];

/// Component arguments that can carry inline credentials and are logged
/// verbatim at info level by k3s (`daemonconfig.ArgString`), so they are
/// rejected in both `k8s_args` and `k3s-config.yaml`.
const CREDENTIAL_COMPONENT_ARGS: &[(&str, &[&str])] =
    &[("kubelet-arg", &["manifest-url-header", "manifest-url"])];

/// Settings whose value can carry credentials: accepted only in
/// `k3s-config.yaml`. `k8s_args` end up on the k3s process command line,
/// which `k3s --debug` also writes to its log.
const CREDENTIAL_K3S_FLAGS: &[&str] = &[
    "token",
    "agent-token",
    "etcd-s3-access-key",
    "etcd-s3-secret-key",
    "etcd-s3-session-token",
    "etcd-s3-proxy",
];

fn is_valid_port_str(port: &str) -> bool {
    !port.is_empty()
        && port.bytes().all(|b| b.is_ascii_digit())
        && port.parse::<u16>().is_ok_and(|p| p > 0)
}

/// k3s logs `etcd-s3-endpoint` at info level (`s3.GetClient`) and passes it to
/// `minio.New`, which expects `host` or `host:port` with no scheme, path or
/// userinfo. Reject anything else with a fixed error so credentials placed here
/// by mistake never reach `/var/log/k3s.log`.
fn is_valid_etcd_s3_endpoint(val: &str) -> bool {
    let val = val.trim();
    if val.is_empty() {
        return true;
    }
    if val
        .bytes()
        .any(|b| b <= 0x20 || b == 0x7f || matches!(b, b'@' | b'/' | b'?' | b'#' | b'\\'))
    {
        return false;
    }
    if let Some(after_open) = val.strip_prefix('[') {
        let Some((inside, after_close)) = after_open.split_once(']') else {
            return false;
        };
        if inside.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        if after_close.is_empty() {
            return true;
        }
        let Some(port) = after_close.strip_prefix(':') else {
            return false;
        };
        return is_valid_port_str(port);
    }
    if val.contains(['[', ']']) {
        return false;
    }
    let (host, port) = match val.split_once(':') {
        Some((h, p)) => (h, Some(p)),
        None => (val, None),
    };
    if host.is_empty()
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
    {
        return false;
    }
    port.is_none_or(is_valid_port_str)
}

/// The setting a component argument such as `root-dir=/x`, `--root_dir=/x`
/// or `root-dir+=/x` addresses, normalized the way kubelet/kube-proxy
/// (`_` == `-`), k3s (`+=`/`-=` suffixes and `configfilearg.ToString`
/// whitespace trimming) and `urfave/cli/v2` (`StringSlice.Set` trimming when
/// `KeepSpace` is false) read it.
fn component_arg_key(value: &str) -> String {
    let setting = value.trim().trim_start_matches('-');
    let key = setting.split_once('=').map_or(setting, |(k, _)| k).trim();
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
    if name == "etcd-s3-endpoint" && values.iter().any(|v| !is_valid_etcd_s3_endpoint(v)) {
        return Err(
            "--etcd-s3-endpoint must be a host or host:port without a scheme, path or credentials"
                .into(),
        );
    }
    let component = match name {
        "kube-controller-arg" => "kube-controller-manager-arg",
        "kube-cloud-controller-arg" => "kube-cloud-controller-manager-arg",
        other => other,
    };
    if let Some((_, forbidden)) = CREDENTIAL_COMPONENT_ARGS
        .iter()
        .find(|(f, _)| *f == component)
    {
        for v in values {
            let key = component_arg_key(v);
            if forbidden.contains(&key.as_str()) {
                return Err(format!(
                    "--{name} cannot set {key}: k3s logs component arguments at info level, which would expose inline credentials in /var/log/k3s.log"
                ));
            }
        }
    }
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
/// Values are trimmed the way k3s `configfilearg.ToString` trims them before
/// turning config entries into CLI arguments.
pub fn check_k3s_config_setting(name: &str, values: &[&str]) -> Result<(), String> {
    let trimmed: Vec<&str> = values.iter().map(|v| v.trim()).collect();
    check_k3s_setting(name, &trimmed, false)
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
            return Err(
                "k8s_args: unexpected non-flag argument (every entry must be a k3s server flag or its value)"
                    .into(),
            );
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
        if takes_next && value.is_none() {
            return Err(format!("k8s_args: --{name} requires a value"));
        }
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
        // Bare positional tokens and missing values for non-bool flags are rejected without echoing.
        let bare_err = v(&["placeholder-bare-secret"]).unwrap_err();
        assert!(!bare_err.contains("placeholder-bare-secret"), "{bare_err}");
        assert!(v(&["--debug", "true"]).is_err());
        assert!(v(&["--", "--debug"]).is_err());
        assert!(v(&["--kubelet-arg"]).is_err());
        // Valid etcd-s3-endpoint forms pass; schemes, paths and credentials are rejected.
        v(&["--etcd-s3-endpoint=s3.amazonaws.com"]).unwrap();
        v(&["--etcd-s3-endpoint=minio.local:9000"]).unwrap();
        v(&["--etcd-s3-endpoint=[::1]:9000"]).unwrap();
        v(&["--etcd-arg=quota-backend-bytes=4294967296"]).unwrap();
        v(&["--kube-apiserver-arg=feature-gates=Foo=true,Bar=false"]).unwrap();

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
            "embedded-registry",
            "datastore-endpoint",
            "datastore-cafile",
            "datastore-certfile",
            "datastore-keyfile",
            "vpn-auth",
            "vpn-auth-file",
            "made-up-flag",
            // credentials (YAML-only)
            "token",
            "t",
            "agent-token",
            "etcd-s3-access-key",
            "etcd-s3-secret-key",
            "etcd-s3-session-token",
            "etcd-s3-proxy",
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
        // etcd-s3-endpoint with scheme/credentials/bad port is rejected without echoing.
        for bad_ep in [
            format!("https://ops:{marker}@s3.example.com"),
            format!("ops:{marker}@s3.example.com:9000"),
            format!("s3.example.com/{marker}"),
            "s3.example.com:badport".to_string(),
            "s3.example.com:0".to_string(),
        ] {
            let err = v(&[&format!("--etcd-s3-endpoint={bad_ep}")]).unwrap_err();
            assert!(!err.contains(marker), "{bad_ep} echoed value: {err}");
            let yerr = super::check_k3s_config_setting("etcd-s3-endpoint", &[&bad_ep]).unwrap_err();
            assert!(!yerr.contains(marker), "{bad_ep} echoed value: {yerr}");
        }

        // Credentials are fine in k3s-config.yaml; unknown/managed/excluded keys are not.
        super::check_k3s_config_setting("token", &["x"]).unwrap();
        super::check_k3s_config_setting("etcd-s3-proxy", &["none"]).unwrap();
        super::check_k3s_config_setting("etcd-s3-endpoint", &["  minio.local:9000  "]).unwrap();
        super::check_k3s_config_setting("bind-address", &["x"]).unwrap_err();
        super::check_k3s_config_setting("embedded-registry", &["true"]).unwrap_err();
        super::check_k3s_config_setting("kube-controller-arg", &["cluster-cidr=x"]).unwrap_err();
        // External datastores and VPN auth are excluded even in YAML, with no secret echoed.
        let pg_err = super::check_k3s_config_setting(
            "datastore-endpoint",
            &[&format!("postgres://ops:{marker}@db:badport/k3s")],
        )
        .unwrap_err();
        assert!(!pg_err.contains(marker), "{pg_err}");
        let vpn_err = super::check_k3s_config_setting(
            "vpn-auth",
            &[&format!("name=tailscale,joinKey={marker}")],
        )
        .unwrap_err();
        assert!(!vpn_err.contains(marker), "{vpn_err}");
        // Leading/trailing whitespace in YAML component args (trimmed by k3s
        // configfilearg.ToString) is caught for managed and credential keys.
        super::check_k3s_config_setting("kube-apiserver-arg", &["  bind-address=0.0.0.0"])
            .unwrap_err();
        super::check_k3s_config_setting("kubelet-arg", &["  fail-swap-on=true"]).unwrap_err();
        super::check_k3s_config_setting("etcd-arg", &["  data-dir=/tmp/etcd"]).unwrap_err();
        let yaml_hdr_err = super::check_k3s_config_setting(
            "kubelet-arg",
            &[&format!(
                "  manifest-url-header=Authorization:Bearer {marker}"
            )],
        )
        .unwrap_err();
        assert!(yaml_hdr_err.contains("credentials"), "{yaml_hdr_err}");
        assert!(!yaml_hdr_err.contains(marker), "{yaml_hdr_err}");

        // CLI credential component arg gives a credential-specific error and never echoes secrets.
        let cli_hdr_err = v(&[&format!(
            "--kubelet-arg=manifest-url-header=Authorization:Bearer {marker}"
        )])
        .unwrap_err();
        assert!(cli_hdr_err.contains("credentials"), "{cli_hdr_err}");
        assert!(!cli_hdr_err.contains(marker), "{cli_hdr_err}");

        for kubelet in [
            vec!["--kubelet-arg=root-dir=/x"],
            vec!["--kubelet-arg=--root-dir=/x"],
            vec!["--kubelet-arg", "root-dir=/x"],
            vec!["--kubelet-arg", "  root-dir=/x"],
            vec!["--kubelet-arg=root_dir=/x"],
            vec!["--kubelet-arg=root-dir+=/x"],
            vec!["--kubelet-arg=root-dir-=/x"],
            vec!["-kubelet-arg=container-runtime-endpoint=unix:///x"],
            vec!["--kubelet-arg=container_runtime_endpoint=unix:///x"],
            vec!["--kubelet-arg=image-service-endpoint=unix:///x"],
            vec!["--kubelet-arg=root-dir"],
            vec!["--kubelet-arg=node-ip=10.0.0.2"],
            vec!["--kubelet-arg=resolv-conf=/etc/alt.conf"],
            vec!["--kubelet-arg=tls-cert-file=/x"],
            vec!["--kubelet-arg=manifest-url-header=Authorization:Bearer x"],
            vec!["--kubelet-arg=manifest-url=https://user:pass@example.com/pod.yaml"],
            vec!["--kube-proxy-arg=proxy-mode=iptables"],
            vec!["--kube-proxy-arg", "proxy_mode=ipvs"],
            vec!["--kube-proxy-arg=kubeconfig=/tmp/kc"],
            vec!["--kube-proxy-arg=master=https://10.0.0.1:6443"],
            vec!["--kube-proxy-arg=config=/var/lib/kp.yaml"],
            vec!["--kube-proxy-arg=write-config-to=/tmp/x"],
            vec!["--kubelet-arg=config=/etc/kubelet.yaml"],
            vec!["--kubelet-arg=config-dir=/etc/kubelet.d"],
            vec!["--kubelet-arg=hostname-override=x"],
            vec!["--kube-apiserver-arg=secure-port=7443"],
            vec!["--kube-apiserver-arg= bind-address=0.0.0.0"],
            vec!["--kube-apiserver-arg=service-cluster-ip-range=10.9.0.0/16"],
            vec!["--kube-apiserver-arg=etcd-servers=https://etcd:2379"],
            vec!["--kube-controller-manager-arg=cluster-cidr=10.8.0.0/16"],
            vec!["--kube-controller-manager-arg=allocate_node_cidrs=false"],
            vec!["--kube-controller-manager-arg=kubeconfig=/tmp/kc"],
            vec!["--kube-controller-manager-arg=authorization-kubeconfig=/tmp/kc"],
            vec!["--kube-controller-arg=cluster-cidr=10.8.0.0/16"],
            vec!["--kube-controller-arg=kubeconfig=/tmp/kc"],
            vec!["--kube-scheduler-arg=kubeconfig=/tmp/kc"],
            vec!["--kube-scheduler-arg=config=/tmp/sched.yaml"],
            vec!["--kube-cloud-controller-manager-arg=kubeconfig=/tmp/kc"],
            vec!["--kube-cloud-controller-arg=kubeconfig=/tmp/kc"],
            vec!["--helm-controller-arg=kubeconfig=/tmp/kc"],
            vec!["--etcd-arg=data-dir=/tmp/etcd"],
            vec!["--etcd-arg=--data-dir=/tmp/etcd"],
            vec!["--etcd-arg", "  data-dir=/tmp/etcd"],
            vec!["--etcd-arg=wal-dir=/tmp/wal"],
            vec!["--etcd-arg=config-file=/tmp/etcd.yaml"],
            vec!["--etcd-arg=listen-client-urls=https://0.0.0.0:2379"],
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
            K8sMsg::Reset { images: false },
            K8sMsg::Reset { images: true },
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
            K8sMsg::ResetAccepted,
            K8sMsg::ResetDataCleared,
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
            K8sMsg::Reset { images: false },
            K8sMsg::Reset { images: true },
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
            K8sMsg::ResetAccepted,
            K8sMsg::ResetDataCleared,
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

    #[test]
    fn reset_wire_compatibility_and_validation() {
        // Default reset keeps an empty payload for backwards compatibility.
        let default_frame = K8sMsg::Reset { images: false }.encode();
        assert_eq!(&default_frame[HEADER..], &[]);
        assert_eq!(
            K8sMsg::decode(kind::RESET, &[]).unwrap(),
            K8sMsg::Reset { images: false }
        );
        assert_eq!(
            K8sMsg::decode(kind::RESET, &[0]).unwrap(),
            K8sMsg::Reset { images: false }
        );

        let with_images = K8sMsg::Reset { images: true }.encode();
        assert_eq!(&with_images[HEADER..], &[1]);
        assert_eq!(
            K8sMsg::decode(kind::RESET, &[1]).unwrap(),
            K8sMsg::Reset { images: true }
        );

        assert_eq!(
            K8sMsg::decode(kind::RESET, &[2]),
            Err(ProtoError::InvalidValue("k8s reset images", 2))
        );
        assert_eq!(
            K8sMsg::decode(kind::RESET, &[1, 0]),
            Err(ProtoError::TrailingBytes(1))
        );
    }
}
