//! Re-label an amd64-only OCI image as the guest's native platform so
//! Kubernetes can run it under the registered x86-64 emulator.
//!
//! containerd's CRI image service (through at least v2.4) resolves every
//! image against the node platform (`linux/arm64` and its arm variants), with
//! no configuration to accept other platforms. An image that only has a
//! `linux/amd64` manifest locally is therefore invisible to kubelet even though
//! the binfmt-registered emulator (Rosetta or qemu) runs its binaries fine, as
//! `docker run` does.
//!
//! Rewriting runs in two metadata-driven stages so neither guest PID 1 memory
//! nor the shared guest data disk ever holds a second copy of image layers:
//!
//! 1. [`prepare_rewrite_from_store`] traverses the OCI descriptor graph in
//!    `moby`'s content store within a strict aggregate metadata byte budget
//!    (reading only JSON metadata blobs, never layer payloads, and dropping
//!    parsed JSON trees before recursing), selects a locally complete
//!    `linux/amd64` variant, records the root index's export platform selector,
//!    and computes the rewritten `linux/<native_arch>` config and manifest.
//! 2. [`stream_rewritten_archive`] streams `ctr images export`'s tar directly
//!    into `ctr images import`: only layer blobs belonging to the planned
//!    manifest pass straight through in O(1) memory and zero temporary disk
//!    space (deduplicated in containerd's shared content store). Before
//!    emitting the rewritten metadata and `index.json`, it verifies that the
//!    exported `index.json` still targets the planned source digest, rejects
//!    digest-qualified name annotations, and confirms all expected layers were
//!    streamed.

use std::collections::BTreeSet;
use std::fmt;
use std::io::{self, Read, Write};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Aggregate cap on raw JSON metadata bytes read across the entire descriptor
/// graph walk and rewrite of one image. Generously above 4 MiB so large
/// configs/indexes succeed, while strictly bounding guest PID 1 heap even
/// across nested indexes.
pub const MAX_TOTAL_METADATA_BYTES: usize = 16 << 20;

/// Cap on the number of metadata blobs inspected during a single image walk so
/// pathological indexes cannot spin or exhaust PID 1 memory.
const MAX_METADATA_BLOBS: usize = 64;

/// Maximum OCI index nesting depth traversed per branch. Hitting this cap fails
/// only the overly deep branch so shallower sibling manifests remain usable.
const MAX_INDEX_DEPTH: usize = 4;

/// Maximum regular file size representable in a standard POSIX ustar header
/// (8 GiB - 1); larger layers use a GNU tar header.
const USTAR_MAX_SIZE: u64 = (1u64 << 33) - 1;

const MT_OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
const MT_DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
const MT_OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";

const ANN_CONTAINERD_IMAGE_NAME: &str = "io.containerd.image.name";
const ANN_OCI_REF_NAME: &str = "org.opencontainers.image.ref.name";

/// Read-only content-addressed blob lookup backed by containerd's `moby`
/// content store (or an in-memory map in tests).
pub trait BlobStore {
    /// True if `digest` (`sha256:<hex>`) exists locally.
    fn has_blob(&self, digest: &str) -> bool;

    /// Read a JSON metadata blob, failing without allocating past `max_bytes`.
    fn read_json_blob(&mut self, digest: &str, max_bytes: usize) -> Result<Vec<u8>, String>;
}

/// True when `name` is pinned to a content digest (e.g. `repo@sha256:...`,
/// `repo:tag@sha256:...`, or a bare `sha256:...` digest) rather than a mutable
/// tag. Rewritten manifests have a new digest and must never be published under
/// a digest-pinned reference.
pub fn is_digest_reference(name: &str) -> bool {
    if name.contains('@') {
        return true;
    }
    matches!(
        name.split_once(':'),
        Some(("sha256" | "sha384" | "sha512", hex))
            if !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit())
    )
}

/// Platforms listed by `ctr images ls` that the CRI accepts without help.
pub fn has_native_platform(platforms: &str, native_arch: &str) -> bool {
    let (want_arch, _) = normalize_arch_variant(native_arch, None);
    platform_list(platforms).any(|(os, arch, _)| {
        os.eq_ignore_ascii_case("linux") && normalize_arch_variant(arch, None).0 == want_arch
    })
}

/// True when `ctr images ls` lists a `linux/amd64` platform variant.
pub fn has_amd64_platform(platforms: &str) -> bool {
    platform_list(platforms).any(|(os, arch, _)| {
        os.eq_ignore_ascii_case("linux") && normalize_arch_variant(arch, None).0 == "amd64"
    })
}

fn platform_list(platforms: &str) -> impl Iterator<Item = (&str, &str, Option<&str>)> {
    platforms.split(',').filter_map(|p| {
        let mut parts = p.trim().split('/');
        let os = parts.next()?;
        let arch = parts.next()?;
        let variant = parts.next();
        Some((os, arch, variant))
    })
}

/// Normalize `(architecture, variant)` following containerd's
/// `platforms.Normalize` rules so `--platform` matches `platforms.Ordered`.
fn normalize_arch_variant(arch: &str, variant: Option<&str>) -> (String, Option<String>) {
    let arch = arch.to_ascii_lowercase();
    let variant = variant
        .map(str::to_ascii_lowercase)
        .filter(|v| !v.is_empty());
    match arch.as_str() {
        "amd64" | "x86_64" | "x86-64" => {
            let v = variant.filter(|v| v != "v1");
            ("amd64".into(), v)
        }
        "arm64" | "aarch64" => {
            let v = match variant.as_deref() {
                None | Some("8" | "v8" | "v8.0") => None,
                Some("9" | "9.0" | "v9.0") => Some("v9".into()),
                Some(other) => Some(other.into()),
            };
            ("arm64".into(), v)
        }
        _ => (arch, variant),
    }
}

fn format_amd64_platform(variant: Option<&str>) -> String {
    let (_, norm_var) = normalize_arch_variant("amd64", variant);
    match norm_var {
        Some(v) => format!("linux/amd64/{v}"),
        None => "linux/amd64".into(),
    }
}

/// Rank amd64 platforms so baseline `linux/amd64` is preferred over
/// microarchitecture variants when multiple amd64 variants are locally complete.
fn amd64_platform_rank(platform: &str) -> (u8, &str) {
    match platform {
        "linux/amd64" => (0, ""),
        "linux/amd64/v2" => (1, "v2"),
        "linux/amd64/v3" => (2, "v3"),
        "linux/amd64/v4" => (3, "v4"),
        other => (4, other),
    }
}

/// Pre-resolved metadata plan built purely from the source content store before
/// streaming layer blobs between `ctr export` and `ctr import`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewritePlan {
    /// Expected target digest of the top-level descriptor in `index.json`.
    pub source_digest: String,
    /// Platform selector to pass to `ctr images export --platform` so
    /// containerd's root-index filter selects the winning branch (e.g.
    /// `"linux/amd64"` or `"linux/amd64/v3"`).
    pub export_platform: String,
    /// Set of `sha256` hex layer digests belonging to the selected manifest.
    pub layer_hexes: BTreeSet<String>,
    /// Rewritten `linux/<native_arch>` config blob hex digest and bytes.
    pub new_config_hex: String,
    pub new_config_bytes: Vec<u8>,
    /// Rewritten single-platform manifest blob hex digest, bytes, and media type.
    pub new_manifest_hex: String,
    pub new_manifest_bytes: Vec<u8>,
    pub manifest_media_type: String,
    /// Target native architecture (e.g. `"arm64"`).
    pub native_arch: String,
}

/// Result of [`prepare_rewrite_from_store`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareRewrite {
    /// A locally complete `linux/amd64` manifest was found and rewritten.
    Ready(Box<RewritePlan>),
    /// The root blob exists, but no complete `linux/amd64` manifest is locally
    /// present in the content store.
    NotAvailable,
    /// The root digest itself is absent from the content store (concurrent
    /// retag or deletion).
    RootMissing,
}

/// Outcome of a successful [`stream_rewritten_archive`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewritten {
    /// Digest of the new single-platform manifest.
    pub manifest: String,
    /// Digest of the new image config (the image ID kubelet sees).
    pub config: String,
}

/// Error returned by [`stream_rewritten_archive`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewriteError {
    /// The exported archive's top-level digest differed from
    /// [`RewritePlan::source_digest`], indicating Docker retagged the image
    /// concurrently. No `index.json` or rewritten metadata was written.
    SourceChanged { expected: String, actual: String },
    /// Any other archive, manifest, or I/O error.
    Other(String),
}

impl fmt::Display for RewriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceChanged { expected, actual } => {
                write!(
                    f,
                    "source image digest changed before export (expected {expected}, got {actual})"
                )
            }
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

/// Resolve a locally complete `linux/amd64` manifest from `store` starting at
/// `root_digest`, rewrite its config and manifest for `linux/<native_arch>`,
/// and return a metadata-only [`RewritePlan`] without reading or copying any
/// layer payloads.
pub fn prepare_rewrite_from_store(
    root_digest: &str,
    native_arch: &str,
    store: &mut impl BlobStore,
) -> Result<PrepareRewrite, String> {
    if !store.has_blob(root_digest) {
        return Ok(PrepareRewrite::RootMissing);
    }
    let mut budget = BudgetedStore::new(store, MAX_TOTAL_METADATA_BYTES, MAX_METADATA_BLOBS);
    let mut depth_err: Option<String> = None;
    let candidate = walk_amd64_manifest(root_digest, None, None, &mut budget, 0, &mut depth_err)?;
    let Some(cand) = candidate else {
        if let Some(err) = depth_err {
            return Err(err);
        }
        return Ok(PrepareRewrite::NotAvailable);
    };

    let plan = build_rewrite_plan(root_digest, native_arch, cand)?;
    Ok(PrepareRewrite::Ready(Box::new(plan)))
}

fn build_rewrite_plan(
    root_digest: &str,
    native_arch: &str,
    cand: Candidate,
) -> Result<RewritePlan, String> {
    // Parse and rewrite config first, then drop the Value immediately before
    // parsing the manifest so peak parsed JSON stays at one document.
    let (new_config_hex, new_config_bytes) = {
        let mut config: Value = serde_json::from_slice(&cand.config_bytes)
            .map_err(|e| format!("parse image config: {e}"))?;
        let config_obj = config
            .as_object_mut()
            .ok_or("image config is not an object")?;
        config_obj.insert("architecture".into(), json!(native_arch));
        config_obj.remove("variant");
        let bytes = serde_json::to_vec(&config).map_err(|e| e.to_string())?;
        let hex = sha256(&bytes);
        (hex, bytes)
    };

    let (new_manifest_hex, new_manifest_bytes, manifest_media_type, layer_hexes) = {
        let mut manifest: Value = serde_json::from_slice(&cand.manifest_bytes)
            .map_err(|e| format!("parse manifest: {e}"))?;
        let config_desc = manifest
            .get_mut("config")
            .and_then(Value::as_object_mut)
            .ok_or("manifest config is not an object")?;
        config_desc.insert("digest".into(), json!(format!("sha256:{new_config_hex}")));
        config_desc.insert("size".into(), json!(new_config_bytes.len()));
        // Drop any OCI inline `data` bytes so the rewritten descriptor cannot
        // carry the original amd64 config bytes alongside the new digest/size.
        config_desc.remove("data");

        let mut layer_hexes = BTreeSet::new();
        if let Some(layers) = manifest.get("layers").and_then(Value::as_array) {
            for layer in layers {
                let digest = layer
                    .get("digest")
                    .and_then(Value::as_str)
                    .ok_or("layer descriptor has no digest")?;
                let hex = digest
                    .strip_prefix("sha256:")
                    .ok_or_else(|| format!("unsupported digest {digest}"))?;
                layer_hexes.insert(hex.to_string());
            }
        }
        let media_type = manifest
            .get("mediaType")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(cand.desc_media_type)
            .unwrap_or_else(|| MT_OCI_MANIFEST.to_string());
        let bytes = serde_json::to_vec(&manifest).map_err(|e| e.to_string())?;
        let hex = sha256(&bytes);
        (hex, bytes, media_type, layer_hexes)
    };

    Ok(RewritePlan {
        source_digest: root_digest.to_string(),
        export_platform: cand.export_platform,
        layer_hexes,
        new_config_hex,
        new_config_bytes,
        new_manifest_hex,
        new_manifest_bytes,
        manifest_media_type,
        native_arch: native_arch.to_string(),
    })
}

/// Writer wrapper that suppresses `tar::Builder`'s automatic zero-block EOF
/// trailer on drop when [`stream_rewritten_archive`] aborts with an error, so
/// an aborted stream never finishes a tar archive.
struct GuardedWriter<W: Write> {
    inner: W,
    suppress_writes: bool,
}

impl<W: Write> Write for GuardedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.suppress_writes {
            return Err(io::Error::other("archive stream aborted"));
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.suppress_writes {
            return Ok(());
        }
        self.inner.flush()
    }
}

/// Stream an OCI tar archive from `input` (`ctr images export`) to `output`
/// (`ctr images import`) using a pre-resolved [`RewritePlan`].
///
/// Only layer blobs in `plan.layer_hexes` are copied straight from `input` to
/// `output` in O(1) memory and zero temporary disk space. Old metadata blobs
/// and `manifest.json` are skipped. Before emitting the rewritten config,
/// manifest, `oci-layout`, and `index.json`, the stream's `index.json` is
/// verified against `plan.source_digest` and checked for digest-qualified
/// image name annotations; on any mismatch or error, `index.json` and the tar
/// EOF trailer are never written so `ctr images import` cannot publish a broken
/// or mislabelled tag.
pub fn stream_rewritten_archive(
    input: impl Read,
    output: impl Write,
    plan: &RewritePlan,
) -> Result<Rewritten, RewriteError> {
    let mut out = tar::Builder::new(GuardedWriter {
        inner: output,
        suppress_writes: false,
    });
    out.mode(tar::HeaderMode::Deterministic);
    let res = stream_rewritten_archive_inner(input, &mut out, plan);
    if res.is_err() {
        out.get_mut().suppress_writes = true;
        return res;
    }
    out.into_inner()
        .and_then(|mut w| w.inner.flush())
        .map_err(|e| RewriteError::Other(format!("finish archive: {e}")))?;
    res
}

fn stream_rewritten_archive_inner<W: Write>(
    input: impl Read,
    out: &mut tar::Builder<W>,
    plan: &RewritePlan,
) -> Result<Rewritten, RewriteError> {
    let mut seen_layers: BTreeSet<String> = BTreeSet::new();
    let mut index_bytes: Option<Vec<u8>> = None;

    let mut archive = tar::Archive::new(input);
    let entries = archive
        .entries()
        .map_err(|e| RewriteError::Other(format!("read export archive: {e}")))?;
    for entry in entries {
        let mut entry =
            entry.map_err(|e| RewriteError::Other(format!("read export archive entry: {e}")))?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry
            .path()
            .map_err(|e| RewriteError::Other(format!("export archive path: {e}")))?
            .to_string_lossy()
            .into_owned();
        let path = path.trim_start_matches("./");
        if path == "index.json" {
            index_bytes = Some(
                read_bounded(&mut entry, MAX_TOTAL_METADATA_BYTES as u64, "index.json")
                    .map_err(RewriteError::Other)?,
            );
        } else if let Some(hex) = path.strip_prefix("blobs/sha256/")
            && plan.layer_hexes.contains(hex)
            && seen_layers.insert(hex.to_string())
        {
            let size = entry.size();
            append_stream(out, &format!("blobs/sha256/{hex}"), size, &mut entry)
                .map_err(RewriteError::Other)?;
        }
        // Superseded amd64 index/manifest/config blobs and unreferenced
        // entries are skipped directly without buffering or disk spooling.
    }

    let index_bytes = index_bytes
        .ok_or_else(|| RewriteError::Other("export archive has no index.json".into()))?;
    let index: Value = serde_json::from_slice(&index_bytes)
        .map_err(|e| RewriteError::Other(format!("parse index.json: {e}")))?;
    let top = index
        .get("manifests")
        .and_then(Value::as_array)
        .and_then(|m| m.first())
        .ok_or_else(|| RewriteError::Other("index.json lists no manifests".into()))?;
    let top_digest = top
        .get("digest")
        .and_then(Value::as_str)
        .ok_or_else(|| RewriteError::Other("descriptor has no digest".into()))?;
    if top_digest != plan.source_digest {
        return Err(RewriteError::SourceChanged {
            expected: plan.source_digest.clone(),
            actual: top_digest.to_string(),
        });
    }
    let sanitized_annotations =
        validate_top_annotations(top.get("annotations")).map_err(RewriteError::Other)?;

    if let Some(missing) = plan.layer_hexes.difference(&seen_layers).next() {
        return Err(RewriteError::Other(format!(
            "layer blob sha256:{missing} missing from export archive"
        )));
    }

    let mut desc = json!({
        "mediaType": plan.manifest_media_type,
        "digest": format!("sha256:{}", plan.new_manifest_hex),
        "size": plan.new_manifest_bytes.len(),
        "platform": {"architecture": plan.native_arch, "os": "linux"},
    });
    if let Some(annotations) = sanitized_annotations {
        desc["annotations"] = annotations;
    }
    let new_index = json!({
        "schemaVersion": 2,
        "mediaType": MT_OCI_INDEX,
        "manifests": [desc],
    });
    let new_index_bytes =
        serde_json::to_vec(&new_index).map_err(|e| RewriteError::Other(e.to_string()))?;

    append(
        out,
        &format!("blobs/sha256/{}", plan.new_config_hex),
        &plan.new_config_bytes,
    )
    .map_err(RewriteError::Other)?;
    append(
        out,
        &format!("blobs/sha256/{}", plan.new_manifest_hex),
        &plan.new_manifest_bytes,
    )
    .map_err(RewriteError::Other)?;
    append(out, "oci-layout", br#"{"imageLayoutVersion":"1.0.0"}"#).map_err(RewriteError::Other)?;
    append(out, "index.json", &new_index_bytes).map_err(RewriteError::Other)?;

    Ok(Rewritten {
        manifest: format!("sha256:{}", plan.new_manifest_hex),
        config: format!("sha256:{}", plan.new_config_hex),
    })
}

/// Ensure top-level `index.json` descriptor annotations do not publish the
/// rewritten manifest under an original digest-qualified image reference.
fn validate_top_annotations(annotations: Option<&Value>) -> Result<Option<Value>, String> {
    let Some(val) = annotations else {
        return Ok(None);
    };
    let Some(obj) = val.as_object() else {
        return Ok(None);
    };
    for key in [ANN_CONTAINERD_IMAGE_NAME, ANN_OCI_REF_NAME] {
        if let Some(name) = obj.get(key).and_then(Value::as_str)
            && is_digest_reference(name)
        {
            return Err(format!(
                "refusing to rewrite digest-qualified image reference {name}; tag it locally first"
            ));
        }
    }
    Ok(Some(Value::Object(obj.clone())))
}

/// Wrapper enforcing an aggregate byte and blob-count cap across every metadata
/// read performed while traversing an image's descriptor graph.
struct BudgetedStore<'a, S: BlobStore> {
    inner: &'a mut S,
    remaining_bytes: usize,
    remaining_blobs: usize,
    total_limit: usize,
}

impl<'a, S: BlobStore> BudgetedStore<'a, S> {
    fn new(inner: &'a mut S, byte_limit: usize, blob_limit: usize) -> Self {
        Self {
            inner,
            remaining_bytes: byte_limit,
            remaining_blobs: blob_limit,
            total_limit: byte_limit,
        }
    }

    fn has_blob(&self, digest: &str) -> bool {
        self.inner.has_blob(digest)
    }

    fn read_json_blob(&mut self, digest: &str) -> Result<Vec<u8>, String> {
        if self.remaining_blobs == 0 {
            return Err(format!(
                "metadata walk exceeded {MAX_METADATA_BLOBS}-blob limit while reading {digest}"
            ));
        }
        if self.remaining_bytes == 0 {
            return Err(format!(
                "metadata walk exceeded {}-byte aggregate limit while reading {digest}",
                self.total_limit
            ));
        }
        self.remaining_blobs -= 1;
        let data = self
            .inner
            .read_json_blob(digest, self.remaining_bytes)
            .map_err(|e| {
                if e.contains("exceeded") {
                    format!(
                        "metadata walk exceeded {}-byte aggregate limit while reading {digest}: {e}",
                        self.total_limit
                    )
                } else {
                    e
                }
            })?;
        if data.len() > self.remaining_bytes {
            return Err(format!(
                "metadata walk exceeded {}-byte aggregate limit while reading {digest}",
                self.total_limit
            ));
        }
        self.remaining_bytes -= data.len();
        Ok(data)
    }
}

#[derive(Debug, Clone)]
struct PlatformDesc {
    os: Option<String>,
    architecture: Option<String>,
    variant: Option<String>,
}

impl PlatformDesc {
    fn from_value(v: &Value) -> Option<Self> {
        let obj = v.as_object()?;
        Some(Self {
            os: obj.get("os").and_then(Value::as_str).map(str::to_string),
            architecture: obj
                .get("architecture")
                .and_then(Value::as_str)
                .map(str::to_string),
            variant: obj
                .get("variant")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    fn allows_amd64(&self) -> bool {
        if let Some(os) = &self.os
            && !os.eq_ignore_ascii_case("linux")
        {
            return false;
        }
        if let Some(arch) = &self.architecture
            && normalize_arch_variant(arch, None).0 != "amd64"
        {
            return false;
        }
        true
    }

    /// Explicit normalized `linux/amd64[/variant]` selector when this
    /// descriptor declares an architecture; `None` when omitted.
    fn explicit_amd64_selector(&self) -> Option<String> {
        self.architecture.as_ref()?;
        Some(format_amd64_platform(self.variant.as_deref()))
    }
}

struct ChildDescriptor {
    digest: String,
    media_type: Option<String>,
    platform: Option<PlatformDesc>,
}

enum ParsedBlob {
    Index(Vec<ChildDescriptor>),
    Manifest {
        config_digest: Option<String>,
        layers: Vec<Option<String>>,
    },
}

struct Candidate {
    desc_media_type: Option<String>,
    manifest_bytes: Vec<u8>,
    config_bytes: Vec<u8>,
    /// Leaf platform used to prefer baseline `linux/amd64` over microarch variants.
    leaf_platform: String,
    /// Platform selector passed to `ctr images export --platform`: preserves
    /// the root index's immediate descriptor platform when explicit.
    export_platform: String,
}

/// Parse a JSON blob into a compact [`ParsedBlob`] and immediately drop the
/// intermediate `serde_json::Value` so parent JSON trees are never held across
/// recursive calls.
fn classify_blob(raw: &[u8], desc_media_type: Option<&str>) -> Option<ParsedBlob> {
    let parsed: Value = serde_json::from_slice(raw).ok()?;
    let media_type = desc_media_type
        .or_else(|| parsed.get("mediaType").and_then(Value::as_str))
        .unwrap_or("");
    let is_index = media_type == MT_OCI_INDEX
        || media_type == MT_DOCKER_LIST
        || (media_type.is_empty() && parsed.get("manifests").is_some());

    if is_index {
        let children = parsed
            .get("manifests")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|child| {
                        let digest = child.get("digest")?.as_str()?.to_string();
                        let media_type = child
                            .get("mediaType")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        let platform = child.get("platform").and_then(PlatformDesc::from_value);
                        Some(ChildDescriptor {
                            digest,
                            media_type,
                            platform,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        return Some(ParsedBlob::Index(children));
    }

    let config_digest = parsed
        .pointer("/config/digest")
        .and_then(Value::as_str)
        .map(str::to_string);
    let layers = parsed
        .get("layers")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|l| l.get("digest").and_then(Value::as_str).map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(ParsedBlob::Manifest {
        config_digest,
        layers,
    })
}

/// Parse only `(os, architecture, variant)` from a config blob and immediately
/// drop the `serde_json::Value`.
fn inspect_config_platform(raw: &[u8]) -> Option<(String, String, Option<String>)> {
    let parsed: Value = serde_json::from_slice(raw).ok()?;
    let os = parsed.get("os")?.as_str()?.to_string();
    let arch = parsed.get("architecture")?.as_str()?.to_string();
    let variant = parsed
        .get("variant")
        .and_then(Value::as_str)
        .map(str::to_string);
    Some((os, arch, variant))
}

/// Walk `digest` (a manifest, or an OCI index / Docker manifest list, including
/// nested and platform-less descriptors) to find a locally complete
/// `linux/amd64` image manifest.
///
/// Hitting [`MAX_INDEX_DEPTH`] fails only the current branch (recording the
/// first depth error in `depth_err`) so shallower sibling manifests are still
/// inspected. At `depth == 0`, the winning branch's `export_platform` preserves
/// the root index's immediate child platform when declared so containerd's
/// top-level `--platform` export filter matches it.
fn walk_amd64_manifest<S: BlobStore>(
    digest: &str,
    desc_media_type: Option<&str>,
    desc_platform: Option<&PlatformDesc>,
    store: &mut BudgetedStore<'_, S>,
    depth: usize,
    depth_err: &mut Option<String>,
) -> Result<Option<Candidate>, String> {
    if !store.has_blob(digest) {
        return Ok(None);
    }
    let raw = store.read_json_blob(digest)?;
    let Some(parsed) = classify_blob(&raw, desc_media_type) else {
        return Ok(None);
    };

    match parsed {
        ParsedBlob::Manifest {
            config_digest,
            layers,
        } => resolve_leaf_manifest(
            raw,
            config_digest,
            layers,
            desc_media_type,
            desc_platform,
            store,
        ),
        ParsedBlob::Index(children) => {
            // Drop `raw` immediately before recursing so ancestor index bytes
            // are never retained on the stack.
            drop(raw);
            if depth >= MAX_INDEX_DEPTH {
                if depth_err.is_none() {
                    *depth_err = Some(format!("{digest}: image indexes nested too deeply"));
                }
                return Ok(None);
            }
            let mut best: Option<Candidate> = None;
            for child in children {
                if !store.has_blob(&child.digest) {
                    continue;
                }
                if child.platform.as_ref().is_some_and(|p| !p.allows_amd64()) {
                    continue;
                }
                let outer_selector = if depth == 0 {
                    child
                        .platform
                        .as_ref()
                        .and_then(PlatformDesc::explicit_amd64_selector)
                } else {
                    None
                };
                let Some(mut found) = walk_amd64_manifest(
                    &child.digest,
                    child.media_type.as_deref(),
                    child.platform.as_ref(),
                    store,
                    depth + 1,
                    depth_err,
                )?
                else {
                    continue;
                };
                if let Some(outer) = outer_selector {
                    found.export_platform = outer;
                }
                let better = best.as_ref().is_none_or(|cur| {
                    (
                        amd64_platform_rank(&found.leaf_platform),
                        amd64_platform_rank(&found.export_platform),
                    ) < (
                        amd64_platform_rank(&cur.leaf_platform),
                        amd64_platform_rank(&cur.export_platform),
                    )
                });
                if better {
                    let is_ideal = found.leaf_platform == "linux/amd64"
                        && found.export_platform == "linux/amd64";
                    best = Some(found);
                    if is_ideal {
                        break;
                    }
                }
            }
            Ok(best)
        }
    }
}

fn resolve_leaf_manifest<S: BlobStore>(
    manifest_bytes: Vec<u8>,
    config_digest: Option<String>,
    layers: Vec<Option<String>>,
    desc_media_type: Option<&str>,
    desc_platform: Option<&PlatformDesc>,
    store: &mut BudgetedStore<'_, S>,
) -> Result<Option<Candidate>, String> {
    let Some(config_digest) = config_digest else {
        return Ok(None);
    };
    if !store.has_blob(&config_digest) {
        return Ok(None);
    }
    for layer in &layers {
        let Some(layer_digest) = layer.as_deref() else {
            return Ok(None);
        };
        if !layer_digest.starts_with("sha256:") || !store.has_blob(layer_digest) {
            return Ok(None);
        }
    }

    let config_bytes = store.read_json_blob(&config_digest)?;
    let Some((cfg_os, cfg_arch, cfg_variant)) = inspect_config_platform(&config_bytes) else {
        return Ok(None);
    };
    if !cfg_os.eq_ignore_ascii_case("linux") || normalize_arch_variant(&cfg_arch, None).0 != "amd64"
    {
        return Ok(None);
    }

    let leaf_platform = match desc_platform.and_then(PlatformDesc::explicit_amd64_selector) {
        Some(sel) => sel,
        None => format_amd64_platform(cfg_variant.as_deref()),
    };

    Ok(Some(Candidate {
        desc_media_type: desc_media_type.map(str::to_string),
        manifest_bytes,
        config_bytes,
        export_platform: leaf_platform.clone(),
        leaf_platform,
    }))
}

fn read_bounded(r: &mut impl Read, limit: u64, what: &str) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut limited = r.take(limit + 1);
    limited
        .read_to_end(&mut buf)
        .map_err(|e| format!("read {what}: {e}"))?;
    if buf.len() as u64 > limit {
        return Err(format!("{what} exceeds {limit}-byte limit"));
    }
    Ok(buf)
}

fn append_stream<W: Write>(
    out: &mut tar::Builder<W>,
    path: &str,
    size: u64,
    data: &mut impl Read,
) -> Result<(), String> {
    let mut header = if size <= USTAR_MAX_SIZE {
        tar::Header::new_ustar()
    } else {
        tar::Header::new_gnu()
    };
    header.set_size(size);
    header.set_mode(0o644);
    header.set_entry_type(tar::EntryType::Regular);
    out.append_data(&mut header, path, data)
        .map_err(|e| format!("write {path}: {e}"))
}

fn append<W: Write>(out: &mut tar::Builder<W>, path: &str, data: &[u8]) -> Result<(), String> {
    append_stream(out, path, data.len() as u64, &mut io::Cursor::new(data))
}

fn sha256(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    const MT_DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";

    fn add(b: &mut tar::Builder<Vec<u8>>, path: &str, data: &[u8]) {
        append(b, path, data).unwrap();
    }

    fn add_blob(b: &mut tar::Builder<Vec<u8>>, store: &mut MemStore, data: &[u8]) -> String {
        let hex = sha256(data);
        add(b, &format!("blobs/sha256/{hex}"), data);
        let digest = format!("sha256:{hex}");
        store.0.insert(digest.clone(), data.to_vec());
        digest
    }

    struct MemStore(HashMap<String, Vec<u8>>);

    impl MemStore {
        fn new() -> Self {
            Self(HashMap::new())
        }

        fn insert(&mut self, data: Vec<u8>) -> String {
            let digest = format!("sha256:{}", sha256(&data));
            self.0.insert(digest.clone(), data);
            digest
        }
    }

    impl BlobStore for MemStore {
        fn has_blob(&self, digest: &str) -> bool {
            self.0.contains_key(digest)
        }

        fn read_json_blob(&mut self, digest: &str, max_bytes: usize) -> Result<Vec<u8>, String> {
            let data = self
                .0
                .get(digest)
                .ok_or_else(|| format!("missing {digest}"))?;
            if data.len() > max_bytes {
                return Err(format!(
                    "blob {digest} ({} bytes) exceeded {max_bytes}-byte limit",
                    data.len()
                ));
            }
            Ok(data.clone())
        }
    }

    /// Build both the `moby` content store and the corresponding `ctr export`
    /// archive for an amd64 image (with or without an outer OCI index).
    fn amd64_fixture(with_index: bool) -> (MemStore, Vec<u8>, String, Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut store = MemStore::new();
        let mut b = tar::Builder::new(Vec::new());
        let small = b"\x1f\x8bsmall gzip layer".to_vec();
        let json_like = b"{json-looking small layer}".to_vec();
        let large = vec![7u8; (4 << 20) + 1];
        let small_d = add_blob(&mut b, &mut store, &small);
        let json_like_d = add_blob(&mut b, &mut store, &json_like);
        let large_d = add_blob(&mut b, &mut store, &large);
        let config = serde_json::to_vec(&json!({
            "architecture": "amd64",
            "variant": "v3",
            "os": "linux",
            "config": {"Entrypoint": ["/app"]},
            "rootfs": {"type": "layers", "diff_ids": ["sha256:00"]},
        }))
        .unwrap();
        let config_d = add_blob(&mut b, &mut store, &config);
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_DOCKER_MANIFEST,
            "config": {"mediaType": "application/vnd.docker.container.image.v1+json",
                       "digest": config_d, "size": config.len()},
            "layers": [
                {"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "digest": small_d, "size": small.len()},
                {"mediaType": "application/vnd.docker.image.rootfs.diff.tar",
                 "digest": json_like_d, "size": json_like.len()},
                {"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "digest": large_d, "size": large.len()},
            ],
        }))
        .unwrap();
        let manifest_d = add_blob(&mut b, &mut store, &manifest);
        let annotations = json!({"io.containerd.image.name": "docker.io/library/app:dev"});
        let (top, top_digest) = if with_index {
            let idx = serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [
                    {"mediaType": MT_DOCKER_MANIFEST, "digest": format!("sha256:{}", "e".repeat(64)),
                     "size": 1, "platform": {"architecture": "amd64", "os": "linux"}},
                    {"mediaType": MT_OCI_MANIFEST, "digest": format!("sha256:{}", "d".repeat(64)),
                     "size": 1, "platform": {"architecture": "unknown", "os": "unknown"}},
                    {"mediaType": MT_DOCKER_MANIFEST, "digest": manifest_d, "size": manifest.len(),
                     "platform": {"architecture": "amd64", "os": "linux"}},
                ],
            }))
            .unwrap();
            let idx_d = add_blob(&mut b, &mut store, &idx);
            (
                json!({"mediaType": MT_OCI_INDEX, "digest": idx_d, "size": idx.len(),
                       "annotations": annotations}),
                idx_d,
            )
        } else {
            (
                json!({"mediaType": MT_DOCKER_MANIFEST, "digest": manifest_d, "size": manifest.len(),
                       "annotations": annotations}),
                manifest_d,
            )
        };
        add(&mut b, "manifest.json", b"[]");
        add(&mut b, "oci-layout", br#"{"imageLayoutVersion":"1.0.0"}"#);
        let index = serde_json::to_vec(&json!({"schemaVersion": 2, "manifests": [top]})).unwrap();
        add(&mut b, "index.json", &index);
        (
            store,
            b.into_inner().unwrap(),
            top_digest,
            small,
            json_like,
            large,
        )
    }

    fn read_archive(data: &[u8]) -> HashMap<String, Vec<u8>> {
        let mut a = tar::Archive::new(data);
        a.entries()
            .unwrap()
            .map(|e| {
                let mut e = e.unwrap();
                let path = e.path().unwrap().to_string_lossy().into_owned();
                let mut buf = Vec::new();
                e.read_to_end(&mut buf).unwrap();
                (path, buf)
            })
            .collect()
    }

    fn check(with_index: bool) {
        let (mut store, input, top_digest, small, json_like, large) = amd64_fixture(with_index);
        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&top_digest, "arm64", &mut store).unwrap()
        else {
            panic!("expected PrepareRewrite::Ready");
        };
        let mut output = Vec::new();
        let r = stream_rewritten_archive(input.as_slice(), &mut output, &plan).unwrap();
        let files = read_archive(&output);

        assert!(!files.contains_key("manifest.json"));
        assert!(files.contains_key("oci-layout"));
        for (path, data) in &files {
            if let Some(hex) = path.strip_prefix("blobs/sha256/") {
                assert_eq!(sha256(data), hex, "{path} content does not match its name");
            }
        }
        let blob_count = files
            .keys()
            .filter(|k| k.starts_with("blobs/sha256/"))
            .count();
        assert_eq!(blob_count, 5);

        let index: Value = serde_json::from_slice(&files["index.json"]).unwrap();
        let descs = index["manifests"].as_array().unwrap();
        assert_eq!(descs.len(), 1);
        let desc = &descs[0];
        assert_eq!(desc["digest"], r.manifest);
        assert_eq!(desc["mediaType"], MT_DOCKER_MANIFEST);
        assert_eq!(
            desc["platform"],
            json!({"architecture": "arm64", "os": "linux"})
        );
        assert_eq!(
            desc["annotations"]["io.containerd.image.name"],
            "docker.io/library/app:dev"
        );

        let blob_of = |d: &str| &files[&format!("blobs/sha256/{}", &d[7..])];
        let manifest_bytes = blob_of(&r.manifest);
        assert_eq!(desc["size"], manifest_bytes.len());
        let manifest: Value = serde_json::from_slice(manifest_bytes).unwrap();
        assert_eq!(manifest["config"]["digest"], r.config);
        let config_bytes = blob_of(&r.config);
        assert_eq!(manifest["config"]["size"], config_bytes.len());
        let config: Value = serde_json::from_slice(config_bytes).unwrap();
        assert_eq!(config["architecture"], "arm64");
        assert_eq!(config["os"], "linux");
        assert!(config.get("variant").is_none());
        assert_eq!(config["config"]["Entrypoint"], json!(["/app"]));

        assert_eq!(blob_of(&format!("sha256:{}", sha256(&small))), &small);
        assert_eq!(
            blob_of(&format!("sha256:{}", sha256(&json_like))),
            &json_like
        );
        assert_eq!(blob_of(&format!("sha256:{}", sha256(&large))), &large);
    }

    #[test]
    fn rewrites_amd64_index_to_native_manifest() {
        check(true);
    }

    #[test]
    fn rewrites_amd64_single_manifest() {
        check(false);
    }

    /// Regression test: layer payloads must stream straight to `output` with
    /// zero temporary disk copy and without buffering until `index.json`
    /// arrives (so an image larger than remaining disk headroom still mirrors).
    #[test]
    fn streams_large_layers_without_disk_copy_or_buffering() {
        let mut store = MemStore::new();
        let mut b = tar::Builder::new(Vec::new());
        // A 12 MiB `{`-prefixed layer (representing an image whose layers exceed
        // temporary disk headroom, while metadata is only a few hundred bytes).
        let mut huge_layer = vec![b'{'; 12 << 20];
        huge_layer[1] = b'z';
        let layer_d = add_blob(&mut b, &mut store, &huge_layer);
        let cfg = serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap();
        let cfg_d = add_blob(&mut b, &mut store, &cfg);
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": cfg_d, "size": cfg.len()},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                        "digest": layer_d, "size": huge_layer.len()}],
        }))
        .unwrap();
        let manifest_d = add_blob(&mut b, &mut store, &manifest);
        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "manifests": [{
                "mediaType": MT_OCI_MANIFEST,
                "digest": manifest_d,
                "size": manifest.len(),
                "annotations": {"io.containerd.image.name": "docker.io/library/huge:dev"},
            }]
        }))
        .unwrap();
        // Record the byte offset in the tar stream right before `index.json`.
        let before_index_pos = b.get_ref().len();
        add(&mut b, "index.json", &index);
        let input = b.into_inner().unwrap();

        // Store that panics if `prepare_rewrite_from_store` ever tries to read
        // layer bytes (metadata-only lookup must only inspect `has_blob`).
        struct MetadataOnlyStore {
            inner: MemStore,
            layer_digest: String,
        }
        impl BlobStore for MetadataOnlyStore {
            fn has_blob(&self, digest: &str) -> bool {
                self.inner.has_blob(digest)
            }
            fn read_json_blob(
                &mut self,
                digest: &str,
                max_bytes: usize,
            ) -> Result<Vec<u8>, String> {
                assert_ne!(digest, self.layer_digest, "must never read layer payload");
                self.inner.read_json_blob(digest, max_bytes)
            }
        }
        let mut meta_store = MetadataOnlyStore {
            inner: store,
            layer_digest: layer_d.clone(),
        };
        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&manifest_d, "arm64", &mut meta_store).unwrap()
        else {
            panic!("expected Ready");
        };

        // Observe written bytes at the moment `input` crosses `before_index_pos`:
        // the full 12 MiB layer must ALREADY have been streamed to `output`
        // (proving neither RAM buffering nor disk spooling held the layer).
        let bytes_written = Arc::new(Mutex::new(0usize));
        let observed_before_index = Arc::new(Mutex::new(None::<usize>));

        struct CountingWriter {
            written: Arc<Mutex<usize>>,
        }
        impl Write for CountingWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                *self.written.lock().unwrap() += buf.len();
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        struct ObservingReader<'a> {
            data: &'a [u8],
            pos: usize,
            mark: usize,
            written: Arc<Mutex<usize>>,
            observed: Arc<Mutex<Option<usize>>>,
        }
        impl Read for ObservingReader<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.pos >= self.mark && self.observed.lock().unwrap().is_none() {
                    *self.observed.lock().unwrap() = Some(*self.written.lock().unwrap());
                }
                let avail = &self.data[self.pos..];
                let n = avail.len().min(buf.len());
                buf[..n].copy_from_slice(&avail[..n]);
                self.pos += n;
                Ok(n)
            }
        }

        let reader = ObservingReader {
            data: &input,
            pos: 0,
            mark: before_index_pos,
            written: Arc::clone(&bytes_written),
            observed: Arc::clone(&observed_before_index),
        };
        let writer = CountingWriter {
            written: Arc::clone(&bytes_written),
        };

        let r = stream_rewritten_archive(reader, writer, &plan).unwrap();
        assert!(r.manifest.starts_with("sha256:"));
        let streamed_before_index = observed_before_index.lock().unwrap().unwrap();
        assert!(
            streamed_before_index >= huge_layer.len(),
            "layer ({} bytes) must stream straight through before index.json (only {streamed_before_index} bytes written)",
            huge_layer.len()
        );
    }

    #[test]
    fn rejects_stale_expected_source_digest_without_emitting_valid_index() {
        let (mut store, input, top_digest, _, _, _) = amd64_fixture(true);
        let PrepareRewrite::Ready(mut plan) =
            prepare_rewrite_from_store(&top_digest, "arm64", &mut store).unwrap()
        else {
            panic!("expected Ready");
        };
        plan.source_digest =
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".into();

        let mut output = Vec::new();
        let err = stream_rewritten_archive(input.as_slice(), &mut output, &plan).unwrap_err();
        assert_eq!(
            err,
            RewriteError::SourceChanged {
                expected: plan.source_digest.clone(),
                actual: top_digest,
            }
        );
        // Output must NOT contain index.json or rewritten metadata.
        let mut a = tar::Archive::new(output.as_slice());
        for entry in a.entries().unwrap().flatten() {
            let p = entry.path().unwrap().to_string_lossy().into_owned();
            assert_ne!(p, "index.json", "stale digest must never emit index.json");
            assert_ne!(p, format!("blobs/sha256/{}", plan.new_manifest_hex));
        }
    }

    #[test]
    fn strips_stale_inline_config_data_from_rewritten_manifest() {
        let mut store = MemStore::new();
        let config = serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap();
        let config_d = store.insert(config.clone());
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_d,
                "size": config.len(),
                "data": "eyJhcmNoaXRlY3R1cmUiOiJhbWQ2NCIsIm9zIjoibGludXgifQ==",
            },
            "layers": [],
        }))
        .unwrap();
        let manifest_d = store.insert(manifest);

        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&manifest_d, "arm64", &mut store).unwrap()
        else {
            panic!("expected Ready");
        };
        let new_manifest: Value = serde_json::from_slice(&plan.new_manifest_bytes).unwrap();
        assert_eq!(
            new_manifest["config"]["digest"],
            format!("sha256:{}", plan.new_config_hex)
        );
        assert!(
            new_manifest["config"].get("data").is_none(),
            "rewritten config descriptor must omit stale inline data: {new_manifest}"
        );
    }

    #[test]
    fn rewrites_config_with_long_leading_whitespace_and_over_4_mib() {
        let mut store = MemStore::new();
        let mut config = vec![b' '; 256];
        let big_label = "x".repeat((4 << 20) + 16_384);
        let body = serde_json::to_vec(&json!({
            "architecture": "amd64",
            "os": "linux",
            "config": {"Labels": {"big": big_label}},
        }))
        .unwrap();
        config.extend_from_slice(&body);
        assert!(config.len() > (4 << 20));
        let config_d = store.insert(config.clone());
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_d,
                "size": config.len(),
            },
            "layers": [],
        }))
        .unwrap();
        let manifest_d = store.insert(manifest);

        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&manifest_d, "arm64", &mut store).unwrap()
        else {
            panic!("expected Ready");
        };
        assert_eq!(sha256(&plan.new_config_bytes), plan.new_config_hex);
        let new_cfg: Value = serde_json::from_slice(&plan.new_config_bytes).unwrap();
        assert_eq!(new_cfg["architecture"], "arm64");
        assert_eq!(
            new_cfg["config"]["Labels"]["big"].as_str().unwrap().len(),
            (4 << 20) + 16_384
        );
    }

    #[test]
    fn enforces_aggregate_metadata_byte_budget_across_recursive_walk() {
        // Five 4 MiB JSON blobs (20 MiB total) exceed MAX_TOTAL_METADATA_BYTES (16 MiB)
        // even though each individual blob is well below the limit.
        let mut store = MemStore::new();
        let pad = "a".repeat(4 << 20);
        let mut children = Vec::new();
        for i in 0..5u8 {
            let child = serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [],
                "annotations": {"pad": format!("{i}{pad}")},
            }))
            .unwrap();
            let d = store.insert(child);
            children.push(json!({"mediaType": MT_OCI_INDEX, "digest": d, "size": 1}));
        }
        let root_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": children,
            }))
            .unwrap(),
        );

        let err = prepare_rewrite_from_store(&root_d, "arm64", &mut store).unwrap_err();
        assert!(err.contains("aggregate limit"), "{err}");
    }

    #[test]
    fn rejects_digest_qualified_name_annotations() {
        for (key, value) in [
            (
                ANN_CONTAINERD_IMAGE_NAME,
                "docker.io/library/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                ANN_CONTAINERD_IMAGE_NAME,
                "docker.io/library/app:dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                ANN_OCI_REF_NAME,
                "docker.io/library/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
        ] {
            let mut store = MemStore::new();
            let mut b = tar::Builder::new(Vec::new());
            let config =
                serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap();
            let config_d = add_blob(&mut b, &mut store, &config);
            let manifest = serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": config_d, "size": config.len()},
                "layers": [],
            }))
            .unwrap();
            let manifest_d = add_blob(&mut b, &mut store, &manifest);
            let index = serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "manifests": [{
                    "mediaType": MT_OCI_MANIFEST,
                    "digest": manifest_d,
                    "size": manifest.len(),
                    "annotations": { key: value },
                }]
            }))
            .unwrap();
            add(&mut b, "index.json", &index);

            let PrepareRewrite::Ready(plan) =
                prepare_rewrite_from_store(&manifest_d, "arm64", &mut store).unwrap()
            else {
                panic!("expected Ready");
            };
            let mut output = Vec::new();
            let err =
                stream_rewritten_archive(b.into_inner().unwrap().as_slice(), &mut output, &plan)
                    .unwrap_err();
            assert!(err.to_string().contains("digest-qualified"), "{err}");
            assert!(
                output.is_empty(),
                "digest-qualified reference must not emit output"
            );
        }
    }

    #[test]
    fn resolves_nested_platformless_index_and_platformless_leaf() {
        let mut store = MemStore::new();
        let layer_d = store.insert(b"amd64-layer-bytes".to_vec());

        let s390x_cfg_d = store
            .insert(serde_json::to_vec(&json!({"architecture": "s390x", "os": "linux"})).unwrap());
        let s390x_m_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": s390x_cfg_d, "size": 1},
                "layers": [],
            }))
            .unwrap(),
        );

        let amd64_cfg_d = store
            .insert(serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap());
        let amd64_m_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": amd64_cfg_d, "size": 1},
                "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                            "digest": layer_d, "size": 17}],
            }))
            .unwrap(),
        );

        let inner_idx_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [
                    {"mediaType": MT_OCI_MANIFEST, "digest": s390x_m_d, "size": 1},
                    {"mediaType": MT_OCI_MANIFEST, "digest": amd64_m_d, "size": 1},
                ],
            }))
            .unwrap(),
        );
        let outer_idx_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [
                    {"mediaType": MT_OCI_INDEX, "digest": inner_idx_d, "size": 1},
                ],
            }))
            .unwrap(),
        );

        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&outer_idx_d, "arm64", &mut store).unwrap()
        else {
            panic!("expected Ready");
        };
        assert_eq!(plan.export_platform, "linux/amd64");
        assert!(plan.layer_hexes.contains(&layer_d[7..]));
    }

    #[test]
    fn deep_platformless_sibling_does_not_block_usable_amd64_sibling() {
        let mut store = MemStore::new();
        // Build an over-limit platformless chain (depth > MAX_INDEX_DEPTH).
        let mut deep_d = store.insert(
            serde_json::to_vec(
                &json!({"schemaVersion": 2, "mediaType": MT_OCI_INDEX, "manifests": []}),
            )
            .unwrap(),
        );
        for _ in 0..=MAX_INDEX_DEPTH {
            deep_d = store.insert(
                serde_json::to_vec(&json!({
                    "schemaVersion": 2,
                    "mediaType": MT_OCI_INDEX,
                    "manifests": [{"mediaType": MT_OCI_INDEX, "digest": deep_d, "size": 1}],
                }))
                .unwrap(),
            );
        }

        // Shallow, complete explicit amd64 sibling listed AFTER the deep branch.
        let layer_d = store.insert(b"good-amd64".to_vec());
        let cfg_d = store
            .insert(serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap());
        let manifest_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": cfg_d, "size": 1},
                "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                            "digest": layer_d, "size": 10}],
            }))
            .unwrap(),
        );

        let root_with_good_sibling = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [
                    {"mediaType": MT_OCI_INDEX, "digest": deep_d, "size": 1},
                    {"mediaType": MT_OCI_MANIFEST, "digest": manifest_d, "size": 1,
                     "platform": {"os": "linux", "architecture": "amd64"}},
                ],
            }))
            .unwrap(),
        );
        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&root_with_good_sibling, "arm64", &mut store).unwrap()
        else {
            panic!("usable sibling after over-limit branch must still be selected");
        };
        assert_eq!(plan.export_platform, "linux/amd64");
        assert!(plan.layer_hexes.contains(&layer_d[7..]));

        // When ONLY the over-limit branch exists, preserve the descriptive depth error.
        let err = prepare_rewrite_from_store(&deep_d, "arm64", &mut store).unwrap_err();
        assert!(err.contains("nested too deeply"), "{err}");
    }

    #[test]
    fn preserves_outer_index_platform_for_nested_platformless_leaf() {
        let mut store = MemStore::new();
        let layer_d = store.insert(b"v3-payload".to_vec());
        // Leaf config omits optional `variant`, and nested descriptor omits `platform`.
        let cfg_d = store
            .insert(serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap());
        let leaf_m_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": cfg_d, "size": 1},
                "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                            "digest": layer_d, "size": 10}],
            }))
            .unwrap(),
        );
        let inner_idx_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [{"mediaType": MT_OCI_MANIFEST, "digest": leaf_m_d, "size": 1}],
            }))
            .unwrap(),
        );
        // Root index advertises the nested index as `linux/amd64/v3`; containerd's
        // exporter filters immediate children, so export must use `linux/amd64/v3`.
        let root_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [{
                    "mediaType": MT_OCI_INDEX,
                    "digest": inner_idx_d,
                    "size": 1,
                    "platform": {"os": "linux", "architecture": "amd64", "variant": "v3"},
                }],
            }))
            .unwrap(),
        );

        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&root_d, "arm64", &mut store).unwrap()
        else {
            panic!("expected Ready");
        };
        assert_eq!(plan.export_platform, "linux/amd64/v3");
    }

    #[test]
    fn resolves_indexed_amd64_v3_and_prefers_baseline_when_both_local() {
        let mut store = MemStore::new();
        let layer_d = store.insert(b"v3-layer".to_vec());
        let v3_cfg_d = store.insert(
            serde_json::to_vec(&json!({
                "architecture": "amd64",
                "variant": "v3",
                "os": "linux",
            }))
            .unwrap(),
        );
        let v3_m_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": v3_cfg_d, "size": 1},
                "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                            "digest": layer_d, "size": 8}],
            }))
            .unwrap(),
        );
        let idx_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [
                    {"mediaType": MT_OCI_MANIFEST, "digest": format!("sha256:{}", "a".repeat(64)),
                     "size": 1, "platform": {"os": "linux", "architecture": "amd64"}},
                    {"mediaType": MT_OCI_MANIFEST, "digest": format!("sha256:{}", "b".repeat(64)),
                     "size": 1, "platform": {"os": "linux", "architecture": "arm64"}},
                    {"mediaType": MT_OCI_MANIFEST, "digest": v3_m_d,
                     "size": 1, "platform": {"os": "linux", "architecture": "amd64", "variant": "v3"}},
                ],
            }))
            .unwrap(),
        );

        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&idx_d, "arm64", &mut store).unwrap()
        else {
            panic!("expected Ready");
        };
        assert_eq!(plan.export_platform, "linux/amd64/v3");

        assert_eq!(
            prepare_rewrite_from_store(&format!("sha256:{}", "c".repeat(64)), "arm64", &mut store)
                .unwrap(),
            PrepareRewrite::RootMissing
        );

        // When baseline amd64 is ALSO complete locally, prefer baseline over v3.
        let base_cfg_d = store
            .insert(serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap());
        let base_m_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": base_cfg_d, "size": 1},
                "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                            "digest": layer_d, "size": 8}],
            }))
            .unwrap(),
        );
        let idx_both_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [
                    {"mediaType": MT_OCI_MANIFEST, "digest": v3_m_d,
                     "size": 1, "platform": {"os": "linux", "architecture": "amd64", "variant": "v3"}},
                    {"mediaType": MT_OCI_MANIFEST, "digest": base_m_d,
                     "size": 1, "platform": {"os": "linux", "architecture": "amd64"}},
                ],
            }))
            .unwrap(),
        );
        let PrepareRewrite::Ready(plan_both) =
            prepare_rewrite_from_store(&idx_both_d, "arm64", &mut store).unwrap()
        else {
            panic!("expected Ready");
        };
        assert_eq!(plan_both.export_platform, "linux/amd64");
    }

    #[test]
    fn returns_not_available_for_non_amd64_images() {
        let mut store = MemStore::new();
        let cfg_d = store
            .insert(serde_json::to_vec(&json!({"architecture": "s390x", "os": "linux"})).unwrap());
        let m_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": cfg_d, "size": 1},
                "layers": [],
            }))
            .unwrap(),
        );
        assert_eq!(
            prepare_rewrite_from_store(&m_d, "arm64", &mut store).unwrap(),
            PrepareRewrite::NotAvailable
        );
    }

    #[test]
    fn platform_and_digest_ref_detection() {
        assert!(has_native_platform("linux/arm64", "arm64"));
        assert!(has_native_platform("linux/amd64,linux/arm64/v8", "arm64"));
        assert!(has_native_platform("linux/aarch64", "arm64"));
        assert!(!has_native_platform("linux/amd64", "arm64"));
        assert!(!has_native_platform("linux/arm/v7", "arm64"));
        assert!(!has_native_platform("", "arm64"));
        assert!(!has_native_platform("-", "arm64"));

        assert!(has_amd64_platform("linux/amd64"));
        assert!(has_amd64_platform("linux/386,linux/amd64/v3"));
        assert!(has_amd64_platform("linux/x86_64"));
        assert!(has_amd64_platform("linux/amd64,linux/arm64/v8"));
        assert!(!has_amd64_platform("linux/arm64/v8"));
        assert!(!has_amd64_platform("windows/amd64"));
        assert!(!has_amd64_platform(""));
        assert!(!has_amd64_platform("-"));

        assert!(is_digest_reference("docker.io/library/app@sha256:abcd"));
        assert!(is_digest_reference("docker.io/library/app:dev@sha256:abcd"));
        assert!(is_digest_reference("sha256:deadbeef"));
        assert!(!is_digest_reference("docker.io/library/app:dev"));
        assert!(!is_digest_reference("localhost:5000/app:latest"));
    }
}
