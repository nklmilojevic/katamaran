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
//! Rewriting is purely metadata-driven so neither guest PID 1 memory nor the
//! shared guest data disk ever holds a second copy of image layers:
//!
//! 1. [`prepare_rewrite_from_store`] traverses the OCI descriptor graph in
//!    `moby`'s content store within a strict aggregate metadata byte and blob
//!    budget (reading only JSON metadata blobs, never layer payloads, and
//!    dropping parsed JSON trees before recursing), selects a locally complete
//!    `linux/amd64` variant, and computes the rewritten `linux/<native_arch>`
//!    config and manifest while retaining the original layer descriptors.
//! 2. [`crate::containerd_grpc::publish_rewritten_image`] links those existing
//!    shared layer blobs into `k8s.io` via containerd's shared-content gRPC
//!    commit path (zero disk copies, zero layer bytes transferred), writes only
//!    the rewritten config and manifest blobs with GC child labels, re-checks
//!    the `moby` source digest, and publishes the tag in `k8s.io`.

use std::collections::{HashMap, HashSet};

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

/// Cap on the number of uncached individual content-store stat lookups
/// (`Content.Info`) performed while walking a single image's descriptor graph.
/// Absent index siblings are checked in bounded batches via
/// [`BlobStore::batch_blob_sizes`] and do not consume this budget.
const MAX_STAT_LOOKUPS: usize = 512;

/// Number of platform-eligible child digests (or leaf layers) prefetched per
/// bounded batch presence query so absent siblings neither exhaust
/// [`MAX_STAT_LOOKUPS`] nor force unnecessary queries once an ideal baseline
/// candidate is found.
const PREFETCH_BATCH_SIZE: usize = 128;

/// Maximum OCI index nesting depth traversed per branch. Hitting this cap fails
/// only the overly deep branch so shallower sibling manifests remain usable.
const MAX_INDEX_DEPTH: usize = 4;

const MT_OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
const MT_DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
const MT_OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";

/// Typed error returned by [`BlobStore::read_json_blob`] so metadata-budget
/// exhaustion can be distinguished from fatal content-store I/O errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobReadError {
    /// The requested blob alone exceeded the remaining `max_bytes` budget.
    TooLarge(String),
    /// Any other store, command, or I/O failure.
    Other(String),
}

/// Read-only content-addressed blob lookup backed by containerd's `moby`
/// content store (or an in-memory map in tests).
pub trait BlobStore {
    /// Look up the authoritative stored size (in bytes) of `digest`
    /// (`sha256:<hex>`).
    ///
    /// Returns `Ok(Some(size))` when present locally, `Ok(None)` when absent
    /// (`NOT_FOUND`), or `Err(...)` on store/RPC failure.
    fn blob_size(&mut self, digest: &str) -> Result<Option<u64>, String>;

    /// Batch presence and size lookup for `digests`, returning ONLY the subset
    /// of digests that exist locally along with their authoritative byte sizes.
    /// Absent digests are omitted from the returned map.
    fn batch_blob_sizes(&mut self, digests: &[&str]) -> Result<HashMap<String, u64>, String>;

    /// Read a JSON metadata blob, failing with [`BlobReadError::TooLarge`]
    /// before fetching its payload when the blob's authoritative stored size
    /// exceeds `max_bytes`.
    fn read_json_blob(&mut self, digest: &str, max_bytes: usize) -> Result<Vec<u8>, BlobReadError>;
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

/// Platforms listed by `ctr images ls` that the guest kernel and CRI can run
/// natively without rewriting.
///
/// Katamaran's arm64 guest kernel disables `CONFIG_COMPAT` (no 32-bit ARM
/// execution) and `ctr images export --local` defaults to `DefaultStrict()`, so
/// 32-bit `linux/arm/*` variants are deliberately excluded rather than letting
/// them suppress a usable `linux/amd64` rewrite.
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
/// `platforms.Normalize` rules.
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
        "i386" | "386" => ("386".into(), variant),
        "arm64" | "aarch64" => {
            let v = match variant.as_deref() {
                None | Some("8" | "v8" | "v8.0") => None,
                Some("9" | "9.0" | "v9.0") => Some("v9".into()),
                Some(other) => Some(other.into()),
            };
            ("arm64".into(), v)
        }
        "arm" | "armhf" | "armel" => {
            let v = match variant.as_deref() {
                None | Some("7" | "v7") => Some("v7".into()),
                Some("5" | "v5") => Some("v5".into()),
                Some("6" | "v6") => Some("v6".into()),
                Some("8" | "v8") => Some("v8".into()),
                Some(other) => Some(other.into()),
            };
            ("arm".into(), v)
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

/// Metadata-only rewrite plan built purely from `moby`'s content store without
/// reading or copying any layer payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewritePlan {
    /// Expected target digest of the top-level image in `moby`.
    pub source_digest: String,
    /// Selected `linux/amd64[/variant]` platform string (preserving the root
    /// index's immediate descriptor platform when explicit).
    pub selected_platform: String,
    /// Ordered `sha256:<hex>` layer digests referenced by the rewritten
    /// manifest (matching containerd's `containerd.io/gc.ref.content.l.<idx>`
    /// GC label order).
    pub layer_digests: Vec<String>,
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

/// Digests of a rewritten image published into `k8s.io`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewritten {
    /// Digest of the new single-platform manifest.
    pub manifest: String,
    /// Digest of the new image config (the image ID kubelet sees).
    pub config: String,
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
    let mut budget = BudgetedStore::new(
        store,
        MAX_TOTAL_METADATA_BYTES,
        MAX_METADATA_BLOBS,
        MAX_STAT_LOOKUPS,
    );
    match budget.blob_size(root_digest)? {
        StatOutcome::Size(None) => return Ok(PrepareRewrite::RootMissing),
        StatOutcome::Size(Some(_)) => {}
        StatOutcome::BudgetExhausted(msg) => return Err(msg),
    }
    let mut walk_notes = WalkNotes::default();
    let candidate = walk_amd64_manifest(root_digest, None, None, &mut budget, 0, &mut walk_notes)?;
    let Some(cand) = candidate else {
        if let Some(err) = walk_notes.budget_err {
            return Err(err);
        }
        if let Some(err) = walk_notes.depth_err {
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

    let (new_manifest_hex, new_manifest_bytes, manifest_media_type, layer_digests) = {
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

        let mut layer_digests = Vec::new();
        if let Some(layers) = manifest.get("layers").and_then(Value::as_array) {
            for layer in layers {
                let digest = layer
                    .get("digest")
                    .and_then(Value::as_str)
                    .ok_or("layer descriptor has no digest")?;
                if !digest.starts_with("sha256:") {
                    return Err(format!("unsupported digest {digest}"));
                }
                layer_digests.push(digest.to_string());
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
        (hex, bytes, media_type, layer_digests)
    };

    Ok(RewritePlan {
        source_digest: root_digest.to_string(),
        selected_platform: cand.selected_platform,
        layer_digests,
        new_config_hex,
        new_config_bytes,
        new_manifest_hex,
        new_manifest_bytes,
        manifest_media_type,
        native_arch: native_arch.to_string(),
    })
}

enum StatOutcome {
    Size(Option<u64>),
    BudgetExhausted(String),
}

enum ReadOutcome {
    Data(Vec<u8>),
    BudgetExhausted(String),
}

#[derive(Default)]
struct WalkNotes {
    budget_err: Option<String>,
    depth_err: Option<String>,
}

/// Wrapper enforcing aggregate byte, metadata-blob, and stat-lookup caps across
/// an image's descriptor graph walk, while caching per-digest stat results so
/// each digest is queried at most once.
struct BudgetedStore<'a, S: BlobStore> {
    inner: &'a mut S,
    remaining_bytes: usize,
    remaining_blobs: usize,
    remaining_stats: usize,
    total_limit: usize,
    size_cache: HashMap<String, Option<u64>>,
}

impl<'a, S: BlobStore> BudgetedStore<'a, S> {
    fn new(inner: &'a mut S, byte_limit: usize, blob_limit: usize, stat_limit: usize) -> Self {
        Self {
            inner,
            remaining_bytes: byte_limit,
            remaining_blobs: blob_limit,
            remaining_stats: stat_limit,
            total_limit: byte_limit,
            size_cache: HashMap::new(),
        }
    }

    fn blob_size(&mut self, digest: &str) -> Result<StatOutcome, String> {
        if let Some(&cached) = self.size_cache.get(digest) {
            return Ok(StatOutcome::Size(cached));
        }
        if self.remaining_stats == 0 {
            return Ok(StatOutcome::BudgetExhausted(format!(
                "metadata walk exceeded {MAX_STAT_LOOKUPS}-lookup stat limit while checking {digest}"
            )));
        }
        self.remaining_stats -= 1;
        let size = self.inner.blob_size(digest)?;
        self.size_cache.insert(digest.to_string(), size);
        Ok(StatOutcome::Size(size))
    }

    /// Populate `size_cache` for every uncached digest in `digests` using a
    /// bounded batch presence query so absent siblings do not consume
    /// `remaining_stats`.
    fn prefetch_sizes(&mut self, digests: &[&str]) -> Result<(), String> {
        let mut seen = HashSet::new();
        let uncached: Vec<&str> = digests
            .iter()
            .copied()
            .filter(|d| !self.size_cache.contains_key(*d) && seen.insert(*d))
            .collect();
        if uncached.is_empty() {
            return Ok(());
        }
        let present = self.inner.batch_blob_sizes(&uncached)?;
        for digest in uncached {
            let size = present.get(digest).copied();
            self.size_cache.insert(digest.to_string(), size);
        }
        Ok(())
    }

    fn is_read_exhausted(&self) -> bool {
        self.remaining_blobs == 0 || self.remaining_bytes == 0
    }

    fn read_json_blob(&mut self, digest: &str) -> Result<ReadOutcome, String> {
        if self.remaining_blobs == 0 {
            return Ok(ReadOutcome::BudgetExhausted(format!(
                "metadata walk exceeded {MAX_METADATA_BLOBS}-blob limit while reading {digest}"
            )));
        }
        if self.remaining_bytes == 0 {
            return Ok(ReadOutcome::BudgetExhausted(format!(
                "metadata walk exceeded {}-byte aggregate limit while reading {digest}",
                self.total_limit
            )));
        }
        match self.blob_size(digest)? {
            StatOutcome::BudgetExhausted(msg) => return Ok(ReadOutcome::BudgetExhausted(msg)),
            StatOutcome::Size(None) => {
                return Err(format!(
                    "metadata blob {digest} disappeared during rewrite walk"
                ));
            }
            StatOutcome::Size(Some(size)) if size > self.remaining_bytes as u64 => {
                self.remaining_blobs -= 1;
                return Ok(ReadOutcome::BudgetExhausted(format!(
                    "metadata walk exceeded {}-byte aggregate limit while reading {digest} ({size} bytes)",
                    self.total_limit
                )));
            }
            StatOutcome::Size(Some(_)) => {}
        }
        self.remaining_blobs -= 1;
        let data = match self.inner.read_json_blob(digest, self.remaining_bytes) {
            Ok(data) => data,
            Err(BlobReadError::TooLarge(e)) => {
                return Ok(ReadOutcome::BudgetExhausted(format!(
                    "metadata walk exceeded {}-byte aggregate limit while reading {digest}: {e}",
                    self.total_limit
                )));
            }
            Err(BlobReadError::Other(e)) => return Err(e),
        };
        if data.len() > self.remaining_bytes {
            return Ok(ReadOutcome::BudgetExhausted(format!(
                "metadata walk exceeded {}-byte aggregate limit while reading {digest}",
                self.total_limit
            )));
        }
        self.remaining_bytes -= data.len();
        Ok(ReadOutcome::Data(data))
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
    /// Selected platform preserving the root index's immediate descriptor
    /// platform when explicit.
    selected_platform: String,
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
/// Hitting [`MAX_INDEX_DEPTH`] or exhausting the metadata byte/blob budget only
/// stops the affected branch/ranking (recording the first error in `notes`) so
/// any already-found complete candidate is preserved, while fatal store read
/// errors still propagate immediately.
fn walk_amd64_manifest<S: BlobStore>(
    digest: &str,
    desc_media_type: Option<&str>,
    desc_platform: Option<&PlatformDesc>,
    store: &mut BudgetedStore<'_, S>,
    depth: usize,
    notes: &mut WalkNotes,
) -> Result<Option<Candidate>, String> {
    match store.blob_size(digest)? {
        StatOutcome::Size(None) => return Ok(None),
        StatOutcome::Size(Some(_)) => {}
        StatOutcome::BudgetExhausted(msg) => {
            if notes.budget_err.is_none() {
                notes.budget_err = Some(msg);
            }
            return Ok(None);
        }
    }
    let raw = match store.read_json_blob(digest)? {
        ReadOutcome::Data(data) => data,
        ReadOutcome::BudgetExhausted(msg) => {
            if notes.budget_err.is_none() {
                notes.budget_err = Some(msg);
            }
            return Ok(None);
        }
    };
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
            notes,
        ),
        ParsedBlob::Index(children) => {
            // Drop `raw` immediately before recursing so ancestor index bytes
            // are never retained on the stack.
            drop(raw);
            if depth >= MAX_INDEX_DEPTH {
                if notes.depth_err.is_none() {
                    notes.depth_err = Some(format!("{digest}: image indexes nested too deeply"));
                }
                return Ok(None);
            }
            let eligible: Vec<ChildDescriptor> = children
                .into_iter()
                .filter(|c| !c.platform.as_ref().is_some_and(|p| !p.allows_amd64()))
                .collect();
            let mut best: Option<Candidate> = None;
            'chunks: for chunk in eligible.chunks(PREFETCH_BATCH_SIZE) {
                if store.is_read_exhausted() && notes.budget_err.is_some() {
                    break;
                }
                let chunk_digests: Vec<&str> = chunk.iter().map(|c| c.digest.as_str()).collect();
                store.prefetch_sizes(&chunk_digests)?;

                for child in chunk {
                    match store.blob_size(&child.digest)? {
                        StatOutcome::Size(None) => continue,
                        StatOutcome::Size(Some(_)) => {}
                        StatOutcome::BudgetExhausted(msg) => {
                            if notes.budget_err.is_none() {
                                notes.budget_err = Some(msg);
                            }
                            break 'chunks;
                        }
                    }
                    if store.is_read_exhausted() {
                        if notes.budget_err.is_none() {
                            notes.budget_err = Some(if store.remaining_blobs == 0 {
                                format!(
                                    "metadata walk exceeded {MAX_METADATA_BLOBS}-blob limit before reading {}",
                                    child.digest
                                )
                            } else {
                                format!(
                                    "metadata walk exceeded {}-byte aggregate limit before reading {}",
                                    store.total_limit, child.digest
                                )
                            });
                        }
                        break 'chunks;
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
                        notes,
                    )?
                    else {
                        continue;
                    };
                    if let Some(outer) = outer_selector {
                        found.selected_platform = outer;
                    }
                    let better = best.as_ref().is_none_or(|cur| {
                        (
                            amd64_platform_rank(&found.leaf_platform),
                            amd64_platform_rank(&found.selected_platform),
                        ) < (
                            amd64_platform_rank(&cur.leaf_platform),
                            amd64_platform_rank(&cur.selected_platform),
                        )
                    });
                    if better {
                        let is_ideal = found.leaf_platform == "linux/amd64"
                            && found.selected_platform == "linux/amd64";
                        best = Some(found);
                        if is_ideal {
                            break 'chunks;
                        }
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
    notes: &mut WalkNotes,
) -> Result<Option<Candidate>, String> {
    let Some(config_digest) = config_digest else {
        return Ok(None);
    };
    let mut to_check = Vec::with_capacity(1 + layers.len());
    to_check.push(config_digest.as_str());
    for layer in &layers {
        let Some(layer_digest) = layer.as_deref() else {
            return Ok(None);
        };
        if !layer_digest.starts_with("sha256:") {
            return Ok(None);
        }
        to_check.push(layer_digest);
    }
    for chunk in to_check.chunks(PREFETCH_BATCH_SIZE) {
        store.prefetch_sizes(chunk)?;
        for &digest in chunk {
            match store.blob_size(digest)? {
                StatOutcome::Size(None) => return Ok(None),
                StatOutcome::Size(Some(_)) => {}
                StatOutcome::BudgetExhausted(msg) => {
                    if notes.budget_err.is_none() {
                        notes.budget_err = Some(msg);
                    }
                    return Ok(None);
                }
            }
        }
    }

    let config_bytes = match store.read_json_blob(&config_digest)? {
        ReadOutcome::Data(data) => data,
        ReadOutcome::BudgetExhausted(msg) => {
            if notes.budget_err.is_none() {
                notes.budget_err = Some(msg);
            }
            return Ok(None);
        }
    };
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
        selected_platform: leaf_platform.clone(),
        leaf_platform,
    }))
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

    const MT_DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";

    struct MemStore {
        blobs: HashMap<String, Vec<u8>>,
        /// Optional digest whose read triggers a fatal (non-budget) I/O error.
        fail_digest: Option<String>,
        /// Digests that must never be read (only checked via `blob_size`/`batch_blob_sizes`).
        forbid_read: Vec<String>,
        /// Recorded batch sizes passed to `batch_blob_sizes`.
        batch_sizes: Vec<usize>,
    }

    impl MemStore {
        fn new() -> Self {
            Self {
                blobs: HashMap::new(),
                fail_digest: None,
                forbid_read: Vec::new(),
                batch_sizes: Vec::new(),
            }
        }

        fn insert(&mut self, data: Vec<u8>) -> String {
            let digest = format!("sha256:{}", sha256(&data));
            self.blobs.insert(digest.clone(), data);
            digest
        }

        fn insert_layer(&mut self, data: Vec<u8>) -> String {
            let digest = self.insert(data);
            self.forbid_read.push(digest.clone());
            digest
        }
    }

    impl BlobStore for MemStore {
        fn blob_size(&mut self, digest: &str) -> Result<Option<u64>, String> {
            Ok(self.blobs.get(digest).map(|b| b.len() as u64))
        }

        fn batch_blob_sizes(&mut self, digests: &[&str]) -> Result<HashMap<String, u64>, String> {
            self.batch_sizes.push(digests.len());
            let mut out = HashMap::new();
            for &d in digests {
                if let Some(b) = self.blobs.get(d) {
                    out.insert(d.to_string(), b.len() as u64);
                }
            }
            Ok(out)
        }

        fn read_json_blob(
            &mut self,
            digest: &str,
            max_bytes: usize,
        ) -> Result<Vec<u8>, BlobReadError> {
            assert!(
                !self.forbid_read.iter().any(|d| d == digest),
                "metadata walk must never read layer payload {digest}"
            );
            if self.fail_digest.as_deref() == Some(digest) {
                return Err(BlobReadError::Other(format!(
                    "simulated containerd i/o error reading {digest}"
                )));
            }
            let data = self
                .blobs
                .get(digest)
                .ok_or_else(|| BlobReadError::Other(format!("missing {digest}")))?;
            if data.len() > max_bytes {
                return Err(BlobReadError::TooLarge(format!(
                    "blob {digest} ({} bytes) exceeded {max_bytes}-byte limit",
                    data.len()
                )));
            }
            Ok(data.clone())
        }
    }

    fn check(with_index: bool) {
        let mut store = MemStore::new();
        let small = store.insert_layer(b"\x1f\x8bsmall gzip layer".to_vec());
        let json_like = store.insert_layer(b"{json-looking small layer}".to_vec());
        let large = store.insert_layer(vec![7u8; (4 << 20) + 1]);
        let config = serde_json::to_vec(&json!({
            "architecture": "amd64",
            "variant": "v3",
            "os": "linux",
            "config": {"Entrypoint": ["/app"]},
            "rootfs": {"type": "layers", "diff_ids": ["sha256:00"]},
        }))
        .unwrap();
        let config_d = store.insert(config.clone());
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_DOCKER_MANIFEST,
            "config": {"mediaType": "application/vnd.docker.container.image.v1+json",
                       "digest": config_d, "size": config.len()},
            "layers": [
                {"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "digest": small, "size": 18},
                {"mediaType": "application/vnd.docker.image.rootfs.diff.tar",
                 "digest": json_like, "size": 26},
                {"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "digest": large, "size": (4 << 20) + 1},
            ],
        }))
        .unwrap();
        let manifest_d = store.insert(manifest.clone());
        let top_digest = if with_index {
            store.insert(
                serde_json::to_vec(&json!({
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
                .unwrap(),
            )
        } else {
            manifest_d
        };

        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&top_digest, "arm64", &mut store).unwrap()
        else {
            panic!("expected PrepareRewrite::Ready");
        };

        assert_eq!(plan.source_digest, top_digest);
        assert_eq!(plan.manifest_media_type, MT_DOCKER_MANIFEST);
        assert_eq!(plan.layer_digests, vec![small, json_like, large]);
        assert_eq!(sha256(&plan.new_config_bytes), plan.new_config_hex);
        assert_eq!(sha256(&plan.new_manifest_bytes), plan.new_manifest_hex);

        let new_manifest: Value = serde_json::from_slice(&plan.new_manifest_bytes).unwrap();
        assert_eq!(
            new_manifest["config"]["digest"],
            format!("sha256:{}", plan.new_config_hex)
        );
        assert_eq!(new_manifest["config"]["size"], plan.new_config_bytes.len());
        let new_cfg: Value = serde_json::from_slice(&plan.new_config_bytes).unwrap();
        assert_eq!(new_cfg["architecture"], "arm64");
        assert_eq!(new_cfg["os"], "linux");
        assert!(new_cfg.get("variant").is_none());
        assert_eq!(new_cfg["config"]["Entrypoint"], json!(["/app"]));
    }

    #[test]
    fn rewrites_amd64_index_to_native_manifest_without_reading_layers() {
        check(true);
    }

    #[test]
    fn rewrites_amd64_single_manifest_without_reading_layers() {
        check(false);
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
    fn enforces_aggregate_metadata_byte_budget_when_no_candidate_found() {
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

    /// Regression test: when an index lists a complete `linux/amd64/v3`
    /// candidate first and later siblings exhaust either the 64-blob budget or
    /// the 16 MiB aggregate byte budget while searching for a baseline variant,
    /// `prepare_rewrite_from_store` must keep the already-found `v3` candidate.
    #[test]
    fn keeps_complete_amd64_v3_candidate_when_ranking_exhausts_blob_or_byte_budget() {
        let mut store = MemStore::new();
        let layer_d = store.insert_layer(b"v3-payload".to_vec());
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
                            "digest": layer_d, "size": 10}],
            }))
            .unwrap(),
        );

        // 1. Exhaust 64-blob budget after the v3 candidate.
        let mut many_children = vec![json!({
            "mediaType": MT_OCI_MANIFEST,
            "digest": v3_m_d,
            "size": 1,
            "platform": {"os": "linux", "architecture": "amd64", "variant": "v3"},
        })];
        for i in 0..(MAX_METADATA_BLOBS + 10) {
            let filler = store.insert(
                serde_json::to_vec(&json!({
                    "schemaVersion": 2,
                    "mediaType": MT_OCI_INDEX,
                    "manifests": [],
                    "annotations": {"seq": i.to_string()},
                }))
                .unwrap(),
            );
            many_children.push(json!({"mediaType": MT_OCI_INDEX, "digest": filler, "size": 1}));
        }
        let blob_budget_root = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": many_children,
            }))
            .unwrap(),
        );
        let PrepareRewrite::Ready(plan_blobs) =
            prepare_rewrite_from_store(&blob_budget_root, "arm64", &mut store).unwrap()
        else {
            panic!("expected v3 candidate to survive blob-budget exhaustion");
        };
        assert_eq!(plan_blobs.selected_platform, "linux/amd64/v3");
        assert_eq!(plan_blobs.layer_digests, vec![layer_d.clone()]);

        // 2. Exhaust 16 MiB aggregate byte budget after the v3 candidate.
        let pad = "b".repeat(4 << 20);
        let mut heavy_children = vec![json!({
            "mediaType": MT_OCI_MANIFEST,
            "digest": v3_m_d,
            "size": 1,
            "platform": {"os": "linux", "architecture": "amd64", "variant": "v3"},
        })];
        for i in 0..5u8 {
            let big = store.insert(
                serde_json::to_vec(&json!({
                    "schemaVersion": 2,
                    "mediaType": MT_OCI_INDEX,
                    "manifests": [],
                    "annotations": {"pad": format!("{i}{pad}")},
                }))
                .unwrap(),
            );
            heavy_children.push(json!({"mediaType": MT_OCI_INDEX, "digest": big, "size": 1}));
        }
        let byte_budget_root = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": heavy_children,
            }))
            .unwrap(),
        );
        let PrepareRewrite::Ready(plan_bytes) =
            prepare_rewrite_from_store(&byte_budget_root, "arm64", &mut store).unwrap()
        else {
            panic!("expected v3 candidate to survive byte-budget exhaustion");
        };
        assert_eq!(plan_bytes.selected_platform, "linux/amd64/v3");
        assert_eq!(plan_bytes.layer_digests, vec![layer_d]);

        // 3. When NO usable candidate exists and blob budget runs out, surface the budget error.
        let mut no_cand_children = Vec::new();
        for i in 0..(MAX_METADATA_BLOBS + 5) {
            let filler = store.insert(
                serde_json::to_vec(&json!({
                    "schemaVersion": 2,
                    "mediaType": MT_OCI_INDEX,
                    "manifests": [],
                    "annotations": {"empty": i.to_string()},
                }))
                .unwrap(),
            );
            no_cand_children.push(json!({"mediaType": MT_OCI_INDEX, "digest": filler, "size": 1}));
        }
        let no_cand_root = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": no_cand_children,
            }))
            .unwrap(),
        );
        let err = prepare_rewrite_from_store(&no_cand_root, "arm64", &mut store).unwrap_err();
        assert!(
            err.contains("budget") || err.contains("blob limit"),
            "{err}"
        );

        // 4. Fatal non-budget read failures still propagate rather than being swallowed.
        let broken_sibling = store.insert(b"placeholder".to_vec());
        let fatal_root = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [
                    {"mediaType": MT_OCI_MANIFEST, "digest": v3_m_d, "size": 1,
                     "platform": {"os": "linux", "architecture": "amd64", "variant": "v3"}},
                    {"mediaType": MT_OCI_MANIFEST, "digest": broken_sibling, "size": 1,
                     "platform": {"os": "linux", "architecture": "amd64"}},
                ],
            }))
            .unwrap(),
        );
        store.fail_digest = Some(broken_sibling);
        let err = prepare_rewrite_from_store(&fatal_root, "arm64", &mut store).unwrap_err();
        assert!(err.contains("simulated containerd i/o error"), "{err}");
    }

    #[test]
    fn resolves_nested_platformless_index_and_platformless_leaf() {
        let mut store = MemStore::new();
        let layer_d = store.insert_layer(b"amd64-layer-bytes".to_vec());

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
        assert_eq!(plan.selected_platform, "linux/amd64");
        assert_eq!(plan.layer_digests, vec![layer_d]);
    }

    #[test]
    fn deep_platformless_sibling_does_not_block_usable_amd64_sibling() {
        let mut store = MemStore::new();
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

        let layer_d = store.insert_layer(b"good-amd64".to_vec());
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
        assert_eq!(plan.selected_platform, "linux/amd64");
        assert_eq!(plan.layer_digests, vec![layer_d]);

        let err = prepare_rewrite_from_store(&deep_d, "arm64", &mut store).unwrap_err();
        assert!(err.contains("nested too deeply"), "{err}");
    }

    #[test]
    fn preserves_outer_index_platform_for_nested_platformless_leaf() {
        let mut store = MemStore::new();
        let layer_d = store.insert_layer(b"v3-payload".to_vec());
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
        assert_eq!(plan.selected_platform, "linux/amd64/v3");
    }

    #[test]
    fn resolves_indexed_amd64_v3_and_prefers_baseline_when_both_local() {
        let mut store = MemStore::new();
        let layer_d = store.insert_layer(b"v3-layer".to_vec());
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
        assert_eq!(plan.selected_platform, "linux/amd64/v3");

        assert_eq!(
            prepare_rewrite_from_store(&format!("sha256:{}", "c".repeat(64)), "arm64", &mut store)
                .unwrap(),
            PrepareRewrite::RootMissing
        );

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
        assert_eq!(plan_both.selected_platform, "linux/amd64");
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
        assert!(!has_native_platform("linux/arm/v7", "arm64"));
        assert!(!has_native_platform("linux/arm/v6", "arm64"));
        assert!(!has_native_platform("linux/arm", "arm64"));
        assert!(!has_native_platform("linux/amd64", "arm64"));
        assert!(!has_native_platform("linux/s390x", "arm64"));
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

    /// Regression test: oversized metadata siblings must be rejected before
    /// fetching their payloads (based on authoritative stored size, not the
    /// index's declared size) and must not block a smaller valid amd64 sibling
    /// listed afterward.
    #[test]
    fn oversized_siblings_reject_before_fetching_payload_and_allow_later_small_candidate() {
        struct SizeGuardedStore {
            sizes: HashMap<String, u64>,
            blobs: HashMap<String, Vec<u8>>,
            fetched: Vec<String>,
        }
        impl BlobStore for SizeGuardedStore {
            fn blob_size(&mut self, digest: &str) -> Result<Option<u64>, String> {
                Ok(self.sizes.get(digest).copied())
            }
            fn batch_blob_sizes(
                &mut self,
                digests: &[&str],
            ) -> Result<HashMap<String, u64>, String> {
                let mut out = HashMap::new();
                for &d in digests {
                    if let Some(&sz) = self.sizes.get(d) {
                        out.insert(d.to_string(), sz);
                    }
                }
                Ok(out)
            }
            fn read_json_blob(
                &mut self,
                digest: &str,
                max_bytes: usize,
            ) -> Result<Vec<u8>, BlobReadError> {
                let &size = self
                    .sizes
                    .get(digest)
                    .ok_or_else(|| BlobReadError::Other(format!("missing {digest}")))?;
                if size > max_bytes as u64 {
                    return Err(BlobReadError::TooLarge(format!(
                        "{digest} ({size} bytes) exceeds {max_bytes}-byte budget"
                    )));
                }
                self.fetched.push(digest.to_string());
                Ok(self.blobs[digest].clone())
            }
        }

        let mut store = SizeGuardedStore {
            sizes: HashMap::new(),
            blobs: HashMap::new(),
            fetched: Vec::new(),
        };
        let layer_d = format!("sha256:{}", "1".repeat(64));
        store.sizes.insert(layer_d.clone(), 1024);

        // Two oversized local siblings whose index descriptors falsely claim size: 64.
        let oversized_a = format!("sha256:{}", "a".repeat(64));
        let oversized_b = format!("sha256:{}", "b".repeat(64));
        store
            .sizes
            .insert(oversized_a.clone(), (MAX_TOTAL_METADATA_BYTES as u64) + 1);
        store.sizes.insert(
            oversized_b.clone(),
            (MAX_TOTAL_METADATA_BYTES as u64) + 4096,
        );

        let cfg = serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap();
        let cfg_d = format!("sha256:{}", sha256(&cfg));
        store.sizes.insert(cfg_d.clone(), cfg.len() as u64);
        store.blobs.insert(cfg_d.clone(), cfg);

        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": cfg_d, "size": 36},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                        "digest": layer_d, "size": 1024}],
        }))
        .unwrap();
        let manifest_d = format!("sha256:{}", sha256(&manifest));
        store
            .sizes
            .insert(manifest_d.clone(), manifest.len() as u64);
        store.blobs.insert(manifest_d.clone(), manifest);

        let root = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_INDEX,
            "manifests": [
                {"mediaType": MT_OCI_INDEX, "digest": oversized_a, "size": 64},
                {"mediaType": MT_OCI_INDEX, "digest": oversized_b, "size": 64},
                {"mediaType": MT_OCI_MANIFEST, "digest": manifest_d, "size": 256,
                 "platform": {"os": "linux", "architecture": "amd64"}},
            ],
        }))
        .unwrap();
        let root_d = format!("sha256:{}", sha256(&root));
        store.sizes.insert(root_d.clone(), root.len() as u64);
        store.blobs.insert(root_d.clone(), root);

        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&root_d, "arm64", &mut store).unwrap()
        else {
            panic!("expected small candidate after oversized siblings to succeed");
        };
        assert_eq!(plan.layer_digests, vec![layer_d]);
        assert!(
            !store.fetched.contains(&oversized_a) && !store.fetched.contains(&oversized_b),
            "oversized siblings must never have their payloads fetched: {:?}",
            store.fetched
        );
    }

    /// Regression test: 511 distinct absent `linux/amd64` descriptors listed
    /// before the only locally complete `linux/amd64` descriptor in an OCI
    /// index must not exhaust `MAX_STAT_LOOKUPS` or return a budget error.
    #[test]
    fn absent_eligible_siblings_do_not_exhaust_lookup_budget() {
        let mut store = MemStore::new();
        let layer_d = store.insert_layer(b"complete-amd64-layer".to_vec());
        let cfg_d = store
            .insert(serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap());
        let manifest_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": cfg_d,
                    "size": 1,
                },
                "layers": [{
                    "mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": layer_d,
                    "size": 20,
                }],
            }))
            .unwrap(),
        );

        let mut manifests = Vec::with_capacity(512);
        for i in 0..511u64 {
            manifests.push(json!({
                "mediaType": MT_OCI_MANIFEST,
                "digest": format!("sha256:{i:064x}"),
                "size": 128,
                "platform": {"os": "linux", "architecture": "amd64"},
            }));
        }
        manifests.push(json!({
            "mediaType": MT_OCI_MANIFEST,
            "digest": manifest_d,
            "size": 256,
            "platform": {"os": "linux", "architecture": "amd64"},
        }));

        let root_d = store.insert(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": manifests,
            }))
            .unwrap(),
        );

        let PrepareRewrite::Ready(plan) =
            prepare_rewrite_from_store(&root_d, "arm64", &mut store).unwrap()
        else {
            panic!("expected PrepareRewrite::Ready after 511 absent amd64 siblings");
        };
        assert_eq!(plan.selected_platform, "linux/amd64");
        assert_eq!(plan.layer_digests, vec![layer_d]);
        assert!(
            !store.batch_sizes.is_empty()
                && store
                    .batch_sizes
                    .iter()
                    .all(|&n| n > 0 && n <= PREFETCH_BATCH_SIZE),
            "batch queries must stay bounded by PREFETCH_BATCH_SIZE ({PREFETCH_BATCH_SIZE}): {:?}",
            store.batch_sizes
        );
    }
}
