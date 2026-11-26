//! Re-label an amd64-only OCI image archive as the guest's native platform so
//! Kubernetes can run it under the registered x86-64 emulator.
//!
//! containerd's CRI image service (through at least v2.4) resolves every
//! image against the node platform (`linux/arm64` and its arm variants), with
//! no configuration to accept other platforms. An image that only has a
//! `linux/amd64` manifest locally is therefore invisible to kubelet even though
//! the binfmt-registered emulator (Rosetta or qemu) runs its binaries fine, as
//! `docker run` does.
//!
//! [`rewrite_platform_with_options`] spools the incoming `ctr images export`
//! OCI tar archive to an unlinked temporary file on disk (keeping guest PID 1
//! memory usage O(1) with respect to layer count and payload bytes), resolves
//! the `linux/amd64` manifest purely from the OCI descriptor graph, rewrites
//! only the config's `architecture` (dropping `variant` and any stale inline
//! descriptor `data`), recomputes the config and manifest digests, and streams
//! only the referenced layer blobs plus the rewritten metadata into a
//! single-manifest archive for `ctr images import`. Layer blobs pass through
//! unchanged, so they stay deduplicated in the shared content store and the
//! binaries remain x86-64 (`uname -m` in the pod reports `x86_64`).

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Maximum size of a single JSON metadata blob (`index.json`, OCI index,
/// manifest, or image config) read into memory when traversing the descriptor
/// graph. Generously above 4 MiB so large real-world configs/indexes succeed,
/// while still bounding PID 1 heap against corrupt or hostile blobs.
pub const MAX_JSON_BLOB: u64 = 64 << 20;

/// Cap on the number of `blobs/sha256/*` tar entries indexed per archive so a
/// pathologically fragmented tar cannot exhaust PID 1 memory.
const MAX_ARCHIVE_BLOBS: usize = 65_536;

/// Maximum regular file size representable in a standard POSIX ustar header
/// (8 GiB - 1); larger layers use a GNU tar header.
const USTAR_MAX_SIZE: u64 = (1u64 << 33) - 1;

const MT_OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
const MT_DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
const MT_OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";

const ANN_CONTAINERD_IMAGE_NAME: &str = "io.containerd.image.name";
const ANN_OCI_REF_NAME: &str = "org.opencontainers.image.ref.name";

static SPOOL_SEQ: AtomicU64 = AtomicU64::new(0);

/// Read-only content-addressed blob lookup used by the OCI descriptor-graph
/// resolver, implemented both by the disk-backed archive spool and by
/// containerd's `moby` content store.
pub trait BlobStore {
    /// True if `digest` (`sha256:<hex>`) exists locally.
    fn has_blob(&self, digest: &str) -> bool;

    /// Read a JSON metadata blob (up to [`MAX_JSON_BLOB`] bytes).
    fn read_json_blob(&mut self, digest: &str) -> Result<Vec<u8>, String>;
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

/// Result of resolving a locally complete `linux/amd64` variant from a content
/// store prior to exporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalAmd64 {
    /// A complete `linux/amd64` manifest is locally present; `platform` is the
    /// normalized `--platform` value (e.g. `"linux/amd64"` or `"linux/amd64/v3"`)
    /// matching containerd's `platforms.Ordered`.
    Found { platform: String },
    /// The root blob exists, but no complete `linux/amd64` manifest is locally
    /// present in the content store.
    NotAvailable,
    /// The root digest itself is absent from the content store (concurrent
    /// retag or deletion).
    RootMissing,
}

/// Inspect `root_digest` in `store` and return the normalized platform string
/// of a locally complete `linux/amd64` manifest (including any non-baseline
/// variant such as `v3`), so `ctr images export --platform` selects the exact
/// variant that is present locally.
pub fn resolve_local_amd64_platform(
    root_digest: &str,
    store: &mut impl BlobStore,
) -> Result<LocalAmd64, String> {
    if !store.has_blob(root_digest) {
        return Ok(LocalAmd64::RootMissing);
    }
    match walk_amd64_manifest(root_digest, None, None, store, 0, false)? {
        Some(found) => Ok(LocalAmd64::Found {
            platform: found.platform,
        }),
        None => Ok(LocalAmd64::NotAvailable),
    }
}

/// Outcome of a successful [`rewrite_platform_with_options`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewritten {
    /// Digest of the new single-platform manifest.
    pub manifest: String,
    /// Digest of the new image config (the image ID kubelet sees).
    pub config: String,
}

/// Options controlling [`rewrite_platform_with_options`].
#[derive(Debug, Default, Clone, Copy)]
pub struct RewriteOptions<'a> {
    /// Expected target digest of the top-level descriptor in `index.json`. When
    /// set, rewriting aborts with [`RewriteError::SourceChanged`] before
    /// writing any bytes to `output` if the archive was exported from a
    /// different target.
    pub expected_source_digest: Option<&'a str>,
    /// Directory for the unlinked disk-backed blob spool. Defaults to
    /// `std::env::temp_dir()`. In the guest, callers should pass a directory on
    /// the persistent data disk (not `/tmp`, which is `tmpfs`).
    pub spool_dir: Option<&'a Path>,
}

/// Error returned by [`rewrite_platform_with_options`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewriteError {
    /// The exported archive's top-level digest differed from
    /// [`RewriteOptions::expected_source_digest`], indicating Docker retagged
    /// the image concurrently. No output archive was written.
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

/// Copy the OCI archive `input` to `output`, presenting its `linux/amd64`
/// image as `linux/<native_arch>`, using default [`RewriteOptions`].
#[cfg(test)]
pub fn rewrite_platform(
    input: impl Read,
    output: impl Write,
    native_arch: &str,
) -> Result<Rewritten, String> {
    rewrite_platform_with_options(input, output, native_arch, RewriteOptions::default())
        .map_err(|e| e.to_string())
}

/// Copy the OCI archive `input` to `output`, presenting its `linux/amd64`
/// image as `linux/<native_arch>`.
///
/// Incoming `blobs/sha256/*` entries are spooled straight to an unlinked
/// temporary file on disk using a fixed-size copy buffer so PID 1 heap stays
/// bounded regardless of layer count or content. `index.json`, the expected
/// source digest, name annotations, and the descriptor graph are validated
/// completely before any bytes are written to `output`.
pub fn rewrite_platform_with_options(
    input: impl Read,
    output: impl Write,
    native_arch: &str,
    opts: RewriteOptions<'_>,
) -> Result<Rewritten, RewriteError> {
    let mut spool = create_spool(opts.spool_dir).map_err(RewriteError::Other)?;
    let mut spool_pos: u64 = 0;
    let mut blob_locs: HashMap<String, (u64, u64)> = HashMap::new();
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
                read_bounded(&mut entry, MAX_JSON_BLOB, "index.json")
                    .map_err(RewriteError::Other)?,
            );
        } else if let Some(hex) = path.strip_prefix("blobs/sha256/") {
            if hex.is_empty() || hex.contains('/') {
                continue;
            }
            if !blob_locs.contains_key(hex) && blob_locs.len() >= MAX_ARCHIVE_BLOBS {
                return Err(RewriteError::Other(format!(
                    "export archive exceeds {MAX_ARCHIVE_BLOBS} blobs"
                )));
            }
            let copied = io::copy(&mut entry, &mut spool)
                .map_err(|e| RewriteError::Other(format!("spool {path}: {e}")))?;
            blob_locs.insert(hex.to_string(), (spool_pos, copied));
            spool_pos = spool_pos
                .checked_add(copied)
                .ok_or_else(|| RewriteError::Other("spool offset overflow".into()))?;
        }
        // `oci-layout` is regenerated below; Docker's `manifest.json` would
        // still name the old config, so it is dropped.
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
    if let Some(expected) = opts.expected_source_digest
        && top_digest != expected
    {
        return Err(RewriteError::SourceChanged {
            expected: expected.to_string(),
            actual: top_digest.to_string(),
        });
    }
    let sanitized_annotations =
        validate_top_annotations(top.get("annotations")).map_err(RewriteError::Other)?;

    let top_media_type = top.get("mediaType").and_then(Value::as_str);
    let mut store = SpoolStore {
        file: &mut spool,
        locs: &blob_locs,
    };
    let resolved = walk_amd64_manifest(
        top_digest,
        top_media_type,
        top.get("platform"),
        &mut store,
        0,
        true,
    )
    .map_err(RewriteError::Other)?
    .ok_or_else(|| {
        RewriteError::Other(format!(
            "{top_digest} has no linux/amd64 manifest in the archive"
        ))
    })?;

    let mut config = resolved.config;
    let config_obj = config
        .as_object_mut()
        .ok_or_else(|| RewriteError::Other("image config is not an object".into()))?;
    config_obj.insert("architecture".into(), json!(native_arch));
    config_obj.remove("variant");
    let config_bytes =
        serde_json::to_vec(&config).map_err(|e| RewriteError::Other(e.to_string()))?;
    let new_config = sha256(&config_bytes);

    let mut manifest = resolved.manifest;
    let config_desc = manifest
        .get_mut("config")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| RewriteError::Other("manifest config is not an object".into()))?;
    config_desc.insert("digest".into(), json!(format!("sha256:{new_config}")));
    config_desc.insert("size".into(), json!(config_bytes.len()));
    // Drop any OCI inline `data` bytes so the rewritten descriptor cannot carry
    // the original amd64 config bytes alongside the new digest and size.
    config_desc.remove("data");
    let manifest_bytes =
        serde_json::to_vec(&manifest).map_err(|e| RewriteError::Other(e.to_string()))?;
    let new_manifest = sha256(&manifest_bytes);

    let media_type = manifest
        .get("mediaType")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or(resolved.desc_media_type)
        .unwrap_or_else(|| MT_OCI_MANIFEST.to_string());
    let mut desc = json!({
        "mediaType": media_type,
        "digest": format!("sha256:{new_manifest}"),
        "size": manifest_bytes.len(),
        "platform": {"architecture": native_arch, "os": "linux"},
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

    let mut layer_hexes: BTreeSet<&str> = BTreeSet::new();
    if let Some(layers) = manifest.get("layers").and_then(Value::as_array) {
        for layer in layers {
            let digest = layer
                .get("digest")
                .and_then(Value::as_str)
                .ok_or_else(|| RewriteError::Other("layer descriptor has no digest".into()))?;
            let hex = digest
                .strip_prefix("sha256:")
                .ok_or_else(|| RewriteError::Other(format!("unsupported digest {digest}")))?;
            if !blob_locs.contains_key(hex) {
                return Err(RewriteError::Other(format!(
                    "layer blob {digest} missing from export archive"
                )));
            }
            layer_hexes.insert(hex);
        }
    }

    let mut out = tar::Builder::new(output);
    out.mode(tar::HeaderMode::Deterministic);
    for hex in layer_hexes {
        let (offset, size) = blob_locs[hex];
        spool
            .seek(SeekFrom::Start(offset))
            .map_err(|e| RewriteError::Other(format!("seek layer sha256:{hex}: {e}")))?;
        let mut slice = (&mut spool).take(size);
        append_stream(&mut out, &format!("blobs/sha256/{hex}"), size, &mut slice)
            .map_err(RewriteError::Other)?;
    }
    append(
        &mut out,
        &format!("blobs/sha256/{new_config}"),
        &config_bytes,
    )
    .map_err(RewriteError::Other)?;
    append(
        &mut out,
        &format!("blobs/sha256/{new_manifest}"),
        &manifest_bytes,
    )
    .map_err(RewriteError::Other)?;
    append(&mut out, "oci-layout", br#"{"imageLayoutVersion":"1.0.0"}"#)
        .map_err(RewriteError::Other)?;
    append(&mut out, "index.json", &new_index_bytes).map_err(RewriteError::Other)?;
    out.into_inner()
        .and_then(|mut w| w.flush())
        .map_err(|e| RewriteError::Other(format!("finish archive: {e}")))?;

    Ok(Rewritten {
        manifest: format!("sha256:{new_manifest}"),
        config: format!("sha256:{new_config}"),
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

struct SpoolStore<'a> {
    file: &'a mut File,
    locs: &'a HashMap<String, (u64, u64)>,
}

impl BlobStore for SpoolStore<'_> {
    fn has_blob(&self, digest: &str) -> bool {
        digest
            .strip_prefix("sha256:")
            .is_some_and(|hex| self.locs.contains_key(hex))
    }

    fn read_json_blob(&mut self, digest: &str) -> Result<Vec<u8>, String> {
        let hex = digest
            .strip_prefix("sha256:")
            .ok_or_else(|| format!("unsupported digest {digest}"))?;
        let &(offset, size) = self
            .locs
            .get(hex)
            .ok_or_else(|| format!("blob {digest} missing from export archive"))?;
        if size > MAX_JSON_BLOB {
            return Err(format!(
                "metadata blob {digest} ({size} bytes) exceeds {MAX_JSON_BLOB}-byte limit"
            ));
        }
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| format!("seek {digest}: {e}"))?;
        let mut buf = vec![0u8; size as usize];
        self.file
            .read_exact(&mut buf)
            .map_err(|e| format!("read {digest}: {e}"))?;
        Ok(buf)
    }
}

struct ResolvedAmd64 {
    desc_media_type: Option<String>,
    manifest: Value,
    config: Value,
    platform: String,
}

/// Walk `digest` (a manifest, or an OCI index / Docker manifest list, including
/// nested and platform-less descriptors) to find a locally complete
/// `linux/amd64` image manifest.
///
/// When `strict_root` is true (during archive rewrite), a non-amd64 single
/// manifest or missing root blob returns a descriptive error; when false
/// (during pre-export content-store probing), non-matching roots return
/// `Ok(None)`.
fn walk_amd64_manifest(
    digest: &str,
    desc_media_type: Option<&str>,
    desc_platform: Option<&Value>,
    store: &mut impl BlobStore,
    depth: usize,
    strict_root: bool,
) -> Result<Option<ResolvedAmd64>, String> {
    if !store.has_blob(digest) {
        return if depth == 0 && strict_root {
            Err(format!("blob {digest} missing from export archive"))
        } else {
            Ok(None)
        };
    }
    let raw = store.read_json_blob(digest)?;
    let parsed: Value = match serde_json::from_slice(&raw) {
        Ok(v) => v,
        Err(e) => {
            return if depth == 0 && strict_root {
                Err(format!("parse {digest}: {e}"))
            } else {
                Ok(None)
            };
        }
    };
    let media_type = desc_media_type
        .or_else(|| parsed.get("mediaType").and_then(Value::as_str))
        .unwrap_or("");
    let is_index = media_type == MT_OCI_INDEX
        || media_type == MT_DOCKER_LIST
        || (media_type.is_empty() && parsed.get("manifests").is_some());

    if !is_index {
        return resolve_leaf_manifest(
            parsed,
            desc_media_type,
            desc_platform,
            store,
            depth,
            strict_root,
        );
    }

    if depth >= 4 {
        return Err(format!("{digest}: image indexes nested too deeply"));
    }
    let children = parsed
        .get("manifests")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();

    let mut best: Option<ResolvedAmd64> = None;
    for child in children {
        let Some(child_digest) = child.get("digest").and_then(Value::as_str) else {
            continue;
        };
        if !store.has_blob(child_digest) {
            continue;
        }
        // Skip children whose descriptor explicitly declares a non-linux or
        // non-amd64 platform (including `unknown/unknown` attestations), while
        // still traversing children that omit `platform` (such as nested
        // indexes or platform-less leaf manifests).
        let child_platform = child.get("platform");
        if !descriptor_platform_allows_amd64(child_platform) {
            continue;
        }
        let child_mt = child.get("mediaType").and_then(Value::as_str);
        if let Some(found) = walk_amd64_manifest(
            child_digest,
            child_mt,
            child_platform,
            store,
            depth + 1,
            false,
        )? {
            let better = best.as_ref().is_none_or(|cur| {
                amd64_platform_rank(&found.platform) < amd64_platform_rank(&cur.platform)
            });
            if better {
                let is_baseline = found.platform == "linux/amd64";
                best = Some(found);
                if is_baseline {
                    break;
                }
            }
        }
    }
    Ok(best)
}

fn descriptor_platform_allows_amd64(platform: Option<&Value>) -> bool {
    let Some(p) = platform.and_then(Value::as_object) else {
        return true;
    };
    if let Some(os) = p.get("os").and_then(Value::as_str)
        && !os.eq_ignore_ascii_case("linux")
    {
        return false;
    }
    if let Some(arch) = p.get("architecture").and_then(Value::as_str)
        && normalize_arch_variant(arch, None).0 != "amd64"
    {
        return false;
    }
    true
}

fn resolve_leaf_manifest(
    manifest: Value,
    desc_media_type: Option<&str>,
    desc_platform: Option<&Value>,
    store: &mut impl BlobStore,
    depth: usize,
    strict_root: bool,
) -> Result<Option<ResolvedAmd64>, String> {
    let strict = depth == 0 && strict_root;
    let Some(config_digest) = manifest.pointer("/config/digest").and_then(Value::as_str) else {
        return if strict {
            Err("manifest has no config digest".into())
        } else {
            Ok(None)
        };
    };
    if !store.has_blob(config_digest) {
        return if strict {
            Err(format!("blob {config_digest} missing from export archive"))
        } else {
            Ok(None)
        };
    }
    let config_raw = store.read_json_blob(config_digest)?;
    let config: Value = match serde_json::from_slice(&config_raw) {
        Ok(v) => v,
        Err(e) => {
            return if strict {
                Err(format!("parse image config: {e}"))
            } else {
                Ok(None)
            };
        }
    };
    let cfg_os = config.get("os").and_then(Value::as_str);
    let cfg_arch = config.get("architecture").and_then(Value::as_str);
    let cfg_variant = config.get("variant").and_then(Value::as_str);
    let os_ok = cfg_os.is_some_and(|os| os.eq_ignore_ascii_case("linux"));
    let arch_ok = cfg_arch.is_some_and(|a| normalize_arch_variant(a, None).0 == "amd64");
    if !os_ok || !arch_ok {
        return if strict {
            Err(format!(
                "expected linux/amd64 image config, got {}/{}",
                cfg_os.unwrap_or("?"),
                cfg_arch.unwrap_or("?"),
            ))
        } else {
            Ok(None)
        };
    }

    // Require every layer blob referenced by the manifest to be present locally
    // so an index with a partial baseline amd64 manifest falls through to a
    // complete variant (e.g. linux/amd64/v3).
    if let Some(layers) = manifest.get("layers").and_then(Value::as_array) {
        for layer in layers {
            let Some(layer_digest) = layer.get("digest").and_then(Value::as_str) else {
                return if strict {
                    Err("layer descriptor has no digest".into())
                } else {
                    Ok(None)
                };
            };
            if !store.has_blob(layer_digest) {
                return if strict {
                    Err(format!(
                        "layer blob {layer_digest} missing from export archive"
                    ))
                } else {
                    Ok(None)
                };
            }
        }
    }

    // `ctr images export --platform` filters index descriptors by the
    // descriptor's own `platform` (not the config), so when the index
    // descriptor specifies an architecture we use its variant; when the
    // descriptor omits `platform` (or at the root single manifest) we fall back
    // to the config's variant.
    let desc_has_arch = desc_platform
        .and_then(|p| p.get("architecture"))
        .and_then(Value::as_str)
        .is_some();
    let variant = if desc_has_arch {
        desc_platform
            .and_then(|p| p.get("variant"))
            .and_then(Value::as_str)
    } else {
        cfg_variant
    };
    let platform = format_amd64_platform(variant);

    Ok(Some(ResolvedAmd64 {
        desc_media_type: desc_media_type.map(str::to_string),
        manifest,
        config,
        platform,
    }))
}

fn create_spool(dir: Option<&Path>) -> Result<File, String> {
    let dir = dir.map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
    let pid = std::process::id();
    for attempt in 0..64u32 {
        let seq = SPOOL_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(".kat-oci-rewrite-{pid}-{seq}-{attempt}.tmp"));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => {
                // Unlink immediately so disk space is reclaimed automatically
                // when `file` drops (or if the process exits unexpectedly).
                let _ = fs::remove_file(&path);
                return Ok(file);
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("create rewrite spool in {}: {e}", dir.display())),
        }
    }
    Err(format!(
        "create rewrite spool in {}: too many temporary file collisions",
        dir.display()
    ))
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

    const MT_DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";

    fn add(b: &mut tar::Builder<Vec<u8>>, path: &str, data: &[u8]) {
        append(b, path, data).unwrap();
    }

    fn add_blob(b: &mut tar::Builder<Vec<u8>>, data: &[u8]) -> String {
        let hex = sha256(data);
        add(b, &format!("blobs/sha256/{hex}"), data);
        format!("sha256:{hex}")
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

        fn read_json_blob(&mut self, digest: &str) -> Result<Vec<u8>, String> {
            self.0
                .get(digest)
                .cloned()
                .ok_or_else(|| format!("missing {digest}"))
        }
    }

    /// An archive shaped like `ctr images export --local --platform linux/amd64`
    /// of an amd64 image: index.json -> (optional index ->) manifest -> config,
    /// with a small non-JSON layer, a `{`-prefixed layer, and a >4 MiB layer.
    fn amd64_archive(with_index: bool) -> (Vec<u8>, String, Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut b = tar::Builder::new(Vec::new());
        let small = b"\x1f\x8bsmall gzip layer".to_vec();
        let json_like = b"{json-looking small layer}".to_vec();
        let large = vec![7u8; (4 << 20) + 1];
        let small_d = add_blob(&mut b, &small);
        let json_like_d = add_blob(&mut b, &json_like);
        let large_d = add_blob(&mut b, &large);
        let config = serde_json::to_vec(&json!({
            "architecture": "amd64",
            "variant": "v3",
            "os": "linux",
            "config": {"Entrypoint": ["/app"]},
            "rootfs": {"type": "layers", "diff_ids": ["sha256:00"]},
        }))
        .unwrap();
        let config_d = add_blob(&mut b, &config);
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
        let manifest_d = add_blob(&mut b, &manifest);
        let annotations = json!({"io.containerd.image.name": "docker.io/library/app:dev"});
        let (top, top_digest) = if with_index {
            let idx = serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_INDEX,
                "manifests": [
                    // Listed but not exported: must be skipped.
                    {"mediaType": MT_DOCKER_MANIFEST, "digest": format!("sha256:{}", "e".repeat(64)),
                     "size": 1, "platform": {"architecture": "amd64", "os": "linux"}},
                    // Attestation descriptor: must be skipped.
                    {"mediaType": MT_OCI_MANIFEST, "digest": format!("sha256:{}", "d".repeat(64)),
                     "size": 1, "platform": {"architecture": "unknown", "os": "unknown"}},
                    {"mediaType": MT_DOCKER_MANIFEST, "digest": manifest_d, "size": manifest.len(),
                     "platform": {"architecture": "amd64", "os": "linux"}},
                ],
            }))
            .unwrap();
            let idx_d = add_blob(&mut b, &idx);
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
        (b.into_inner().unwrap(), top_digest, small, json_like, large)
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
        let (input, top_digest, small, json_like, large) = amd64_archive(with_index);
        let mut output = Vec::new();
        let r = rewrite_platform_with_options(
            input.as_slice(),
            &mut output,
            "arm64",
            RewriteOptions {
                expected_source_digest: Some(&top_digest),
                spool_dir: None,
            },
        )
        .unwrap();
        let files = read_archive(&output);

        assert!(!files.contains_key("manifest.json"));
        assert!(files.contains_key("oci-layout"));
        for (path, data) in &files {
            if let Some(hex) = path.strip_prefix("blobs/sha256/") {
                assert_eq!(sha256(data), hex, "{path} content does not match its name");
            }
        }
        // Only the 3 layers + new config + new manifest should be in blobs/ (no superseded metadata).
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

        // Layers are untouched and present, including json-like and large ones.
        for layer in manifest["layers"].as_array().unwrap() {
            let d = layer["digest"].as_str().unwrap();
            assert_eq!(blob_of(d).len() as u64, layer["size"].as_u64().unwrap());
        }
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

    #[test]
    fn rejects_stale_expected_source_digest_without_emitting_output() {
        let (input, top_digest, _, _, _) = amd64_archive(true);
        let mut output = Vec::new();
        let err = rewrite_platform_with_options(
            input.as_slice(),
            &mut output,
            "arm64",
            RewriteOptions {
                expected_source_digest: Some(
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                ),
                spool_dir: None,
            },
        )
        .unwrap_err();
        assert_eq!(
            err,
            RewriteError::SourceChanged {
                expected: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .into(),
                actual: top_digest,
            }
        );
        assert!(
            output.is_empty(),
            "stale digest must not emit any output bytes"
        );
    }

    #[test]
    fn strips_stale_inline_config_data_from_rewritten_manifest() {
        let mut b = tar::Builder::new(Vec::new());
        let config = serde_json::to_vec(&json!({
            "architecture": "amd64",
            "os": "linux",
        }))
        .unwrap();
        let config_d = add_blob(&mut b, &config);
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
        let manifest_d = add_blob(&mut b, &manifest);
        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "manifests": [{
                "mediaType": MT_OCI_MANIFEST,
                "digest": manifest_d,
                "size": manifest.len(),
                "annotations": {"io.containerd.image.name": "docker.io/library/inline:dev"},
            }]
        }))
        .unwrap();
        add(&mut b, "index.json", &index);

        let mut output = Vec::new();
        let r = rewrite_platform(b.into_inner().unwrap().as_slice(), &mut output, "arm64").unwrap();
        let files = read_archive(&output);
        let new_manifest: Value =
            serde_json::from_slice(&files[&format!("blobs/sha256/{}", &r.manifest[7..])]).unwrap();
        assert_eq!(new_manifest["config"]["digest"], r.config);
        assert!(
            new_manifest["config"].get("data").is_none(),
            "rewritten config descriptor must omit stale inline data: {new_manifest}"
        );
    }

    #[test]
    fn rewrites_config_with_long_leading_whitespace() {
        let mut b = tar::Builder::new(Vec::new());
        // Over 64 leading JSON whitespace bytes before `{`.
        let mut config = vec![b' '; 256];
        config.extend_from_slice(br#"{"architecture":"amd64","os":"linux"}"#);
        let config_d = add_blob(&mut b, &config);
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
        let manifest_d = add_blob(&mut b, &manifest);
        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "manifests": [{"mediaType": MT_OCI_MANIFEST, "digest": manifest_d, "size": manifest.len()}]
        }))
        .unwrap();
        add(&mut b, "index.json", &index);

        let mut output = Vec::new();
        let r = rewrite_platform(b.into_inner().unwrap().as_slice(), &mut output, "arm64").unwrap();
        let files = read_archive(&output);
        let new_cfg: Value =
            serde_json::from_slice(&files[&format!("blobs/sha256/{}", &r.config[7..])]).unwrap();
        assert_eq!(new_cfg["architecture"], "arm64");
    }

    #[test]
    fn rewrites_config_larger_than_4_mib() {
        let mut b = tar::Builder::new(Vec::new());
        let big_label = "x".repeat((4 << 20) + 16_384);
        let config = serde_json::to_vec(&json!({
            "architecture": "amd64",
            "os": "linux",
            "config": {"Labels": {"big": big_label}},
        }))
        .unwrap();
        assert!(config.len() > (4 << 20));
        let config_d = add_blob(&mut b, &config);
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
        let manifest_d = add_blob(&mut b, &manifest);
        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "manifests": [{"mediaType": MT_OCI_MANIFEST, "digest": manifest_d, "size": manifest.len()}]
        }))
        .unwrap();
        add(&mut b, "index.json", &index);

        let mut output = Vec::new();
        let r = rewrite_platform(b.into_inner().unwrap().as_slice(), &mut output, "arm64").unwrap();
        let files = read_archive(&output);
        let new_cfg_bytes = &files[&format!("blobs/sha256/{}", &r.config[7..])];
        assert_eq!(sha256(new_cfg_bytes), &r.config[7..]);
        let new_cfg: Value = serde_json::from_slice(new_cfg_bytes).unwrap();
        assert_eq!(new_cfg["architecture"], "arm64");
        assert_eq!(
            new_cfg["config"]["Labels"]["big"].as_str().unwrap().len(),
            (4 << 20) + 16_384
        );
    }

    #[test]
    fn spools_many_json_looking_layers_without_in_memory_blob_map() {
        // 16 distinct 512 KiB `{`-prefixed layers (8 MiB aggregate, well above
        // the old 4 MiB threshold): all spool to disk and stream back intact.
        let mut b = tar::Builder::new(Vec::new());
        let mut layer_descs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..16u8 {
            let mut layer = vec![b'{'; 512 << 10];
            layer[1] = b'0' + (i % 10);
            layer[2] = i;
            let d = add_blob(&mut b, &layer);
            layer_descs.push(json!({
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": d,
                "size": layer.len(),
            }));
            expected.push((d, layer));
        }
        // Plus an unreferenced `{`-prefixed blob that must NOT appear in output.
        let _orphan = add_blob(&mut b, &vec![b'{'; 256 << 10]);

        let config = serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap();
        let config_d = add_blob(&mut b, &config);
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": config_d, "size": config.len()},
            "layers": layer_descs,
        }))
        .unwrap();
        let manifest_d = add_blob(&mut b, &manifest);
        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "manifests": [{"mediaType": MT_OCI_MANIFEST, "digest": manifest_d, "size": manifest.len()}]
        }))
        .unwrap();
        add(&mut b, "index.json", &index);

        let mut output = Vec::new();
        let _r =
            rewrite_platform(b.into_inner().unwrap().as_slice(), &mut output, "arm64").unwrap();
        let files = read_archive(&output);
        let blob_count = files
            .keys()
            .filter(|k| k.starts_with("blobs/sha256/"))
            .count();
        assert_eq!(blob_count, 16 + 2);
        for (d, bytes) in expected {
            assert_eq!(files[&format!("blobs/sha256/{}", &d[7..])], bytes);
        }
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
            let mut b = tar::Builder::new(Vec::new());
            let config =
                serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap();
            let config_d = add_blob(&mut b, &config);
            let manifest = serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": MT_OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                           "digest": config_d, "size": config.len()},
                "layers": [],
            }))
            .unwrap();
            let manifest_d = add_blob(&mut b, &manifest);
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

            let mut output = Vec::new();
            let err = rewrite_platform(b.into_inner().unwrap().as_slice(), &mut output, "arm64")
                .unwrap_err();
            assert!(err.contains("digest-qualified"), "{err}");
            assert!(
                output.is_empty(),
                "digest-qualified reference must not emit output"
            );
        }
    }

    #[test]
    fn resolves_nested_platformless_index_and_platformless_leaf() {
        let mut b = tar::Builder::new(Vec::new());
        let layer = b"amd64-layer-bytes".to_vec();
        let layer_d = add_blob(&mut b, &layer);

        // An earlier platform-less leaf for s390x must be skipped rather than aborting.
        let s390x_cfg =
            serde_json::to_vec(&json!({"architecture": "s390x", "os": "linux"})).unwrap();
        let s390x_cfg_d = add_blob(&mut b, &s390x_cfg);
        let s390x_manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": s390x_cfg_d, "size": s390x_cfg.len()},
            "layers": [],
        }))
        .unwrap();
        let s390x_m_d = add_blob(&mut b, &s390x_manifest);

        let amd64_cfg =
            serde_json::to_vec(&json!({"architecture": "amd64", "os": "linux"})).unwrap();
        let amd64_cfg_d = add_blob(&mut b, &amd64_cfg);
        let amd64_manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": amd64_cfg_d, "size": amd64_cfg.len()},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                        "digest": layer_d, "size": layer.len()}],
        }))
        .unwrap();
        let amd64_m_d = add_blob(&mut b, &amd64_manifest);

        // Inner index has no `platform` on either leaf descriptor.
        let inner_idx = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_INDEX,
            "manifests": [
                {"mediaType": MT_OCI_MANIFEST, "digest": s390x_m_d, "size": s390x_manifest.len()},
                {"mediaType": MT_OCI_MANIFEST, "digest": amd64_m_d, "size": amd64_manifest.len()},
            ],
        }))
        .unwrap();
        let inner_idx_d = add_blob(&mut b, &inner_idx);

        // Outer index has no `platform` on the nested index descriptor.
        let outer_idx = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_INDEX,
            "manifests": [
                {"mediaType": MT_OCI_INDEX, "digest": inner_idx_d, "size": inner_idx.len()},
            ],
        }))
        .unwrap();
        let outer_idx_d = add_blob(&mut b, &outer_idx);

        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "manifests": [{"mediaType": MT_OCI_INDEX, "digest": outer_idx_d, "size": outer_idx.len()}]
        }))
        .unwrap();
        add(&mut b, "index.json", &index);

        let mut output = Vec::new();
        let r = rewrite_platform(b.into_inner().unwrap().as_slice(), &mut output, "arm64").unwrap();
        let files = read_archive(&output);
        let new_cfg: Value =
            serde_json::from_slice(&files[&format!("blobs/sha256/{}", &r.config[7..])]).unwrap();
        assert_eq!(new_cfg["architecture"], "arm64");
        assert_eq!(files[&format!("blobs/sha256/{}", &layer_d[7..])], layer);
    }

    #[test]
    fn resolves_local_amd64_variant_from_index() {
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
        // Index lists baseline amd64 (not present), arm64 (not present), and
        // locally complete linux/amd64/v3.
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

        assert_eq!(
            resolve_local_amd64_platform(&idx_d, &mut store).unwrap(),
            LocalAmd64::Found {
                platform: "linux/amd64/v3".into()
            }
        );

        // Missing root -> RootMissing.
        assert_eq!(
            resolve_local_amd64_platform(&format!("sha256:{}", "c".repeat(64)), &mut store)
                .unwrap(),
            LocalAmd64::RootMissing
        );

        // If baseline amd64 is ALSO complete locally, prefer baseline over v3.
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
        assert_eq!(
            resolve_local_amd64_platform(&idx_both_d, &mut store).unwrap(),
            LocalAmd64::Found {
                platform: "linux/amd64".into()
            }
        );
    }

    #[test]
    fn rewrites_indexed_amd64_v3_without_baseline_or_native_manifest() {
        let mut b = tar::Builder::new(Vec::new());
        let layer = b"v3-binary-layer".to_vec();
        let layer_d = add_blob(&mut b, &layer);
        let cfg = serde_json::to_vec(&json!({
            "architecture": "amd64",
            "variant": "v3",
            "os": "linux",
        }))
        .unwrap();
        let cfg_d = add_blob(&mut b, &cfg);
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": cfg_d, "size": cfg.len()},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                        "digest": layer_d, "size": layer.len()}],
        }))
        .unwrap();
        let manifest_d = add_blob(&mut b, &manifest);
        let idx = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_INDEX,
            "manifests": [
                {"mediaType": MT_OCI_MANIFEST, "digest": format!("sha256:{}", "e".repeat(64)),
                 "size": 1, "platform": {"architecture": "amd64", "os": "linux"}},
                {"mediaType": MT_OCI_MANIFEST, "digest": manifest_d, "size": manifest.len(),
                 "platform": {"architecture": "amd64", "variant": "v3", "os": "linux"}},
            ],
        }))
        .unwrap();
        let idx_d = add_blob(&mut b, &idx);
        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "manifests": [{
                "mediaType": MT_OCI_INDEX,
                "digest": idx_d,
                "size": idx.len(),
                "annotations": {"io.containerd.image.name": "docker.io/library/v3app:dev"},
            }]
        }))
        .unwrap();
        add(&mut b, "index.json", &index);

        let mut output = Vec::new();
        let r = rewrite_platform(b.into_inner().unwrap().as_slice(), &mut output, "arm64").unwrap();
        let files = read_archive(&output);
        let new_cfg: Value =
            serde_json::from_slice(&files[&format!("blobs/sha256/{}", &r.config[7..])]).unwrap();
        assert_eq!(new_cfg["architecture"], "arm64");
        assert!(new_cfg.get("variant").is_none());
        assert_eq!(files[&format!("blobs/sha256/{}", &layer_d[7..])], layer);
    }

    #[test]
    fn rejects_archive_without_amd64_manifest() {
        let mut b = tar::Builder::new(Vec::new());
        let idx = serde_json::to_vec(&json!({"schemaVersion": 2, "mediaType": MT_OCI_INDEX,
            "manifests": [{"digest": format!("sha256:{}", "f".repeat(64)), "size": 1,
                           "platform": {"architecture": "s390x", "os": "linux"}}]}))
        .unwrap();
        let idx_d = add_blob(&mut b, &idx);
        let index = serde_json::to_vec(&json!({"schemaVersion": 2, "manifests": [
            {"mediaType": MT_OCI_INDEX, "digest": idx_d, "size": idx.len()}]}))
        .unwrap();
        add(&mut b, "index.json", &index);
        let input = b.into_inner().unwrap();
        let err = rewrite_platform(input.as_slice(), Vec::new(), "arm64").unwrap_err();
        assert!(err.contains("no linux/amd64 manifest"), "{err}");
    }

    #[test]
    fn rejects_non_amd64_single_manifest_config() {
        let mut b = tar::Builder::new(Vec::new());
        let config = serde_json::to_vec(&json!({
            "architecture": "s390x",
            "os": "linux",
        }))
        .unwrap();
        let config_d = add_blob(&mut b, &config);
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MT_OCI_MANIFEST,
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": config_d, "size": config.len()},
            "layers": [],
        }))
        .unwrap();
        let manifest_d = add_blob(&mut b, &manifest);
        let index = serde_json::to_vec(&json!({"schemaVersion": 2, "manifests": [
            {"mediaType": MT_OCI_MANIFEST, "digest": manifest_d, "size": manifest.len()}]}))
        .unwrap();
        add(&mut b, "index.json", &index);
        let input = b.into_inner().unwrap();
        let err = rewrite_platform(input.as_slice(), Vec::new(), "arm64").unwrap_err();
        assert!(err.contains("expected linux/amd64 image config"), "{err}");
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
