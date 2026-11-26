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
//! [`rewrite_platform`] streams a `ctr images export` OCI archive, picks the
//! `linux/amd64` manifest, rewrites only the config's `architecture` (and
//! drops `variant`), recomputes the config and manifest digests, and emits a
//! single-manifest archive for `ctr images import`. Layer blobs pass through
//! unchanged, so they stay deduplicated in the shared content store and the
//! binaries remain x86-64 (`uname -m` in the pod reports `x86_64`).

use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Maximum size of a JSON metadata blob (index, manifest, or image config)
/// kept in memory while scanning the archive. Layer blobs are detected by
/// their leading bytes (or size) and streamed straight through without
/// buffering.
const METADATA_LIMIT: u64 = 4 << 20;

/// Prefix length inspected to distinguish JSON metadata blobs from layer
/// archives (gzip, zstd, tar, etc.).
const PEEK_LEN: usize = 64;

/// Maximum regular file size representable in a standard POSIX ustar header
/// (8 GiB - 1); larger layers use a GNU tar header.
const USTAR_MAX_SIZE: u64 = (1u64 << 33) - 1;

const MT_OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
const MT_DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
const MT_OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";

/// Platforms listed by `ctr images ls` that the CRI accepts without help.
pub fn has_native_platform(platforms: &str, native_arch: &str) -> bool {
    platform_list(platforms).any(|(os, arch)| os == "linux" && arch == native_arch)
}

/// True when `ctr images ls` lists a `linux/amd64` platform variant.
pub fn has_amd64_platform(platforms: &str) -> bool {
    platform_list(platforms).any(|(os, arch)| os == "linux" && arch == "amd64")
}

fn platform_list(platforms: &str) -> impl Iterator<Item = (&str, &str)> {
    platforms.split(',').filter_map(|p| {
        let mut parts = p.trim().split('/');
        Some((parts.next()?, parts.next()?))
    })
}

/// Outcome of a successful [`rewrite_platform`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewritten {
    /// Digest of the new single-platform manifest.
    pub manifest: String,
    /// Digest of the new image config (the image ID kubelet sees).
    pub config: String,
}

/// Copy the OCI archive `input` to `output`, presenting its `linux/amd64`
/// image as `linux/<native_arch>`.
pub fn rewrite_platform(
    input: impl Read,
    output: impl Write,
    native_arch: &str,
) -> Result<Rewritten, String> {
    let mut archive = tar::Archive::new(input);
    let mut out = tar::Builder::new(output);
    out.mode(tar::HeaderMode::Deterministic);

    let mut blobs: HashMap<String, Vec<u8>> = HashMap::new();
    let mut index: Option<Vec<u8>> = None;

    let entries = archive
        .entries()
        .map_err(|e| format!("read export archive: {e}"))?;
    for entry in entries {
        let mut entry = entry.map_err(|e| format!("read export archive entry: {e}"))?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry
            .path()
            .map_err(|e| format!("export archive path: {e}"))?
            .to_string_lossy()
            .into_owned();
        let path = path.trim_start_matches("./");
        if path == "index.json" {
            index = Some(read_all(&mut entry, "index.json")?);
        } else if let Some(hex) = path.strip_prefix("blobs/sha256/") {
            let size = entry.size();
            if size <= METADATA_LIMIT {
                let mut prefix = [0u8; PEEK_LEN];
                let n = read_prefix(&mut entry, &mut prefix)
                    .map_err(|e| format!("read {path}: {e}"))?;
                if looks_like_json_object(&prefix[..n]) {
                    let mut data = Vec::with_capacity(size as usize);
                    data.extend_from_slice(&prefix[..n]);
                    entry
                        .read_to_end(&mut data)
                        .map_err(|e| format!("read {path}: {e}"))?;
                    blobs.insert(hex.to_string(), data);
                } else {
                    let mut chained = io::Cursor::new(&prefix[..n]).chain(&mut entry);
                    append_stream(&mut out, path, size, &mut chained)?;
                }
            } else {
                append_stream(&mut out, path, size, &mut entry)?;
            }
        }
        // `oci-layout` is regenerated below; Docker's `manifest.json` would
        // still name the old config, so it is dropped.
    }
    let index = index.ok_or("export archive has no index.json")?;
    let index: Value =
        serde_json::from_slice(&index).map_err(|e| format!("parse index.json: {e}"))?;
    let top = index
        .get("manifests")
        .and_then(Value::as_array)
        .and_then(|m| m.first())
        .ok_or("index.json lists no manifests")?;

    let (manifest_digest, desc_media_type) = find_amd64_manifest(top, &blobs, 0)?;
    let mut manifest: Value = serde_json::from_slice(blob(&blobs, &manifest_digest)?)
        .map_err(|e| format!("parse manifest: {e}"))?;
    let config_digest = manifest
        .pointer("/config/digest")
        .and_then(Value::as_str)
        .ok_or("manifest has no config digest")?;
    let config = blob(&blobs, config_digest)?;
    let mut config: Value =
        serde_json::from_slice(config).map_err(|e| format!("parse image config: {e}"))?;
    let cfg_os = config.get("os").and_then(Value::as_str);
    let cfg_arch = config.get("architecture").and_then(Value::as_str);
    if cfg_os != Some("linux") || cfg_arch != Some("amd64") {
        return Err(format!(
            "expected linux/amd64 image config, got {}/{}",
            cfg_os.unwrap_or("?"),
            cfg_arch.unwrap_or("?"),
        ));
    }
    let config_obj = config
        .as_object_mut()
        .ok_or("image config is not an object")?;
    config_obj.insert("architecture".into(), json!(native_arch));
    config_obj.remove("variant");
    let config_bytes = serde_json::to_vec(&config).map_err(|e| e.to_string())?;
    let new_config = sha256(&config_bytes);

    let config_desc = manifest
        .get_mut("config")
        .and_then(Value::as_object_mut)
        .ok_or("manifest config is not an object")?;
    config_desc.insert("digest".into(), json!(format!("sha256:{new_config}")));
    config_desc.insert("size".into(), json!(config_bytes.len()));
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|e| e.to_string())?;
    let new_manifest = sha256(&manifest_bytes);

    let media_type = manifest
        .get("mediaType")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or(desc_media_type)
        .unwrap_or_else(|| MT_OCI_MANIFEST.to_string());
    let mut desc = json!({
        "mediaType": media_type,
        "digest": format!("sha256:{new_manifest}"),
        "size": manifest_bytes.len(),
        "platform": {"architecture": native_arch, "os": "linux"},
    });
    if let Some(annotations) = top.get("annotations") {
        desc["annotations"] = annotations.clone();
    }
    let new_index = json!({
        "schemaVersion": 2,
        "mediaType": MT_OCI_INDEX,
        "manifests": [desc],
    });

    // Write back any buffered blob referenced as a layer (e.g. a small
    // uncompressed tar whose leading bytes looked like JSON), omitting the
    // superseded amd64 index/manifest/config blobs.
    let layer_hexes: HashSet<&str> = manifest
        .get("layers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|l| l.get("digest")?.as_str()?.strip_prefix("sha256:"))
        .collect();
    let mut kept: Vec<&String> = blobs
        .keys()
        .filter(|hex| layer_hexes.contains(hex.as_str()))
        .collect();
    kept.sort();
    for hex in kept {
        append(&mut out, &format!("blobs/sha256/{hex}"), &blobs[hex])?;
    }
    append(
        &mut out,
        &format!("blobs/sha256/{new_config}"),
        &config_bytes,
    )?;
    append(
        &mut out,
        &format!("blobs/sha256/{new_manifest}"),
        &manifest_bytes,
    )?;
    append(&mut out, "oci-layout", br#"{"imageLayoutVersion":"1.0.0"}"#)?;
    let index_bytes = serde_json::to_vec(&new_index).map_err(|e| e.to_string())?;
    append(&mut out, "index.json", &index_bytes)?;
    out.into_inner()
        .and_then(|mut w| w.flush())
        .map_err(|e| format!("finish archive: {e}"))?;

    Ok(Rewritten {
        manifest: format!("sha256:{new_manifest}"),
        config: format!("sha256:{new_config}"),
    })
}

/// Follow `desc` (a manifest, or an index / manifest list) to the
/// `linux/amd64` image manifest present in the archive. Returns the manifest
/// digest and the media type its descriptor declared, if any.
fn find_amd64_manifest(
    desc: &Value,
    blobs: &HashMap<String, Vec<u8>>,
    depth: usize,
) -> Result<(String, Option<String>), String> {
    let digest = desc
        .get("digest")
        .and_then(Value::as_str)
        .ok_or("descriptor has no digest")?;
    let desc_media_type = desc.get("mediaType").and_then(Value::as_str);
    let parsed: Value =
        serde_json::from_slice(blob(blobs, digest)?).map_err(|e| format!("parse {digest}: {e}"))?;
    let media_type = desc_media_type
        .or_else(|| parsed.get("mediaType").and_then(Value::as_str))
        .unwrap_or("");
    let is_index = media_type == MT_OCI_INDEX
        || media_type == MT_DOCKER_LIST
        || (media_type.is_empty() && parsed.get("manifests").is_some());
    if !is_index {
        return Ok((digest.to_string(), desc_media_type.map(str::to_string)));
    }
    if depth >= 4 {
        return Err(format!("{digest}: image indexes nested too deeply"));
    }
    let children = parsed
        .get("manifests")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for child in children {
        let os = child.pointer("/platform/os").and_then(Value::as_str);
        let arch = child
            .pointer("/platform/architecture")
            .and_then(Value::as_str);
        let present = child
            .get("digest")
            .and_then(Value::as_str)
            .is_some_and(|d| blob(blobs, d).is_ok());
        if os == Some("linux") && arch == Some("amd64") && present {
            return find_amd64_manifest(child, blobs, depth + 1);
        }
    }
    Err(format!(
        "{digest} has no linux/amd64 manifest in the archive"
    ))
}

fn looks_like_json_object(prefix: &[u8]) -> bool {
    prefix
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|&b| b == b'{')
}

fn read_prefix(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

fn blob<'a>(blobs: &'a HashMap<String, Vec<u8>>, digest: &str) -> Result<&'a [u8], String> {
    let hex = digest
        .strip_prefix("sha256:")
        .ok_or_else(|| format!("unsupported digest {digest}"))?;
    blobs
        .get(hex)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("blob {digest} missing from export archive"))
}

fn read_all(r: &mut impl Read, what: &str) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    r.read_to_end(&mut buf)
        .map_err(|e| format!("read {what}: {e}"))?;
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

    /// An archive shaped like `ctr images export --local --platform linux/amd64`
    /// of an amd64 image: index.json -> (optional index ->) manifest -> config,
    /// with a small non-JSON layer (streamed), a small `{`-prefixed layer
    /// (buffered then retained), and a large layer (streamed).
    fn amd64_archive(with_index: bool) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut b = tar::Builder::new(Vec::new());
        let small = b"\x1f\x8bsmall gzip layer".to_vec();
        let json_like = b"{json-looking small layer}".to_vec();
        let large = vec![7u8; (METADATA_LIMIT + 1) as usize];
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
        let top = if with_index {
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
            json!({"mediaType": MT_OCI_INDEX, "digest": idx_d, "size": idx.len(),
                   "annotations": annotations})
        } else {
            json!({"mediaType": MT_DOCKER_MANIFEST, "digest": manifest_d, "size": manifest.len(),
                   "annotations": annotations})
        };
        add(&mut b, "manifest.json", b"[]");
        add(&mut b, "oci-layout", br#"{"imageLayoutVersion":"1.0.0"}"#);
        let index = serde_json::to_vec(&json!({"schemaVersion": 2, "manifests": [top]})).unwrap();
        add(&mut b, "index.json", &index);
        (b.into_inner().unwrap(), small, json_like, large)
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
        let (input, small, json_like, large) = amd64_archive(with_index);
        let mut output = Vec::new();
        let r = rewrite_platform(input.as_slice(), &mut output, "arm64").unwrap();
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

        // Layers are untouched and present, including streamed and json-like ones.
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
    fn platform_detection() {
        assert!(has_native_platform("linux/arm64", "arm64"));
        assert!(has_native_platform("linux/amd64,linux/arm64/v8", "arm64"));
        assert!(!has_native_platform("linux/amd64", "arm64"));
        assert!(!has_native_platform("linux/arm/v7", "arm64"));
        assert!(!has_native_platform("", "arm64"));
        assert!(!has_native_platform("-", "arm64"));

        assert!(has_amd64_platform("linux/amd64"));
        assert!(has_amd64_platform("linux/386,linux/amd64/v3"));
        assert!(has_amd64_platform("linux/amd64,linux/arm64/v8"));
        assert!(!has_amd64_platform("linux/arm64/v8"));
        assert!(!has_amd64_platform("windows/amd64"));
        assert!(!has_amd64_platform(""));
        assert!(!has_amd64_platform("-"));
    }
}
