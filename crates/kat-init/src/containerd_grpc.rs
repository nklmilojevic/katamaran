//! Minimal HTTP/2 (h2c) gRPC client over containerd's Unix domain socket for
//! operations that `ctr` either does not expose or cannot perform without
//! duplicating layer payloads or losing structured metadata:
//!
//! - `Images.List`: returns structured `map<string, string>` labels and target
//!   descriptors so label values containing commas can never spoof ownership
//!   markers.
//! - `Images.Delete` with `DeleteImageRequest.target` (and source-guarded
//!   `retire_image_if_unchanged`): verifies `moby`'s source digest and performs
//!   an atomic compare-and-delete inside containerd's BoltDB write transaction,
//!   leaving any concurrent replacement image untouched.
//! - `Content.Info`: resolves authoritative blob sizes via direct O(1)
//!   digest-keyed lookups (never `Content.List`, which scans the entire
//!   namespace in containerd v2.4.2) so unrelated `moby` blobs never slow or
//!   block a rewrite.
//! - Zero-copy rewritten image publication (`Leases.Create`/`Delete`,
//!   `Content.Info`/`Write`/`Update`, `Images.Get`/`Delete`/`Create`): links
//!   existing `moby` layer blobs into `k8s.io` through containerd's shared
//!   content commit path without reading, transmitting, or copying layer bytes
//!   to disk, writes only the rewritten config and manifest blobs with GC child
//!   labels, re-checks the `moby` source digest, and publishes the tag via
//!   guarded `Images.Create` (or atomic compare-and-delete + `Images.Create`
//!   when replacing a known `k8s.io` target) so concurrent native kubelet pulls
//!   are never overwritten.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::oci_rewrite::{RewritePlan, Rewritten, is_digest_reference};
use crate::unix_connect::connect_unix_until;

const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

const FRAME_DATA: u8 = 0x00;
const FRAME_HEADERS: u8 = 0x01;
const FRAME_RST_STREAM: u8 = 0x03;
const FRAME_SETTINGS: u8 = 0x04;
const FRAME_PING: u8 = 0x06;
const FRAME_GOAWAY: u8 = 0x07;
const FRAME_WINDOW_UPDATE: u8 = 0x08;
const FRAME_CONTINUATION: u8 = 0x09;

const FLAG_END_STREAM: u8 = 0x01;
const FLAG_ACK: u8 = 0x01;
const FLAG_END_HEADERS: u8 = 0x04;
const FLAG_PADDED: u8 = 0x08;
const FLAG_PRIORITY: u8 = 0x20;

const SETTINGS_HEADER_TABLE_SIZE: u16 = 0x01;
const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x04;
const SETTINGS_MAX_FRAME_SIZE: u16 = 0x05;

const DEFAULT_H2_WINDOW: i64 = 65_535;
const CLIENT_RECV_WINDOW: u32 = 1 << 20;
const DEFAULT_MAX_FRAME_SIZE: usize = 16_384;
const MAX_SERVER_FRAME: usize = 1 << 20;
const MAX_HEADER_BLOCK: usize = 256 << 10;
const DEFAULT_DYNAMIC_TABLE_SIZE: usize = 4096;
const DEFAULT_UNARY_RESPONSE_LIMIT: usize = 1 << 20;
/// Chunk size for streaming metadata blobs over `Content.Write` (fits well
/// inside HTTP/2's default 65,535-byte window so each chunk round-trips cleanly).
const CONTENT_WRITE_CHUNK: usize = 32_768;
/// Maximum number of digest-keyed `Content.Info` lookups processed per chunk in
/// [`ContentInfoClient::batch_blob_sizes`].
pub const CONTENT_INFO_BATCH_SIZE: usize = 64;

const GRPC_OK: &str = "0";
const GRPC_NOT_FOUND: &str = "5";
const GRPC_ALREADY_EXISTS: &str = "6";

const H2_ERR_CANCEL: u32 = 0x8;

const LEASE_HEADER: &str = "containerd-lease";
const LEASE_EXPIRE_LABEL: &str = "containerd.io/gc.expire";
const LEASE_TTL_SECS: u64 = 600;
/// Per-RPC timeout sent via `grpc-timeout` on `Images.Delete` when replacing an
/// existing `k8s.io` tag, bounding how long an uncertain delete can remain in
/// flight on the server before recovery's BoltDB write barrier settles it.
const DELETE_RPC_TIMEOUT: Duration = Duration::from_secs(2);
const DELETE_SERVER_GRACE: Duration = Duration::from_millis(250);
const UNCERTAIN_DELETE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const RECOVERY_OP_TIMEOUT: Duration = Duration::from_secs(5);

/// OCI content descriptor (`containerd.types.Descriptor`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Descriptor {
    pub media_type: String,
    pub digest: String,
    pub size: i64,
    pub annotations: HashMap<String, String>,
}

/// Structured image record (`containerd.services.images.v1.Image`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerdImage {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub target: Descriptor,
}

/// Content blob metadata (`containerd.services.content.v1.Info`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentInfo {
    pub digest: String,
    pub size: i64,
}

/// Outcome of a conditional `Images.Delete` RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteOutcome {
    /// Image existed with the expected target digest and was deleted.
    Deleted,
    /// Image was already absent, or its target digest changed (for example,
    /// replaced by a concurrent native kubelet pull) and was left untouched.
    NotFoundOrChanged,
}

/// Outcome of a source-and-target guarded [`retire_image_if_unchanged`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireOutcome {
    /// `moby` still matched the expected source digest (or remained absent for
    /// orphan cleanup) and `k8s.io` still matched `expected_k8s_digest`, so the
    /// `k8s.io` mirror was deleted.
    Deleted,
    /// `k8s.io` was already absent or its target digest changed concurrently
    /// (for example, replaced by a native kubelet pull) and was left untouched.
    NotFoundOrChanged,
    /// `moby` changed concurrently (retagged, created, or deleted); `k8s.io`
    /// was left untouched so the caller can re-list and re-evaluate.
    SourceChanged,
}

/// Error returned by [`publish_rewritten_image`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishError {
    /// The `moby` source image was deleted or retagged before publication
    /// completed; no `k8s.io` image record was modified.
    SourceChanged { expected: String, actual: String },
    /// The `k8s.io` destination tag changed concurrently (for example, created
    /// or replaced by a native kubelet pull); the existing `k8s.io` tag was
    /// left untouched.
    DestinationChanged { expected: String, actual: String },
    /// Any other containerd gRPC, content, or I/O error.
    Failed(String),
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceChanged { expected, actual } => write!(
                f,
                "source image digest changed before publish (expected {expected}, got {actual})"
            ),
            Self::DestinationChanged { expected, actual } => write!(
                f,
                "k8s.io destination digest changed before publish (expected {expected}, got {actual})"
            ),
            Self::Failed(msg) => f.write_str(msg),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteOutcome {
    Committed,
    AlreadyExists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateOutcome {
    Created,
    AlreadyExists,
}

struct GrpcResponse {
    grpc_status: String,
    grpc_message: String,
    messages: Vec<Vec<u8>>,
}

/// List all images in `namespace` via `/containerd.services.images.v1.Images/List`,
/// returning authoritative target digests and structured label maps without
/// text splitting. Fails explicitly if the response exceeds `max_response_bytes`.
pub fn list_images(
    socket_path: &Path,
    namespace: &str,
    max_response_bytes: usize,
    timeout: Duration,
) -> Result<Vec<ContainerdImage>, String> {
    let deadline = Instant::now() + timeout;
    let mut conn = GrpcConn::connect(socket_path, deadline)?;
    let resp = conn.unary_rpc(
        "/containerd.services.images.v1.Images/List",
        namespace,
        None,
        &[],
        max_response_bytes,
    )?;
    if resp.grpc_status != GRPC_OK {
        return Err(format!(
            "containerd Images.List ({namespace}) failed (grpc-status {}): {}",
            resp.grpc_status, resp.grpc_message
        ));
    }
    let body = resp.messages.first().map(Vec::as_slice).unwrap_or(&[]);
    decode_list_images_response(body)
}

/// Reusable containerd gRPC client for direct digest-keyed `Content.Info`
/// metadata lookups over a single deadline-bounded Unix-socket HTTP/2
/// connection (never calling `Content.List`, which scans the entire namespace
/// in containerd v2.4.2).
pub struct ContentInfoClient {
    conn: GrpcConn,
}

impl ContentInfoClient {
    pub fn connect(socket_path: &Path, timeout: Duration) -> Result<Self, String> {
        let deadline = Instant::now() + timeout;
        Ok(Self {
            conn: GrpcConn::connect(socket_path, deadline)?,
        })
    }

    /// Look up the authoritative committed byte size of `digest` in `namespace`.
    /// Returns `Ok(None)` when containerd reports gRPC `NOT_FOUND`, and `Err` on
    /// transport or RPC failures.
    pub fn blob_size(&mut self, namespace: &str, digest: &str) -> Result<Option<u64>, String> {
        if !is_valid_oci_digest(digest) {
            return Ok(None);
        }
        let Some(info) = self.conn.content_info(namespace, digest)? else {
            return Ok(None);
        };
        let size = u64::try_from(info.size)
            .map_err(|_| format!("invalid negative size {} for {digest}", info.size))?;
        Ok(Some(size))
    }

    /// Look up authoritative committed byte sizes for a batch of digests in
    /// `namespace` via direct digest-keyed `Content.Info` RPCs in bounded
    /// chunks of [`CONTENT_INFO_BATCH_SIZE`], returning ONLY the subset of
    /// digests that exist locally.
    ///
    /// Invalid digest strings are skipped locally, duplicates are queried once,
    /// `NOT_FOUND` omits the digest, and all other RPC or shared-deadline
    /// failures propagate immediately. Never calls `Content.List`.
    pub fn batch_blob_sizes(
        &mut self,
        namespace: &str,
        digests: &[&str],
    ) -> Result<HashMap<String, u64>, String> {
        let mut seen = HashSet::new();
        let mut valid: Vec<&str> = Vec::new();
        for &digest in digests {
            if is_valid_oci_digest(digest) && seen.insert(digest) {
                valid.push(digest);
            }
        }
        let mut out = HashMap::new();
        for chunk in valid.chunks(CONTENT_INFO_BATCH_SIZE) {
            for &digest in chunk {
                if let Some(size) = self.blob_size(namespace, digest)? {
                    out.insert(digest.to_string(), size);
                }
            }
        }
        Ok(out)
    }

    /// Look up the current target digest of `name` in `namespace` via
    /// `Images.Get`, returning `Ok(None)` when absent.
    pub fn image_digest(&mut self, namespace: &str, name: &str) -> Result<Option<String>, String> {
        Ok(self
            .conn
            .get_image(namespace, name)?
            .map(|img| img.target.digest))
    }
}

/// Call containerd's `/containerd.services.images.v1.Images/Delete` RPC in
/// `namespace` with `DeleteImageRequest.target.digest` set to `target_digest`.
///
/// Containerd atomically checks that the stored image still points to
/// `target_digest` before deleting it, preserving any replacement image if the
/// tag changed after it was inspected.
#[cfg(test)]
fn delete_image_if_target_matches(
    socket_path: &Path,
    namespace: &str,
    name: &str,
    target_digest: &str,
    timeout: Duration,
) -> Result<DeleteOutcome, String> {
    let deadline = Instant::now() + timeout;
    let mut conn = GrpcConn::connect(socket_path, deadline)?;
    conn.delete_image_if_target_matches(namespace, name, target_digest)
}

/// Guarded retirement of a Katamaran-created `k8s.io` mirror over a single
/// bounded HTTP/2 connection:
///
/// 1. Queries `Images.Get("moby", name)` and confirms that `moby` still matches
///    `expected_moby_digest` (`Some(digest)` when retiring for a known source
///    digest, or `None` when retiring an orphan after `name` disappeared from
///    `moby`). If `moby` changed concurrently, returns
///    [`RetireOutcome::SourceChanged`] without touching `k8s.io`.
/// 2. Calls conditional `Images.Delete("k8s.io", name, target = expected_k8s_digest)`
///    so any concurrent native replacement in `k8s.io` is also preserved.
pub fn retire_image_if_unchanged(
    socket_path: &Path,
    name: &str,
    expected_moby_digest: Option<&str>,
    expected_k8s_digest: &str,
    timeout: Duration,
) -> Result<RetireOutcome, String> {
    if name.is_empty() {
        return Err("refusing to retire image with empty name".into());
    }
    if expected_k8s_digest.is_empty() {
        return Err(format!(
            "refusing to retire {name} without an expected k8s.io target digest"
        ));
    }

    let deadline = Instant::now() + timeout;
    let mut conn = GrpcConn::connect(socket_path, deadline)?;
    let current_moby = conn.get_image("moby", name)?;
    let actual_moby_digest = current_moby.as_ref().map(|i| i.target.digest.as_str());
    if actual_moby_digest != expected_moby_digest {
        return Ok(RetireOutcome::SourceChanged);
    }

    match conn.delete_image_if_target_matches("k8s.io", name, expected_k8s_digest)? {
        DeleteOutcome::Deleted => Ok(RetireOutcome::Deleted),
        DeleteOutcome::NotFoundOrChanged => Ok(RetireOutcome::NotFoundOrChanged),
    }
}

/// Publish a metadata-only [`RewritePlan`] into `k8s.io` without reading,
/// transmitting, or writing a second copy of any layer blobs.
///
/// Existing layer blobs from `moby` are linked into `k8s.io` via containerd's
/// shared-content `Content.Write` commit (`offset == size`, empty `data`), only
/// the rewritten config and manifest blobs are written (with containerd GC
/// child labels on the manifest), the `moby` source image digest is verified
/// both before and immediately prior to image publication, and a temporary
/// lease protects in-flight metadata from GC until guarded publication
/// completes.
///
/// `expected_k8s_digest` prevents overwriting concurrent Kubernetes image pulls:
/// - `None`: publishes strictly via `Images.Create`; if the tag already exists
///   (e.g. a concurrent native kubelet pull), returns
///   [`PublishError::DestinationChanged`] without touching `k8s.io`.
/// - `Some(digest)`: verifies `k8s.io` still points to `digest`, atomically
///   compare-and-deletes `digest` via `Images.Delete(target = digest)`, and
///   creates the new record via `Images.Create` (never unconditional
///   `Images.Update`).
enum RestoreState {
    /// `Images.Delete` definitely committed, and the subsequent `Images.Create`
    /// failed. Recovery can restore immediately if the tag is absent.
    DefiniteDelete(ContainerdImage),
    /// `Images.Delete` returned an uncertain error (timeout or dropped
    /// connection) and may still commit in BoltDB until `server_deadline` +
    /// a BoltDB write-lock barrier completes.
    UncertainDelete {
        old_image: ContainerdImage,
        server_deadline: Instant,
    },
}

pub fn publish_rewritten_image(
    socket_path: &Path,
    name: &str,
    source_digest_label: &str,
    expected_k8s_digest: Option<&str>,
    plan: &RewritePlan,
    timeout: Duration,
) -> Result<Rewritten, PublishError> {
    publish_rewritten_image_with_delete_timeout(
        socket_path,
        name,
        source_digest_label,
        expected_k8s_digest,
        plan,
        timeout,
        DELETE_RPC_TIMEOUT,
    )
}

fn publish_rewritten_image_with_delete_timeout(
    socket_path: &Path,
    name: &str,
    source_digest_label: &str,
    expected_k8s_digest: Option<&str>,
    plan: &RewritePlan,
    timeout: Duration,
    delete_rpc_timeout: Duration,
) -> Result<Rewritten, PublishError> {
    if name.is_empty() {
        return Err(PublishError::Failed(
            "refusing to publish rewritten image with empty name".into(),
        ));
    }
    if is_digest_reference(name) {
        return Err(PublishError::Failed(format!(
            "refusing to rewrite digest-qualified image reference {name}; tag it locally first"
        )));
    }
    if matches!(expected_k8s_digest, Some("")) {
        return Err(PublishError::Failed(format!(
            "refusing to publish {name} with empty expected k8s.io target digest"
        )));
    }

    let deadline = Instant::now() + timeout;
    let mut conn = GrpcConn::connect(socket_path, deadline).map_err(PublishError::Failed)?;

    // 1. Verify `moby` still maps `name` to `plan.source_digest`.
    verify_moby_source_digest(&mut conn, name, &plan.source_digest)?;

    // 2. Look up authoritative stored sizes for each unique layer in `moby`.
    let mut unique_layers: Vec<(&str, i64)> = Vec::new();
    let mut seen_layers: HashSet<&str> = HashSet::new();
    for digest in &plan.layer_digests {
        if !seen_layers.insert(digest.as_str()) {
            continue;
        }
        let info = conn
            .content_info("moby", digest)
            .map_err(PublishError::Failed)?;
        let Some(info) = info else {
            // Re-check whether `moby` retagged or removed the source image.
            verify_moby_source_digest(&mut conn, name, &plan.source_digest)?;
            return Err(PublishError::Failed(format!(
                "layer blob {digest} missing from moby content store"
            )));
        };
        if info.size < 0 {
            return Err(PublishError::Failed(format!(
                "invalid negative size {} for layer {digest}",
                info.size
            )));
        }
        unique_layers.push((digest.as_str(), info.size));
    }

    // 3. Create a temporary GC lease in `k8s.io` with a server-generated random
    //    ID (so lease IDs never repeat across guest boots), a fallback
    //    expiration timestamp, and guaranteed deletion before returning.
    let expire = rfc3339_utc_after(LEASE_TTL_SECS);
    let lease_id = conn
        .create_lease("k8s.io", &[(LEASE_EXPIRE_LABEL, &expire)])
        .map_err(PublishError::Failed)?;

    let mut restore_state: Option<RestoreState> = None;
    let mut publish_res = publish_under_lease(
        &mut conn,
        name,
        source_digest_label,
        expected_k8s_digest,
        plan,
        &unique_layers,
        &lease_id,
        delete_rpc_timeout,
        &mut restore_state,
    );

    // If replacing an existing destination failed after `Images.Delete` was
    // attempted (or its outcome was uncertain), shut down and drop the possibly
    // broken/expired publish connection and recover BEFORE deleting the lease
    // (which pins the old target content) using a fresh bounded connection.
    if let Some(state) = restore_state {
        let _ = conn.stream.shutdown(std::net::Shutdown::Both);
        drop(conn);
        if let Err(restore_err) =
            restore_deleted_image_and_cleanup_lease(socket_path, "k8s.io", &lease_id, &state)
            && let Err(PublishError::Failed(ref mut msg)) = publish_res
        {
            msg.push_str(&format!("; failed to restore previous tag: {restore_err}"));
        }
        return publish_res;
    }

    // Best-effort lease cleanup: try on the active connection first, falling
    // back to a fresh bounded dial if the connection broke mid-publish.
    if conn.delete_lease("k8s.io", &lease_id).is_err()
        && let Ok(mut cleanup_conn) =
            GrpcConn::connect(socket_path, Instant::now() + Duration::from_secs(2))
    {
        let _ = cleanup_conn.delete_lease("k8s.io", &lease_id);
    }

    publish_res
}

fn restore_deleted_image_and_cleanup_lease(
    socket_path: &Path,
    namespace: &str,
    lease_id: &str,
    restore_state: &RestoreState,
) -> Result<(), String> {
    let conn_deadline = match restore_state {
        RestoreState::DefiniteDelete(_) => Instant::now() + RECOVERY_OP_TIMEOUT,
        RestoreState::UncertainDelete {
            server_deadline, ..
        } => (*server_deadline + RECOVERY_OP_TIMEOUT).max(Instant::now() + RECOVERY_OP_TIMEOUT),
    };
    let mut recovery_conn = GrpcConn::connect(socket_path, conn_deadline)?;

    let restore_res = match restore_state {
        RestoreState::DefiniteDelete(old_image) => {
            restore_if_absent(&mut recovery_conn, namespace, lease_id, old_image)
        }
        RestoreState::UncertainDelete {
            old_image,
            server_deadline,
        } => recover_uncertain_delete(
            &mut recovery_conn,
            namespace,
            lease_id,
            old_image,
            *server_deadline,
        ),
    };

    // Only delete the temporary lease once recovery is confirmed complete. If
    // recovery failed, keep `lease_id` pinned until its `gc.expire` TTL so
    // containerd GC cannot collect `old_image.target.digest` prematurely.
    if restore_res.is_ok()
        && recovery_conn.delete_lease(namespace, lease_id).is_err()
        && let Ok(mut cleanup_conn) =
            GrpcConn::connect(socket_path, Instant::now() + Duration::from_secs(2))
    {
        let _ = cleanup_conn.delete_lease(namespace, lease_id);
    }
    restore_res
}

fn restore_if_absent(
    conn: &mut GrpcConn,
    namespace: &str,
    lease_id: &str,
    old_image: &ContainerdImage,
) -> Result<(), String> {
    match conn.get_image(namespace, &old_image.name)? {
        Some(_) => Ok(()),
        None => match conn.create_image(namespace, Some(lease_id), old_image)? {
            CreateOutcome::Created | CreateOutcome::AlreadyExists => Ok(()),
        },
    }
}

fn recover_uncertain_delete(
    conn: &mut GrpcConn,
    namespace: &str,
    lease_id: &str,
    old_image: &ContainerdImage,
    server_deadline: Instant,
) -> Result<(), String> {
    // While an uncertain `Images.Delete(name, target = old_digest)` may still
    // commit on the server, never conclude recovery early just because
    // `Images.Get` still sees `old_image`:
    // - If a competing writer replaced the tag with a DIFFERENT digest, our
    //   conditional `Delete(target = old_digest)` can never match it -> done.
    // - If the tag disappeared (`None`), immediately restore `old_image` via
    //   create-only `Images.Create` so pods do not observe a missing tag, then
    //   continue until `server_deadline` + the BoltDB write barrier so no late
    //   delete can undo the restore.
    loop {
        match conn.get_image(namespace, &old_image.name)? {
            Some(cur) if cur.target.digest != old_image.target.digest => return Ok(()),
            Some(_) => {}
            None => match conn.create_image(namespace, Some(lease_id), old_image)? {
                CreateOutcome::Created => {}
                CreateOutcome::AlreadyExists => {
                    if let Some(cur) = conn.get_image(namespace, &old_image.name)?
                        && cur.target.digest != old_image.target.digest
                    {
                        return Ok(());
                    }
                }
            },
        }
        let Some(remaining) = server_deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        if remaining.is_zero() {
            break;
        }
        std::thread::sleep(remaining.min(UNCERTAIN_DELETE_POLL_INTERVAL));
    }

    // BoltDB write-lock barrier: `Leases.AddResource` is idempotent
    // (`OK | ALREADY_EXISTS`) and executes a BoltDB write transaction strictly
    // after any FIFO-queued `Images.Delete` that entered BoltDB before
    // `server_deadline` expired.
    conn.add_lease_resource(namespace, lease_id, &old_image.target.digest, "content")?;
    restore_if_absent(conn, namespace, lease_id, old_image)
}

#[allow(clippy::too_many_arguments)]
fn publish_under_lease(
    conn: &mut GrpcConn,
    name: &str,
    source_digest_label: &str,
    expected_k8s_digest: Option<&str>,
    plan: &RewritePlan,
    unique_layers: &[(&str, i64)],
    lease_id: &str,
    delete_rpc_timeout: Duration,
    restore_state: &mut Option<RestoreState>,
) -> Result<Rewritten, PublishError> {
    // 4. Zero-copy link every existing layer blob into `k8s.io`.
    for (idx, &(digest, size)) in unique_layers.iter().enumerate() {
        let write_ref = format!("kat-link-{lease_id}-{idx}");
        if let Err(e) = conn.link_shared_content("k8s.io", lease_id, &write_ref, digest, size) {
            let _ = conn.abort_content("k8s.io", lease_id, &write_ref);
            return Err(PublishError::Failed(format!(
                "zero-copy link layer {digest} into k8s.io: {e}"
            )));
        }
    }

    // 5. Write the rewritten config blob into `k8s.io`.
    let config_digest = format!("sha256:{}", plan.new_config_hex);
    let cfg_ref = format!("kat-cfg-{lease_id}");
    if let Err(e) = conn.write_content_blob(
        "k8s.io",
        lease_id,
        &cfg_ref,
        &config_digest,
        &plan.new_config_bytes,
        &[],
    ) {
        let _ = conn.abort_content("k8s.io", lease_id, &cfg_ref);
        return Err(PublishError::Failed(format!(
            "write rewritten config {config_digest}: {e}"
        )));
    }

    // 6. Write the rewritten manifest blob with containerd GC child labels so
    //    `k8s.io` GC keeps the rewritten config and every linked layer alive.
    let manifest_digest = format!("sha256:{}", plan.new_manifest_hex);
    let mut gc_labels: Vec<(String, String)> = Vec::with_capacity(1 + plan.layer_digests.len());
    gc_labels.push((
        "containerd.io/gc.ref.content.config".into(),
        config_digest.clone(),
    ));
    for (idx, layer_digest) in plan.layer_digests.iter().enumerate() {
        gc_labels.push((
            format!("containerd.io/gc.ref.content.l.{idx}"),
            layer_digest.clone(),
        ));
    }
    let manifest_ref = format!("kat-manifest-{lease_id}");
    match conn.write_content_blob(
        "k8s.io",
        lease_id,
        &manifest_ref,
        &manifest_digest,
        &plan.new_manifest_bytes,
        &gc_labels,
    ) {
        Ok(WriteOutcome::Committed) => {}
        Ok(WriteOutcome::AlreadyExists) => {
            conn.update_content_labels("k8s.io", Some(lease_id), &manifest_digest, &gc_labels)
                .map_err(|e| {
                    PublishError::Failed(format!(
                        "ensure GC labels on manifest {manifest_digest}: {e}"
                    ))
                })?;
        }
        Err(e) => {
            let _ = conn.abort_content("k8s.io", lease_id, &manifest_ref);
            return Err(PublishError::Failed(format!(
                "write rewritten manifest {manifest_digest}: {e}"
            )));
        }
    }

    // 7. Source-digest guard immediately prior to publishing the `k8s.io` tag:
    //    if Docker concurrently retagged or deleted the source image, abort
    //    without touching `k8s.io` so the lease cleanup releases any unused
    //    rewritten metadata.
    verify_moby_source_digest(conn, name, &plan.source_digest)?;

    // 8. Guarded publication of the `k8s.io` image record (never unconditional
    //    `Images.Update`, so a concurrent native kubelet pull cannot be
    //    overwritten).
    let manifest_size = i64::try_from(plan.new_manifest_bytes.len())
        .map_err(|_| PublishError::Failed("rewritten manifest size overflows i64".into()))?;
    let image = ContainerdImage {
        name: name.to_string(),
        labels: HashMap::from([(source_digest_label.to_string(), plan.source_digest.clone())]),
        target: Descriptor {
            media_type: plan.manifest_media_type.clone(),
            digest: manifest_digest.clone(),
            size: manifest_size,
            annotations: HashMap::new(),
        },
    };
    match expected_k8s_digest {
        None => match conn
            .create_image("k8s.io", Some(lease_id), &image)
            .map_err(PublishError::Failed)?
        {
            CreateOutcome::Created => {}
            CreateOutcome::AlreadyExists => {
                return Err(PublishError::DestinationChanged {
                    expected: "<absent>".into(),
                    actual: "<exists>".into(),
                });
            }
        },
        Some(expected_k8s) => {
            let old_image = match conn
                .get_image("k8s.io", name)
                .map_err(PublishError::Failed)?
            {
                Some(cur)
                    if cur.target.digest == manifest_digest
                        && cur.labels.get(source_digest_label).map(String::as_str)
                            == Some(plan.source_digest.as_str()) =>
                {
                    return Ok(Rewritten {
                        manifest: manifest_digest,
                        config: config_digest,
                    });
                }
                Some(cur) if cur.target.digest != expected_k8s => {
                    return Err(PublishError::DestinationChanged {
                        expected: expected_k8s.to_string(),
                        actual: cur.target.digest,
                    });
                }
                None => {
                    return Err(PublishError::DestinationChanged {
                        expected: expected_k8s.to_string(),
                        actual: "<deleted>".into(),
                    });
                }
                Some(cur) => cur,
            };

            // Pin the existing image's target content under `lease_id` before
            // deleting so containerd GC cannot collect it if replacement
            // creation fails and recovery needs to restore `old_image`.
            conn.add_lease_resource("k8s.io", lease_id, &old_image.target.digest, "content")
                .map_err(|e| {
                    PublishError::Failed(format!(
                        "pin existing target {} under lease: {e}",
                        old_image.target.digest
                    ))
                })?;

            let del_timeout = remaining_or_timeout(conn.deadline, "Images.Delete")
                .map_err(PublishError::Failed)?
                .min(delete_rpc_timeout);
            let server_deadline = Instant::now() + del_timeout + DELETE_SERVER_GRACE;

            // Record `UncertainDelete` BEFORE sending `Images.Delete` so
            // recovery waits out the server-side RPC deadline + BoltDB write
            // barrier if `Images.Delete`'s outcome is uncertain (e.g.
            // connection drop or deadline expiry mid-RPC).
            *restore_state = Some(RestoreState::UncertainDelete {
                old_image: old_image.clone(),
                server_deadline,
            });

            // Atomic BoltDB compare-and-delete on `expected_k8s` with a tight
            // per-RPC `grpc-timeout`, followed by `Images.Create` under `lease_id`.
            match conn
                .delete_image_if_target_matches_with_timeout(
                    "k8s.io",
                    name,
                    expected_k8s,
                    del_timeout,
                )
                .map_err(PublishError::Failed)?
            {
                DeleteOutcome::Deleted => {
                    *restore_state = Some(RestoreState::DefiniteDelete(old_image));
                }
                DeleteOutcome::NotFoundOrChanged => {
                    *restore_state = None;
                    return Err(PublishError::DestinationChanged {
                        expected: expected_k8s.to_string(),
                        actual: "<changed>".into(),
                    });
                }
            }
            match conn
                .create_image("k8s.io", Some(lease_id), &image)
                .map_err(PublishError::Failed)?
            {
                CreateOutcome::Created => {
                    *restore_state = None;
                }
                CreateOutcome::AlreadyExists => {
                    *restore_state = None;
                    return Err(PublishError::DestinationChanged {
                        expected: expected_k8s.to_string(),
                        actual: "<exists>".into(),
                    });
                }
            }
        }
    }

    Ok(Rewritten {
        manifest: manifest_digest,
        config: config_digest,
    })
}

fn verify_moby_source_digest(
    conn: &mut GrpcConn,
    name: &str,
    expected_digest: &str,
) -> Result<(), PublishError> {
    let current = conn.get_image("moby", name).map_err(PublishError::Failed)?;
    match current {
        Some(img) if img.target.digest == expected_digest => Ok(()),
        Some(img) => Err(PublishError::SourceChanged {
            expected: expected_digest.to_string(),
            actual: img.target.digest,
        }),
        None => Err(PublishError::SourceChanged {
            expected: expected_digest.to_string(),
            actual: "<deleted>".to_string(),
        }),
    }
}

/// Stateful HTTP/2 (h2c) client connection over a Unix domain socket bounded
/// by a single shared deadline across connect, write, and read operations.
struct GrpcConn {
    stream: UnixStream,
    deadline: Instant,
    next_stream_id: u32,
    decoder: HpackDecoder,
    peer_conn_window: i64,
    peer_initial_stream_window: i64,
    peer_max_frame_size: usize,
}

impl GrpcConn {
    fn connect(socket_path: &Path, deadline: Instant) -> Result<Self, String> {
        let mut stream = connect_unix_until(socket_path, deadline)
            .map_err(|e| format!("connect {}: {e}", socket_path.display()))?;
        let left = remaining_or_timeout(deadline, "connect containerd")?;
        stream
            .set_read_timeout(Some(left))
            .map_err(|e| format!("set read timeout: {e}"))?;
        stream
            .set_write_timeout(Some(left))
            .map_err(|e| format!("set write timeout: {e}"))?;

        // Client connection preface + initial SETTINGS:
        // - HEADER_TABLE_SIZE = 0 (we still decode server dynamic entries if sent)
        // - INITIAL_WINDOW_SIZE = 1 MiB for large responses like Images.List
        stream
            .write_all(CLIENT_PREFACE)
            .map_err(|e| format!("write h2 preface: {e}"))?;
        let mut settings = Vec::with_capacity(12);
        settings.extend_from_slice(&SETTINGS_HEADER_TABLE_SIZE.to_be_bytes());
        settings.extend_from_slice(&0u32.to_be_bytes());
        settings.extend_from_slice(&SETTINGS_INITIAL_WINDOW_SIZE.to_be_bytes());
        settings.extend_from_slice(&CLIENT_RECV_WINDOW.to_be_bytes());
        write_frame(&mut stream, FRAME_SETTINGS, 0, 0, &settings)
            .map_err(|e| format!("write h2 settings: {e}"))?;
        let conn_inc = CLIENT_RECV_WINDOW - (DEFAULT_H2_WINDOW as u32);
        write_frame(
            &mut stream,
            FRAME_WINDOW_UPDATE,
            0,
            0,
            &conn_inc.to_be_bytes(),
        )
        .map_err(|e| format!("write h2 window update: {e}"))?;
        stream
            .flush()
            .map_err(|e| format!("flush h2 preface: {e}"))?;

        Ok(Self {
            stream,
            deadline,
            next_stream_id: 1,
            decoder: HpackDecoder::new(DEFAULT_DYNAMIC_TABLE_SIZE),
            peer_conn_window: DEFAULT_H2_WINDOW,
            peer_initial_stream_window: DEFAULT_H2_WINDOW,
            peer_max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        })
    }

    fn alloc_stream_id(&mut self) -> u32 {
        let sid = self.next_stream_id;
        self.next_stream_id = self.next_stream_id.wrapping_add(2);
        sid
    }

    fn apply_write_timeout(&self, op: &str) -> Result<Duration, String> {
        let left = remaining_or_timeout(self.deadline, op)?;
        let _ = self.stream.set_write_timeout(Some(left));
        Ok(left)
    }

    fn apply_read_timeout(&self, op: &str) -> Result<Duration, String> {
        let left = remaining_or_timeout(self.deadline, op)?;
        // Best-effort refresh: on macOS/BSD, setsockopt(SO_RCVTIMEO) returns
        // EINVAL once the peer closes its half of a Unix socket even while
        // response frames remain buffered in the kernel receive queue.
        let _ = self.stream.set_read_timeout(Some(left));
        Ok(left)
    }

    fn send_request_headers(
        &mut self,
        sid: u32,
        path: &str,
        namespace: &str,
        lease: Option<&str>,
    ) -> Result<(), String> {
        self.send_request_headers_with_timeout(sid, path, namespace, lease, None)
    }

    fn send_request_headers_with_timeout(
        &mut self,
        sid: u32,
        path: &str,
        namespace: &str,
        lease: Option<&str>,
        timeout_override: Option<Duration>,
    ) -> Result<(), String> {
        let left = self.apply_write_timeout(path)?;
        let grpc_timeout = timeout_override.map_or(left, |t| left.min(t));
        let timeout_hdr = format_grpc_timeout(grpc_timeout);
        let mut hdrs = vec![
            (":method", "POST"),
            (":scheme", "http"),
            (":path", path),
            (":authority", "localhost"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
            ("containerd-namespace", namespace),
            ("grpc-timeout", timeout_hdr.as_str()),
        ];
        if let Some(lid) = lease {
            hdrs.push((LEASE_HEADER, lid));
        }
        let encoded = encode_request_headers(&hdrs);
        write_frame(
            &mut self.stream,
            FRAME_HEADERS,
            FLAG_END_HEADERS,
            sid,
            &encoded,
        )
        .map_err(|e| format!("write h2 headers for {path}: {e}"))?;
        Ok(())
    }

    fn send_rst_stream_cancel(&mut self, sid: u32) {
        let _ = self
            .stream
            .set_write_timeout(Some(Duration::from_millis(100)));
        let _ = write_frame(
            &mut self.stream,
            FRAME_RST_STREAM,
            0,
            sid,
            &H2_ERR_CANCEL.to_be_bytes(),
        );
        let _ = self.stream.flush();
    }

    fn unary_rpc(
        &mut self,
        path: &str,
        namespace: &str,
        lease: Option<&str>,
        req_proto: &[u8],
        max_response_bytes: usize,
    ) -> Result<GrpcResponse, String> {
        self.unary_rpc_with_timeout(path, namespace, lease, req_proto, max_response_bytes, None)
    }

    fn unary_rpc_with_timeout(
        &mut self,
        path: &str,
        namespace: &str,
        lease: Option<&str>,
        req_proto: &[u8],
        max_response_bytes: usize,
        rpc_timeout: Option<Duration>,
    ) -> Result<GrpcResponse, String> {
        let saved_deadline = self.deadline;
        if let Some(t) = rpc_timeout {
            self.deadline = self.deadline.min(Instant::now() + t);
        }
        let sid = self.alloc_stream_id();
        let res = (|| {
            self.send_request_headers_with_timeout(sid, path, namespace, lease, rpc_timeout)?;
            let framed = frame_grpc_message(req_proto)?;
            let mut state =
                StreamState::new(sid, self.peer_initial_stream_window, max_response_bytes);
            self.send_stream_data(&mut state, &framed, true, path)?;
            self.stream
                .flush()
                .map_err(|e| format!("flush h2 request for {path}: {e}"))?;
            self.finish_stream(&mut state, path)
        })();
        self.deadline = saved_deadline;
        if res.is_err() {
            self.send_rst_stream_cancel(sid);
        }
        res
    }

    /// Send `bytes` as HTTP/2 DATA frames on `state.sid`, respecting peer
    /// connection and stream flow-control windows. If the peer closes `sid`
    /// early (for example, `AlreadyExists` trailers-only), stops sending cleanly.
    fn send_stream_data(
        &mut self,
        state: &mut StreamState,
        bytes: &[u8],
        end_stream: bool,
        op: &str,
    ) -> Result<(), String> {
        let mut offset = 0usize;
        while offset < bytes.len() && !state.ended {
            while (self.peer_conn_window <= 0 || state.peer_window <= 0) && !state.ended {
                self.read_one_frame(state, op)?;
            }
            if state.ended {
                return Ok(());
            }
            let avail_win = self.peer_conn_window.min(state.peer_window) as usize;
            let chunk_len = (bytes.len() - offset)
                .min(self.peer_max_frame_size)
                .min(avail_win);
            let is_last = offset + chunk_len == bytes.len();
            let flags = if is_last && end_stream {
                FLAG_END_STREAM
            } else {
                0
            };
            self.apply_write_timeout(op)?;
            write_frame(
                &mut self.stream,
                FRAME_DATA,
                flags,
                state.sid,
                &bytes[offset..offset + chunk_len],
            )
            .map_err(|e| format!("write h2 data for {op}: {e}"))?;
            self.peer_conn_window -= chunk_len as i64;
            state.peer_window -= chunk_len as i64;
            offset += chunk_len;
        }
        if bytes.is_empty() && end_stream && !state.ended {
            self.apply_write_timeout(op)?;
            write_frame(
                &mut self.stream,
                FRAME_DATA,
                FLAG_END_STREAM,
                state.sid,
                &[],
            )
            .map_err(|e| format!("write h2 end-stream for {op}: {e}"))?;
        }
        Ok(())
    }

    fn finish_stream(&mut self, state: &mut StreamState, op: &str) -> Result<GrpcResponse, String> {
        while !state.ended {
            self.read_one_frame(state, op)?;
        }
        if let Some(status) = &state.http_status
            && status != "200"
        {
            return Err(format!(
                "containerd {op} returned HTTP {status} ({})",
                state.grpc_message
            ));
        }
        let Some(grpc_status) = state.grpc_status.take() else {
            return Err(format!(
                "containerd {op} response missing grpc-status trailer"
            ));
        };
        if !state.raw_data.is_empty() && grpc_status == GRPC_OK {
            return Err(format!("containerd {op} returned truncated gRPC message"));
        }
        Ok(GrpcResponse {
            grpc_status,
            grpc_message: std::mem::take(&mut state.grpc_message),
            messages: std::mem::take(&mut state.messages),
        })
    }

    fn read_one_frame(&mut self, state: &mut StreamState, op: &str) -> Result<(), String> {
        self.apply_read_timeout(op)?;
        let (frame_type, flags, stream_id, payload) = read_frame(&mut self.stream)?;
        match frame_type {
            FRAME_SETTINGS => {
                if flags & FLAG_ACK == 0 {
                    let (chunks, rem) = payload.as_chunks::<6>();
                    if !rem.is_empty() {
                        return Err("invalid HTTP/2 SETTINGS payload length".into());
                    }
                    for chunk in chunks {
                        let id = u16::from_be_bytes([chunk[0], chunk[1]]);
                        let val = u32::from_be_bytes([chunk[2], chunk[3], chunk[4], chunk[5]]);
                        match id {
                            SETTINGS_INITIAL_WINDOW_SIZE => {
                                let new_win = val as i64;
                                let delta = new_win - self.peer_initial_stream_window;
                                self.peer_initial_stream_window = new_win;
                                state.peer_window += delta;
                            }
                            SETTINGS_MAX_FRAME_SIZE if (16_384..=16_777_215).contains(&val) => {
                                self.peer_max_frame_size = val as usize;
                            }
                            _ => {}
                        }
                    }
                    // Best-effort ACK: a peer that already sent the full
                    // response and closed its socket may return EPIPE while
                    // response frames remain buffered for us to read.
                    let _ = write_frame(&mut self.stream, FRAME_SETTINGS, FLAG_ACK, 0, &[]);
                    let _ = self.stream.flush();
                }
            }
            FRAME_PING => {
                if flags & FLAG_ACK == 0 && payload.len() == 8 {
                    let _ = write_frame(&mut self.stream, FRAME_PING, FLAG_ACK, 0, &payload);
                    let _ = self.stream.flush();
                }
            }
            FRAME_WINDOW_UPDATE => {
                if payload.len() == 4 {
                    let inc = (u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                        & 0x7fff_ffff) as i64;
                    if stream_id == 0 {
                        self.peer_conn_window += inc;
                    } else if stream_id == state.sid {
                        state.peer_window += inc;
                    }
                }
            }
            FRAME_GOAWAY => {
                let last_stream_id = if payload.len() >= 4 {
                    u32::from_be_bytes([payload[0] & 0x7f, payload[1], payload[2], payload[3]])
                } else {
                    0
                };
                if last_stream_id < state.sid {
                    let code = if payload.len() >= 8 {
                        u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]])
                    } else {
                        0
                    };
                    let debug = if payload.len() > 8 {
                        String::from_utf8_lossy(&payload[8..]).into_owned()
                    } else {
                        String::new()
                    };
                    return Err(format!(
                        "containerd closed HTTP/2 connection with GOAWAY (code {code}): {debug}"
                    ));
                }
            }
            FRAME_RST_STREAM if stream_id == state.sid => {
                let code = if payload.len() >= 4 {
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                } else {
                    0
                };
                return Err(format!(
                    "containerd reset HTTP/2 stream {} (code {code})",
                    state.sid
                ));
            }
            FRAME_DATA if stream_id == state.sid => {
                let flow_len = payload.len() as u32;
                let data = strip_data_padding(&payload, flags)?;
                let end_stream = flags & FLAG_END_STREAM != 0;
                if flow_len > 0 {
                    let _ = write_frame(
                        &mut self.stream,
                        FRAME_WINDOW_UPDATE,
                        0,
                        0,
                        &flow_len.to_be_bytes(),
                    );
                    if !end_stream {
                        let _ = write_frame(
                            &mut self.stream,
                            FRAME_WINDOW_UPDATE,
                            0,
                            state.sid,
                            &flow_len.to_be_bytes(),
                        );
                    }
                    let _ = self.stream.flush();
                }
                state.push_data(data, op)?;
                if end_stream {
                    state.ended = true;
                }
            }
            FRAME_HEADERS if stream_id == state.sid => {
                let end_stream = flags & FLAG_END_STREAM != 0;
                let mut block = strip_headers_framing(&payload, flags)?;
                let mut cur_flags = flags;
                while cur_flags & FLAG_END_HEADERS == 0 {
                    self.apply_read_timeout(op)?;
                    let (ct, cf, cs, cp) = read_frame(&mut self.stream)?;
                    if ct != FRAME_CONTINUATION || cs != state.sid {
                        return Err(format!(
                            "expected CONTINUATION on stream {}, got type {ct} on stream {cs}",
                            state.sid
                        ));
                    }
                    if block.len() + cp.len() > MAX_HEADER_BLOCK {
                        return Err("HTTP/2 header block exceeded limit".into());
                    }
                    block.extend_from_slice(&cp);
                    cur_flags = cf;
                }
                for (k, v) in self.decoder.decode(&block)? {
                    match k.as_str() {
                        ":status" => state.http_status = Some(v),
                        "grpc-status" => state.grpc_status = Some(v),
                        "grpc-message" => state.grpc_message = percent_decode(&v),
                        _ => {}
                    }
                }
                if end_stream {
                    state.ended = true;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn delete_image_if_target_matches(
        &mut self,
        namespace: &str,
        name: &str,
        target_digest: &str,
    ) -> Result<DeleteOutcome, String> {
        self.delete_image_if_target_matches_inner(namespace, name, target_digest, None)
    }

    fn delete_image_if_target_matches_with_timeout(
        &mut self,
        namespace: &str,
        name: &str,
        target_digest: &str,
        rpc_timeout: Duration,
    ) -> Result<DeleteOutcome, String> {
        self.delete_image_if_target_matches_inner(namespace, name, target_digest, Some(rpc_timeout))
    }

    fn delete_image_if_target_matches_inner(
        &mut self,
        namespace: &str,
        name: &str,
        target_digest: &str,
        rpc_timeout: Option<Duration>,
    ) -> Result<DeleteOutcome, String> {
        if name.is_empty() {
            return Err("refusing to delete image with empty name".into());
        }
        if target_digest.is_empty() {
            return Err(format!(
                "refusing to delete {name} without an expected target digest"
            ));
        }

        let proto = encode_delete_image_request(name, target_digest);
        let resp = self.unary_rpc_with_timeout(
            "/containerd.services.images.v1.Images/Delete",
            namespace,
            None,
            &proto,
            DEFAULT_UNARY_RESPONSE_LIMIT,
            rpc_timeout,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK => Ok(DeleteOutcome::Deleted),
            // gRPC status 5 = NOT_FOUND: returned both when the image does not
            // exist and when DeleteImageRequest.target.digest does not match.
            GRPC_NOT_FOUND => Ok(DeleteOutcome::NotFoundOrChanged),
            code => Err(format!(
                "containerd Images.Delete failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }

    fn get_image(
        &mut self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ContainerdImage>, String> {
        let mut req = Vec::with_capacity(name.len() + 4);
        encode_proto_bytes(&mut req, 1, name.as_bytes());
        let resp = self.unary_rpc(
            "/containerd.services.images.v1.Images/Get",
            namespace,
            None,
            &req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK => {
                let body = resp.messages.first().map(Vec::as_slice).unwrap_or(&[]);
                decode_get_image_response(body)
            }
            GRPC_NOT_FOUND => Ok(None),
            code => Err(format!(
                "containerd Images.Get {name} ({namespace}) failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }

    fn create_image(
        &mut self,
        namespace: &str,
        lease: Option<&str>,
        image: &ContainerdImage,
    ) -> Result<CreateOutcome, String> {
        let create_req = encode_create_image_request(image);
        let resp = self.unary_rpc(
            "/containerd.services.images.v1.Images/Create",
            namespace,
            lease,
            &create_req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK => Ok(CreateOutcome::Created),
            GRPC_ALREADY_EXISTS => Ok(CreateOutcome::AlreadyExists),
            code => Err(format!(
                "containerd Images.Create {} ({namespace}) failed (grpc-status {code}): {}",
                image.name, resp.grpc_message
            )),
        }
    }

    fn create_lease(&mut self, namespace: &str, labels: &[(&str, &str)]) -> Result<String, String> {
        let req = encode_create_lease_request(labels);
        let resp = self.unary_rpc(
            "/containerd.services.leases.v1.Leases/Create",
            namespace,
            None,
            &req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK => {
                let body = resp.messages.first().map(Vec::as_slice).unwrap_or(&[]);
                let id = decode_create_lease_response(body)?;
                if id.is_empty() {
                    return Err(format!(
                        "containerd Leases.Create ({namespace}) returned empty lease ID"
                    ));
                }
                Ok(id)
            }
            code => Err(format!(
                "containerd Leases.Create ({namespace}) failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }

    fn add_lease_resource(
        &mut self,
        namespace: &str,
        lease_id: &str,
        resource_id: &str,
        resource_type: &str,
    ) -> Result<(), String> {
        let req = encode_add_lease_resource_request(lease_id, resource_id, resource_type);
        let resp = self.unary_rpc(
            "/containerd.services.leases.v1.Leases/AddResource",
            namespace,
            None,
            &req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK | GRPC_ALREADY_EXISTS => Ok(()),
            code => Err(format!(
                "containerd Leases.AddResource {resource_type}/{resource_id} ({namespace}) failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }

    fn delete_lease(&mut self, namespace: &str, id: &str) -> Result<(), String> {
        let mut req = Vec::with_capacity(id.len() + 4);
        encode_proto_bytes(&mut req, 1, id.as_bytes());
        let resp = self.unary_rpc(
            "/containerd.services.leases.v1.Leases/Delete",
            namespace,
            None,
            &req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK | GRPC_NOT_FOUND => Ok(()),
            code => Err(format!(
                "containerd Leases.Delete {id} ({namespace}) failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }

    fn content_info(
        &mut self,
        namespace: &str,
        digest: &str,
    ) -> Result<Option<ContentInfo>, String> {
        let mut req = Vec::with_capacity(digest.len() + 4);
        encode_proto_bytes(&mut req, 1, digest.as_bytes());
        let resp = self.unary_rpc(
            "/containerd.services.content.v1.Content/Info",
            namespace,
            None,
            &req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK => {
                let body = resp.messages.first().map(Vec::as_slice).unwrap_or(&[]);
                decode_content_info_response(body)
            }
            GRPC_NOT_FOUND => Ok(None),
            code => Err(format!(
                "containerd Content.Info {digest} ({namespace}) failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }

    fn update_content_labels(
        &mut self,
        namespace: &str,
        lease: Option<&str>,
        digest: &str,
        labels: &[(String, String)],
    ) -> Result<(), String> {
        if labels.is_empty() {
            return Ok(());
        }
        let req = encode_update_content_labels_request(digest, labels);
        let resp = self.unary_rpc(
            "/containerd.services.content.v1.Content/Update",
            namespace,
            lease,
            &req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        if resp.grpc_status == GRPC_OK {
            Ok(())
        } else {
            Err(format!(
                "containerd Content.Update {digest} ({namespace}) failed (grpc-status {}): {}",
                resp.grpc_status, resp.grpc_message
            ))
        }
    }

    fn abort_content(
        &mut self,
        namespace: &str,
        lease: &str,
        write_ref: &str,
    ) -> Result<(), String> {
        let mut req = Vec::with_capacity(write_ref.len() + 4);
        encode_proto_bytes(&mut req, 1, write_ref.as_bytes());
        let resp = self.unary_rpc(
            "/containerd.services.content.v1.Content/Abort",
            namespace,
            Some(lease),
            &req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK | GRPC_NOT_FOUND => Ok(()),
            code => Err(format!(
                "containerd Content.Abort {write_ref} failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }

    /// Link an already-committed shared content blob (from `moby`) into
    /// `namespace` (`k8s.io`) with zero disk writes and zero payload bytes.
    ///
    /// In containerd's shared content store (`core/metadata/content.go`),
    /// passing `expected = digest` and `total = size` causes `Writer` to open a
    /// shared `namespacedWriter` (`nw.w == nil`, `ws.Offset == size`) without
    /// creating any ingest file. Sending `action = COMMIT`, `offset = size`,
    /// `data = &[]` matches `ws.Offset`, skips `Truncate`/`createAndCopy`, and
    /// commits the BoltDB reference directly.
    fn link_shared_content(
        &mut self,
        namespace: &str,
        lease: &str,
        write_ref: &str,
        digest: &str,
        size: i64,
    ) -> Result<WriteOutcome, String> {
        let req = encode_write_content_request(
            WRITE_ACTION_COMMIT,
            write_ref,
            size,
            digest,
            size,
            &[],
            &[],
        );
        let resp = self.unary_rpc(
            "/containerd.services.content.v1.Content/Write",
            namespace,
            Some(lease),
            &req,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        )?;
        match resp.grpc_status.as_str() {
            GRPC_OK => Ok(WriteOutcome::Committed),
            GRPC_ALREADY_EXISTS => Ok(WriteOutcome::AlreadyExists),
            code => Err(format!(
                "containerd Content.Write link {digest} failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }

    /// Write a metadata blob (`new_config` or `new_manifest`) into `namespace`
    /// over bidirectional-streaming `/containerd.services.content.v1.Content/Write`,
    /// chunking at [`CONTENT_WRITE_CHUNK`] bytes so multi-megabyte configs
    /// respect HTTP/2 flow control without deadlock.
    fn write_content_blob(
        &mut self,
        namespace: &str,
        lease: &str,
        write_ref: &str,
        expected_digest: &str,
        data: &[u8],
        labels: &[(String, String)],
    ) -> Result<WriteOutcome, String> {
        let total = i64::try_from(data.len()).map_err(|_| "blob size overflows i64".to_string())?;
        let op = "/containerd.services.content.v1.Content/Write";

        if data.len() <= CONTENT_WRITE_CHUNK {
            let req = encode_write_content_request(
                WRITE_ACTION_COMMIT,
                write_ref,
                total,
                expected_digest,
                0,
                data,
                labels,
            );
            let resp = self.unary_rpc(
                op,
                namespace,
                Some(lease),
                &req,
                DEFAULT_UNARY_RESPONSE_LIMIT,
            )?;
            return match resp.grpc_status.as_str() {
                GRPC_OK => Ok(WriteOutcome::Committed),
                GRPC_ALREADY_EXISTS => Ok(WriteOutcome::AlreadyExists),
                code => Err(format!(
                    "containerd Content.Write {expected_digest} failed (grpc-status {code}): {}",
                    resp.grpc_message
                )),
            };
        }

        let sid = self.alloc_stream_id();
        self.send_request_headers(sid, op, namespace, Some(lease))?;
        let mut state = StreamState::new(
            sid,
            self.peer_initial_stream_window,
            DEFAULT_UNARY_RESPONSE_LIMIT,
        );

        let mut offset = 0usize;
        while offset < data.len() && !state.ended {
            let end = (offset + CONTENT_WRITE_CHUNK).min(data.len());
            let is_last = end == data.len();
            let action = if is_last {
                WRITE_ACTION_COMMIT
            } else {
                WRITE_ACTION_WRITE
            };
            let req = encode_write_content_request(
                action,
                if offset == 0 { write_ref } else { "" },
                total,
                if offset == 0 { expected_digest } else { "" },
                offset as i64,
                &data[offset..end],
                if is_last { labels } else { &[] },
            );
            let framed = frame_grpc_message(&req)?;
            self.send_stream_data(&mut state, &framed, is_last, op)?;
            self.stream
                .flush()
                .map_err(|e| format!("flush h2 Content.Write chunk: {e}"))?;

            if !is_last {
                // Wait for the server's per-chunk WriteContentResponse (or
                // early stream termination such as ALREADY_EXISTS) so both
                // sides stay synchronized under HTTP/2 flow control.
                let target_msgs = state.messages.len() + 1;
                while state.messages.len() < target_msgs && !state.ended {
                    self.read_one_frame(&mut state, op)?;
                }
            }
            offset = end;
        }

        let resp = self.finish_stream(&mut state, op)?;
        match resp.grpc_status.as_str() {
            GRPC_OK => Ok(WriteOutcome::Committed),
            GRPC_ALREADY_EXISTS => Ok(WriteOutcome::AlreadyExists),
            code => Err(format!(
                "containerd Content.Write {expected_digest} failed (grpc-status {code}): {}",
                resp.grpc_message
            )),
        }
    }
}

struct StreamState {
    sid: u32,
    peer_window: i64,
    max_response_bytes: usize,
    total_response_bytes: usize,
    http_status: Option<String>,
    grpc_status: Option<String>,
    grpc_message: String,
    raw_data: Vec<u8>,
    messages: Vec<Vec<u8>>,
    ended: bool,
}

impl StreamState {
    fn new(sid: u32, peer_window: i64, max_response_bytes: usize) -> Self {
        Self {
            sid,
            peer_window,
            max_response_bytes,
            total_response_bytes: 0,
            http_status: None,
            grpc_status: None,
            grpc_message: String::new(),
            raw_data: Vec::new(),
            messages: Vec::new(),
            ended: false,
        }
    }

    fn push_data(&mut self, chunk: &[u8], op: &str) -> Result<(), String> {
        self.total_response_bytes = self.total_response_bytes.saturating_add(chunk.len());
        if self.total_response_bytes > self.max_response_bytes {
            return Err(format!(
                "containerd {op} response exceeded {}-byte limit",
                self.max_response_bytes
            ));
        }
        self.raw_data.extend_from_slice(chunk);
        loop {
            if self.raw_data.len() < 5 {
                break;
            }
            let compressed = self.raw_data[0];
            if compressed != 0 {
                return Err(format!(
                    "containerd {op} sent unexpected compressed gRPC message"
                ));
            }
            let msg_len = u32::from_be_bytes([
                self.raw_data[1],
                self.raw_data[2],
                self.raw_data[3],
                self.raw_data[4],
            ]) as usize;
            if msg_len > self.max_response_bytes {
                return Err(format!(
                    "containerd {op} message ({msg_len} bytes) exceeded {}-byte limit",
                    self.max_response_bytes
                ));
            }
            if self.raw_data.len() < 5 + msg_len {
                break;
            }
            let msg = self.raw_data[5..5 + msg_len].to_vec();
            self.raw_data.drain(..5 + msg_len);
            self.messages.push(msg);
        }
        Ok(())
    }
}

fn remaining_or_timeout(deadline: Instant, op: &str) -> Result<Duration, String> {
    let Some(left) = deadline.checked_duration_since(Instant::now()) else {
        return Err(format!("containerd {op} timed out"));
    };
    if left.is_zero() {
        return Err(format!("containerd {op} timed out"));
    }
    Ok(left)
}

fn format_grpc_timeout(timeout: Duration) -> String {
    let millis = timeout.as_millis();
    if millis < 10_000 {
        format!("{}m", millis.max(1))
    } else {
        format!("{}S", timeout.as_secs().max(1))
    }
}

fn strip_data_padding(payload: &[u8], flags: u8) -> Result<&[u8], String> {
    if flags & FLAG_PADDED == 0 {
        return Ok(payload);
    }
    let Some((&pad_len, rest)) = payload.split_first() else {
        return Err("truncated padded DATA frame".into());
    };
    let pad_len = pad_len as usize;
    if pad_len > rest.len() {
        return Err("invalid DATA padding length".into());
    }
    Ok(&rest[..rest.len() - pad_len])
}

/// Format `now + delta_secs` as UTC RFC 3339 (`YYYY-MM-DDTHH:MM:SSZ`) for
/// containerd's `containerd.io/gc.expire` lease label.
fn rfc3339_utc_after(delta_secs: u64) -> String {
    let unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_add(delta_secs);
    format_rfc3339_utc(unix_secs)
}

fn format_rfc3339_utc(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;

    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}Z")
}

// ---------------------------------------------------------------------------
// Proto3 encoders and decoders for containerd v2.4.2 gRPC services
// ---------------------------------------------------------------------------

const WRITE_ACTION_WRITE: u64 = 1;
const WRITE_ACTION_COMMIT: u64 = 2;

/// True when `digest` matches the OCI `<algorithm>:<encoded>` grammar
/// (`[a-z0-9+._-]+:[a-zA-Z0-9=_-]+`). Invalid digests cannot exist in
/// containerd's content store and are skipped locally without an RPC.
fn is_valid_oci_digest(digest: &str) -> bool {
    let Some((algo, encoded)) = digest.split_once(':') else {
        return false;
    };
    !algo.is_empty()
        && !encoded.is_empty()
        && algo.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'+' | b'.' | b'_' | b'-')
        })
        && encoded
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'=' | b'_' | b'-'))
}

/// Encode `containerd.services.images.v1.DeleteImageRequest`:
/// - field 1 (string): `name`
/// - field 3 (message `containerd.types.Descriptor`): `target`
///   - field 2 (string): `digest`
fn encode_delete_image_request(name: &str, target_digest: &str) -> Vec<u8> {
    let mut target = Vec::with_capacity(target_digest.len() + 4);
    encode_proto_bytes(&mut target, 2, target_digest.as_bytes());

    let mut req = Vec::with_capacity(name.len() + target.len() + 8);
    encode_proto_bytes(&mut req, 1, name.as_bytes());
    encode_proto_bytes(&mut req, 3, &target);
    req
}

fn encode_descriptor(desc: &Descriptor) -> Vec<u8> {
    let mut out = Vec::new();
    if !desc.media_type.is_empty() {
        encode_proto_bytes(&mut out, 1, desc.media_type.as_bytes());
    }
    if !desc.digest.is_empty() {
        encode_proto_bytes(&mut out, 2, desc.digest.as_bytes());
    }
    if desc.size != 0 {
        encode_proto_varint(&mut out, 3, desc.size as u64);
    }
    let mut annotations: Vec<(&String, &String)> = desc.annotations.iter().collect();
    annotations.sort_by(|a, b| a.0.cmp(b.0));
    for (k, v) in annotations {
        encode_proto_map_entry(&mut out, 5, k, v);
    }
    out
}

fn encode_image(img: &ContainerdImage) -> Vec<u8> {
    let mut out = Vec::new();
    if !img.name.is_empty() {
        encode_proto_bytes(&mut out, 1, img.name.as_bytes());
    }
    let mut labels: Vec<(&String, &String)> = img.labels.iter().collect();
    labels.sort_by(|a, b| a.0.cmp(b.0));
    for (k, v) in labels {
        encode_proto_map_entry(&mut out, 2, k, v);
    }
    let target = encode_descriptor(&img.target);
    if !target.is_empty() {
        encode_proto_bytes(&mut out, 3, &target);
    }
    out
}

fn encode_create_image_request(img: &ContainerdImage) -> Vec<u8> {
    let mut out = Vec::new();
    let encoded_img = encode_image(img);
    encode_proto_bytes(&mut out, 1, &encoded_img);
    out
}

/// `containerd.services.leases.v1.AddResourceRequest`:
/// - field 1 (string): `id`
/// - field 2 (message `Resource`): `id = 1`, `type = 2`
fn encode_add_lease_resource_request(
    lease_id: &str,
    resource_id: &str,
    resource_type: &str,
) -> Vec<u8> {
    let mut res = Vec::with_capacity(resource_id.len() + resource_type.len() + 8);
    if !resource_id.is_empty() {
        encode_proto_bytes(&mut res, 1, resource_id.as_bytes());
    }
    if !resource_type.is_empty() {
        encode_proto_bytes(&mut res, 2, resource_type.as_bytes());
    }
    let mut out = Vec::with_capacity(lease_id.len() + res.len() + 8);
    if !lease_id.is_empty() {
        encode_proto_bytes(&mut out, 1, lease_id.as_bytes());
    }
    encode_proto_bytes(&mut out, 2, &res);
    out
}

/// `containerd.services.leases.v1.CreateRequest`:
/// - field 1 (string): `id` (omitted so containerd generates a unique random ID)
/// - field 3 (map<string, string>): `labels`
fn encode_create_lease_request(labels: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    for &(k, v) in labels {
        encode_proto_map_entry(&mut out, 3, k, v);
    }
    out
}

/// `containerd.services.content.v1.UpdateRequest`:
/// - field 1 (message `Info`): `digest = 1`, `labels = 5`
/// - field 2 (message `FieldMask`): `paths = 1`
fn encode_update_content_labels_request(digest: &str, labels: &[(String, String)]) -> Vec<u8> {
    let mut info = Vec::new();
    encode_proto_bytes(&mut info, 1, digest.as_bytes());
    let mut mask = Vec::new();
    for (k, v) in labels {
        encode_proto_map_entry(&mut info, 5, k, v);
        let path = format!("labels.{k}");
        encode_proto_bytes(&mut mask, 1, path.as_bytes());
    }
    let mut out = Vec::new();
    encode_proto_bytes(&mut out, 1, &info);
    encode_proto_bytes(&mut out, 2, &mask);
    out
}

/// `containerd.services.content.v1.WriteContentRequest`:
/// - field 1 (enum): `action`
/// - field 2 (string): `ref`
/// - field 3 (int64): `total`
/// - field 4 (string): `expected`
/// - field 5 (int64): `offset`
/// - field 6 (bytes): `data`
/// - field 7 (map<string, string>): `labels`
fn encode_write_content_request(
    action: u64,
    write_ref: &str,
    total: i64,
    expected: &str,
    offset: i64,
    data: &[u8],
    labels: &[(String, String)],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + data.len());
    if action != 0 {
        encode_proto_varint(&mut out, 1, action);
    }
    if !write_ref.is_empty() {
        encode_proto_bytes(&mut out, 2, write_ref.as_bytes());
    }
    if total != 0 {
        encode_proto_varint(&mut out, 3, total as u64);
    }
    if !expected.is_empty() {
        encode_proto_bytes(&mut out, 4, expected.as_bytes());
    }
    if offset != 0 {
        encode_proto_varint(&mut out, 5, offset as u64);
    }
    if !data.is_empty() {
        encode_proto_bytes(&mut out, 6, data);
    }
    for (k, v) in labels {
        encode_proto_map_entry(&mut out, 7, k, v);
    }
    out
}

fn encode_proto_varint(out: &mut Vec<u8>, field_num: u32, val: u64) {
    encode_varint(out, (field_num as u64) << 3);
    encode_varint(out, val);
}

fn encode_proto_bytes(out: &mut Vec<u8>, field_num: u32, data: &[u8]) {
    encode_varint(out, ((field_num as u64) << 3) | 2);
    encode_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

fn encode_proto_map_entry(out: &mut Vec<u8>, field_num: u32, key: &str, val: &str) {
    let mut entry = Vec::with_capacity(key.len() + val.len() + 8);
    if !key.is_empty() {
        encode_proto_bytes(&mut entry, 1, key.as_bytes());
    }
    if !val.is_empty() {
        encode_proto_bytes(&mut entry, 2, val.as_bytes());
    }
    encode_proto_bytes(out, field_num, &entry);
}

fn encode_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn decode_varint(mut data: &[u8]) -> Result<(u64, &[u8]), String> {
    let mut val = 0u64;
    let mut shift = 0u32;
    loop {
        let Some((&b, rest)) = data.split_first() else {
            return Err("truncated protobuf varint".into());
        };
        data = rest;
        if shift >= 64 || (shift == 63 && (b & 0x7e) != 0) {
            return Err("protobuf varint overflow".into());
        }
        val |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok((val, data));
        }
        shift += 7;
    }
}

fn decode_len_delimited(data: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let (len, rest) = decode_varint(data)?;
    let len = usize::try_from(len).map_err(|_| "protobuf length overflow".to_string())?;
    if len > rest.len() {
        return Err("truncated protobuf length-delimited field".into());
    }
    Ok(rest.split_at(len))
}

fn skip_proto_field(wt: u64, data: &[u8]) -> Result<&[u8], String> {
    match wt {
        0 => Ok(decode_varint(data)?.1),
        1 => data
            .get(8..)
            .ok_or_else(|| "truncated protobuf 64-bit field".into()),
        2 => Ok(decode_len_delimited(data)?.1),
        5 => data
            .get(4..)
            .ok_or_else(|| "truncated protobuf 32-bit field".into()),
        other => Err(format!("unsupported protobuf wire type {other}")),
    }
}

fn decode_string_map_entry(mut data: &[u8]) -> Result<(String, String), String> {
    let mut key = String::new();
    let mut val = String::new();
    while !data.is_empty() {
        let (tag, rest) = decode_varint(data)?;
        let field = tag >> 3;
        let wt = tag & 0x7;
        match (field, wt) {
            (1, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                key = String::from_utf8_lossy(bytes).into_owned();
                data = next;
            }
            (2, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                val = String::from_utf8_lossy(bytes).into_owned();
                data = next;
            }
            _ => data = skip_proto_field(wt, rest)?,
        }
    }
    Ok((key, val))
}

fn decode_descriptor(mut data: &[u8]) -> Result<Descriptor, String> {
    let mut desc = Descriptor::default();
    while !data.is_empty() {
        let (tag, rest) = decode_varint(data)?;
        let field = tag >> 3;
        let wt = tag & 0x7;
        match (field, wt) {
            (1, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                desc.media_type = String::from_utf8_lossy(bytes).into_owned();
                data = next;
            }
            (2, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                desc.digest = String::from_utf8_lossy(bytes).into_owned();
                data = next;
            }
            (3, 0) => {
                let (v, next) = decode_varint(rest)?;
                desc.size = v as i64;
                data = next;
            }
            (5, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                let (k, v) = decode_string_map_entry(bytes)?;
                desc.annotations.insert(k, v);
                data = next;
            }
            _ => data = skip_proto_field(wt, rest)?,
        }
    }
    Ok(desc)
}

fn decode_image(mut data: &[u8]) -> Result<ContainerdImage, String> {
    let mut img = ContainerdImage::default();
    while !data.is_empty() {
        let (tag, rest) = decode_varint(data)?;
        let field = tag >> 3;
        let wt = tag & 0x7;
        match (field, wt) {
            (1, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                img.name = String::from_utf8_lossy(bytes).into_owned();
                data = next;
            }
            (2, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                let (k, v) = decode_string_map_entry(bytes)?;
                img.labels.insert(k, v);
                data = next;
            }
            (3, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                img.target = decode_descriptor(bytes)?;
                data = next;
            }
            _ => data = skip_proto_field(wt, rest)?,
        }
    }
    Ok(img)
}

fn decode_list_images_response(mut data: &[u8]) -> Result<Vec<ContainerdImage>, String> {
    let mut images = Vec::new();
    while !data.is_empty() {
        let (tag, rest) = decode_varint(data)?;
        let field = tag >> 3;
        let wt = tag & 0x7;
        match (field, wt) {
            (1, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                images.push(decode_image(bytes)?);
                data = next;
            }
            _ => data = skip_proto_field(wt, rest)?,
        }
    }
    Ok(images)
}

fn decode_get_image_response(mut data: &[u8]) -> Result<Option<ContainerdImage>, String> {
    let mut image = None;
    while !data.is_empty() {
        let (tag, rest) = decode_varint(data)?;
        let field = tag >> 3;
        let wt = tag & 0x7;
        match (field, wt) {
            (1, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                image = Some(decode_image(bytes)?);
                data = next;
            }
            _ => data = skip_proto_field(wt, rest)?,
        }
    }
    Ok(image)
}

fn decode_content_info(mut data: &[u8]) -> Result<ContentInfo, String> {
    let mut info = ContentInfo::default();
    while !data.is_empty() {
        let (tag, rest) = decode_varint(data)?;
        let field = tag >> 3;
        let wt = tag & 0x7;
        match (field, wt) {
            (1, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                info.digest = String::from_utf8_lossy(bytes).into_owned();
                data = next;
            }
            (2, 0) => {
                let (v, next) = decode_varint(rest)?;
                info.size = v as i64;
                data = next;
            }
            _ => data = skip_proto_field(wt, rest)?,
        }
    }
    Ok(info)
}

fn decode_content_info_response(mut data: &[u8]) -> Result<Option<ContentInfo>, String> {
    let mut info = None;
    while !data.is_empty() {
        let (tag, rest) = decode_varint(data)?;
        let field = tag >> 3;
        let wt = tag & 0x7;
        match (field, wt) {
            (1, 2) => {
                let (bytes, next) = decode_len_delimited(rest)?;
                info = Some(decode_content_info(bytes)?);
                data = next;
            }
            _ => data = skip_proto_field(wt, rest)?,
        }
    }
    Ok(info)
}

/// Decode `containerd.services.leases.v1.CreateResponse`:
/// - field 1 (message `Lease`):
///   - field 1 (string): `id`
fn decode_create_lease_response(mut data: &[u8]) -> Result<String, String> {
    let mut lease_id = String::new();
    while !data.is_empty() {
        let (tag, rest) = decode_varint(data)?;
        let field = tag >> 3;
        let wt = tag & 0x7;
        match (field, wt) {
            (1, 2) => {
                let (mut lease, next) = decode_len_delimited(rest)?;
                while !lease.is_empty() {
                    let (lt, lr) = decode_varint(lease)?;
                    match (lt >> 3, lt & 0x7) {
                        (1, 2) => {
                            let (bytes, ln) = decode_len_delimited(lr)?;
                            lease_id = String::from_utf8_lossy(bytes).into_owned();
                            lease = ln;
                        }
                        _ => lease = skip_proto_field(lt & 0x7, lr)?,
                    }
                }
                data = next;
            }
            _ => data = skip_proto_field(wt, rest)?,
        }
    }
    Ok(lease_id)
}

// ---------------------------------------------------------------------------
// HTTP/2 framing & RFC 7541 HPACK
// ---------------------------------------------------------------------------

fn frame_grpc_message(payload: &[u8]) -> Result<Vec<u8>, String> {
    let len = u32::try_from(payload.len()).map_err(|_| "gRPC message too large".to_string())?;
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0); // uncompressed
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

fn write_frame(
    w: &mut impl Write,
    frame_type: u8,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> io::Result<()> {
    let len = payload.len();
    let hdr = [
        ((len >> 16) & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        (len & 0xff) as u8,
        frame_type,
        flags,
        ((stream_id >> 24) & 0x7f) as u8,
        ((stream_id >> 16) & 0xff) as u8,
        ((stream_id >> 8) & 0xff) as u8,
        (stream_id & 0xff) as u8,
    ];
    w.write_all(&hdr)?;
    if !payload.is_empty() {
        w.write_all(payload)?;
    }
    Ok(())
}

fn read_frame(r: &mut impl Read) -> Result<(u8, u8, u32, Vec<u8>), String> {
    let mut hdr = [0u8; 9];
    r.read_exact(&mut hdr)
        .map_err(|e| format!("read h2 frame header: {e}"))?;
    let len = ((hdr[0] as usize) << 16) | ((hdr[1] as usize) << 8) | (hdr[2] as usize);
    if len > MAX_SERVER_FRAME {
        return Err(format!("h2 frame length {len} exceeds limit"));
    }
    let frame_type = hdr[3];
    let flags = hdr[4];
    let stream_id = u32::from_be_bytes([hdr[5] & 0x7f, hdr[6], hdr[7], hdr[8]]);
    let mut payload = vec![0u8; len];
    if len > 0 {
        r.read_exact(&mut payload)
            .map_err(|e| format!("read h2 frame payload: {e}"))?;
    }
    Ok((frame_type, flags, stream_id, payload))
}

fn strip_headers_framing(payload: &[u8], flags: u8) -> Result<Vec<u8>, String> {
    let mut slice = payload;
    let pad_len = if flags & FLAG_PADDED != 0 {
        let Some((&p, rest)) = slice.split_first() else {
            return Err("truncated padded HEADERS frame".into());
        };
        slice = rest;
        p as usize
    } else {
        0
    };
    if flags & FLAG_PRIORITY != 0 {
        if slice.len() < 5 {
            return Err("truncated priority HEADERS frame".into());
        }
        slice = &slice[5..];
    }
    if pad_len > slice.len() {
        return Err("invalid HEADERS padding length".into());
    }
    Ok(slice[..slice.len() - pad_len].to_vec())
}

/// Encode headers using RFC 7541 §6.2.2 literal header fields without indexing.
fn encode_request_headers(headers: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    for &(name, value) in headers {
        out.push(0x00);
        encode_hpack_str(&mut out, name.as_bytes());
        encode_hpack_str(&mut out, value.as_bytes());
    }
    out
}

fn encode_hpack_str(out: &mut Vec<u8>, s: &[u8]) {
    encode_hpack_int(out, 0x00, 7, s.len());
    out.extend_from_slice(s);
}

fn encode_hpack_int(out: &mut Vec<u8>, prefix_bits: u8, n: u8, mut val: usize) {
    let max_prefix = (1usize << n) - 1;
    if val < max_prefix {
        out.push(prefix_bits | (val as u8));
        return;
    }
    out.push(prefix_bits | (max_prefix as u8));
    val -= max_prefix;
    while val >= 128 {
        out.push(((val & 0x7f) as u8) | 0x80);
        val >>= 7;
    }
    out.push(val as u8);
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// RFC 7541 HPACK decoder supporting static table, dynamic table with eviction,
/// table size updates, and Appendix B Huffman decoding.
struct HpackDecoder {
    dynamic: VecDeque<(String, String)>,
    dynamic_bytes: usize,
    max_size: usize,
}

impl HpackDecoder {
    fn new(max_size: usize) -> Self {
        Self {
            dynamic: VecDeque::new(),
            dynamic_bytes: 0,
            max_size,
        }
    }

    fn decode(&mut self, mut input: &[u8]) -> Result<Vec<(String, String)>, String> {
        let mut out = Vec::new();
        while !input.is_empty() {
            let b = input[0];
            if b & 0x80 != 0 {
                // Indexed Header Field (§6.1)
                let (idx, rest) = decode_hpack_int(input, 7)?;
                input = rest;
                out.push(self.lookup(idx)?);
            } else if b & 0x40 != 0 {
                // Literal Header Field with Incremental Indexing (§6.2.1)
                let (name, val, rest) = self.decode_literal(input, 6)?;
                input = rest;
                self.insert(name.clone(), val.clone());
                out.push((name, val));
            } else if b & 0x20 != 0 {
                // Dynamic Table Size Update (§6.3)
                let (new_max, rest) = decode_hpack_int(input, 5)?;
                input = rest;
                self.max_size = new_max;
                self.evict();
            } else {
                // Literal without Indexing (§6.2.2) or Never Indexed (§6.2.3)
                let (name, val, rest) = self.decode_literal(input, 4)?;
                input = rest;
                out.push((name, val));
            }
        }
        Ok(out)
    }

    fn decode_literal<'a>(
        &self,
        input: &'a [u8],
        prefix_bits: u8,
    ) -> Result<(String, String, &'a [u8]), String> {
        let (name_idx, rest) = decode_hpack_int(input, prefix_bits)?;
        let (name, rest) = if name_idx == 0 {
            decode_hpack_str(rest)?
        } else {
            (self.lookup(name_idx)?.0, rest)
        };
        let (val, rest) = decode_hpack_str(rest)?;
        Ok((name, val, rest))
    }

    fn lookup(&self, idx: usize) -> Result<(String, String), String> {
        if idx == 0 {
            return Err("invalid HPACK index 0".into());
        }
        if idx <= STATIC_TABLE.len() {
            let (k, v) = STATIC_TABLE[idx - 1];
            return Ok((k.to_string(), v.to_string()));
        }
        let dyn_idx = idx - STATIC_TABLE.len() - 1;
        self.dynamic
            .get(dyn_idx)
            .cloned()
            .ok_or_else(|| format!("HPACK dynamic table index {idx} out of bounds"))
    }

    fn insert(&mut self, name: String, value: String) {
        let entry_size = name.len() + value.len() + 32;
        if entry_size > self.max_size {
            self.dynamic.clear();
            self.dynamic_bytes = 0;
            return;
        }
        self.dynamic.push_front((name, value));
        self.dynamic_bytes += entry_size;
        self.evict();
    }

    fn evict(&mut self) {
        while self.dynamic_bytes > self.max_size {
            if let Some((k, v)) = self.dynamic.pop_back() {
                self.dynamic_bytes = self.dynamic_bytes.saturating_sub(k.len() + v.len() + 32);
            } else {
                self.dynamic_bytes = 0;
                break;
            }
        }
    }
}

fn decode_hpack_int(input: &[u8], n: u8) -> Result<(usize, &[u8]), String> {
    let Some((&first, mut rest)) = input.split_first() else {
        return Err("truncated HPACK integer".into());
    };
    let mask = ((1u16 << n) - 1) as u8;
    let mut val = (first & mask) as usize;
    if val < mask as usize {
        return Ok((val, rest));
    }
    let mut shift = 0u32;
    loop {
        let Some((&b, next)) = rest.split_first() else {
            return Err("truncated HPACK multi-byte integer".into());
        };
        rest = next;
        let add = ((b & 0x7f) as usize)
            .checked_shl(shift)
            .ok_or("HPACK integer overflow")?;
        val = val.checked_add(add).ok_or("HPACK integer overflow")?;
        if b & 0x80 == 0 {
            return Ok((val, rest));
        }
        shift += 7;
        if shift > 28 {
            return Err("HPACK integer overflow".into());
        }
    }
}

fn decode_hpack_str(input: &[u8]) -> Result<(String, &[u8]), String> {
    let Some(&first) = input.first() else {
        return Err("truncated HPACK string".into());
    };
    let huffman = first & 0x80 != 0;
    let (len, rest) = decode_hpack_int(input, 7)?;
    if len > rest.len() {
        return Err("truncated HPACK string payload".into());
    }
    let (raw, tail) = rest.split_at(len);
    let bytes = if huffman {
        huffman_decode(raw)?
    } else {
        raw.to_vec()
    };
    Ok((String::from_utf8_lossy(&bytes).into_owned(), tail))
}

fn huffman_decode(src: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(src.len() * 2);
    let mut cur: u64 = 0;
    let mut nbits: u8 = 0;

    for &byte in src {
        cur = (cur << 8) | (byte as u64);
        nbits += 8;
        while nbits >= 5 {
            let mut matched = None;
            for (sym, (&code, &len)) in HUFFMAN_CODES
                .iter()
                .zip(HUFFMAN_CODE_LEN.iter())
                .enumerate()
            {
                if len <= nbits && ((cur >> (nbits - len)) as u32) == code {
                    matched = Some((sym as u8, len));
                    break;
                }
            }
            if let Some((sym, len)) = matched {
                out.push(sym);
                nbits -= len;
                cur &= (1u64 << nbits) - 1;
            } else {
                break;
            }
        }
        if nbits > 30 {
            return Err("invalid HPACK Huffman code".into());
        }
    }

    if nbits > 7 {
        return Err("invalid HPACK Huffman trailing bits".into());
    }
    if nbits > 0 {
        let pad_mask = (1u64 << nbits) - 1;
        if cur != pad_mask {
            return Err("invalid HPACK Huffman padding".into());
        }
    }
    Ok(out)
}

/// RFC 7541 Appendix A static table (1-based indices 1..=61).
const STATIC_TABLE: [(&str, &str); 61] = [
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

/// RFC 7541 Appendix B Huffman codes for bytes 0..=255.
#[rustfmt::skip]
const HUFFMAN_CODES: [u32; 256] = [
    0x1ff8, 0x7fffd8, 0xfffffe2, 0xfffffe3, 0xfffffe4, 0xfffffe5, 0xfffffe6, 0xfffffe7,
    0xfffffe8, 0xffffea, 0x3ffffffc, 0xfffffe9, 0xfffffea, 0x3ffffffd, 0xfffffeb, 0xfffffec,
    0xfffffed, 0xfffffee, 0xfffffef, 0xffffff0, 0xffffff1, 0xffffff2, 0x3ffffffe, 0xffffff3,
    0xffffff4, 0xffffff5, 0xffffff6, 0xffffff7, 0xffffff8, 0xffffff9, 0xffffffa, 0xffffffb,
    0x14, 0x3f8, 0x3f9, 0xffa, 0x1ff9, 0x15, 0xf8, 0x7fa,
    0x3fa, 0x3fb, 0xf9, 0x7fb, 0xfa, 0x16, 0x17, 0x18,
    0x0, 0x1, 0x2, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
    0x1e, 0x1f, 0x5c, 0xfb, 0x7ffc, 0x20, 0xffb, 0x3fc,
    0x1ffa, 0x21, 0x5d, 0x5e, 0x5f, 0x60, 0x61, 0x62,
    0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a,
    0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72,
    0xfc, 0x73, 0xfd, 0x1ffb, 0x7fff0, 0x1ffc, 0x3ffc, 0x22,
    0x7ffd, 0x3, 0x23, 0x4, 0x24, 0x5, 0x25, 0x26,
    0x27, 0x6, 0x74, 0x75, 0x28, 0x29, 0x2a, 0x7,
    0x2b, 0x76, 0x2c, 0x8, 0x9, 0x2d, 0x77, 0x78,
    0x79, 0x7a, 0x7b, 0x7ffe, 0x7fc, 0x3ffd, 0x1ffd, 0xffffffc,
    0xfffe6, 0x3fffd2, 0xfffe7, 0xfffe8, 0x3fffd3, 0x3fffd4, 0x3fffd5, 0x7fffd9,
    0x3fffd6, 0x7fffda, 0x7fffdb, 0x7fffdc, 0x7fffdd, 0x7fffde, 0xffffeb, 0x7fffdf,
    0xffffec, 0xffffed, 0x3fffd7, 0x7fffe0, 0xffffee, 0x7fffe1, 0x7fffe2, 0x7fffe3,
    0x7fffe4, 0x1fffdc, 0x3fffd8, 0x7fffe5, 0x3fffd9, 0x7fffe6, 0x7fffe7, 0xffffef,
    0x3fffda, 0x1fffdd, 0xfffe9, 0x3fffdb, 0x3fffdc, 0x7fffe8, 0x7fffe9, 0x1fffde,
    0x7fffea, 0x3fffdd, 0x3fffde, 0xfffff0, 0x1fffdf, 0x3fffdf, 0x7fffeb, 0x7fffec,
    0x1fffe0, 0x1fffe1, 0x3fffe0, 0x1fffe2, 0x7fffed, 0x3fffe1, 0x7fffee, 0x7fffef,
    0xfffea, 0x3fffe2, 0x3fffe3, 0x3fffe4, 0x7ffff0, 0x3fffe5, 0x3fffe6, 0x7ffff1,
    0x3ffffe0, 0x3ffffe1, 0xfffeb, 0x7fff1, 0x3fffe7, 0x7ffff2, 0x3fffe8, 0x1ffffec,
    0x3ffffe2, 0x3ffffe3, 0x3ffffe4, 0x7ffffde, 0x7ffffdf, 0x3ffffe5, 0xfffff1, 0x1ffffed,
    0x7fff2, 0x1fffe3, 0x3ffffe6, 0x7ffffe0, 0x7ffffe1, 0x3ffffe7, 0x7ffffe2, 0xfffff2,
    0x1fffe4, 0x1fffe5, 0x3ffffe8, 0x3ffffe9, 0xffffffd, 0x7ffffe3, 0x7ffffe4, 0x7ffffe5,
    0xfffec, 0xfffff3, 0xfffed, 0x1fffe6, 0x3fffe9, 0x1fffe7, 0x1fffe8, 0x7ffff3,
    0x3fffea, 0x3fffeb, 0x1ffffee, 0x1ffffef, 0xfffff4, 0xfffff5, 0x3ffffea, 0x7ffff4,
    0x3ffffeb, 0x7ffffe6, 0x3ffffec, 0x3ffffed, 0x7ffffe7, 0x7ffffe8, 0x7ffffe9, 0x7ffffea,
    0x7ffffeb, 0xffffffe, 0x7ffffec, 0x7ffffed, 0x7ffffee, 0x7ffffef, 0x7fffff0, 0x3ffffee,
];

/// RFC 7541 Appendix B Huffman code lengths (in bits) for bytes 0..=255.
#[rustfmt::skip]
const HUFFMAN_CODE_LEN: [u8; 256] = [
    13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28,
    28, 28, 28, 28, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 28,
     6, 10, 10, 12, 13,  6,  8, 11, 10, 10,  8, 11,  8,  6,  6,  6,
     5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  7,  8, 15,  6, 12, 10,
    13,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
     7,  7,  7,  7,  7,  7,  7,  7,  8,  7,  8, 13, 19, 13, 14,  6,
    15,  5,  6,  5,  6,  5,  6,  6,  6,  5,  7,  7,  6,  6,  6,  5,
     6,  7,  6,  5,  5,  6,  7,  7,  7,  7,  7, 15, 11, 14, 13, 28,
    20, 22, 20, 20, 22, 22, 22, 23, 22, 23, 23, 23, 23, 23, 24, 23,
    24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23, 22, 23, 23, 24,
    22, 21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23,
    21, 21, 22, 21, 23, 22, 23, 23, 20, 22, 22, 22, 23, 22, 22, 23,
    26, 26, 20, 19, 22, 23, 22, 25, 26, 26, 26, 27, 27, 26, 24, 25,
    19, 21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27,
    20, 24, 20, 21, 22, 21, 21, 23, 22, 22, 25, 25, 24, 24, 26, 23,
    26, 27, 26, 26, 27, 27, 27, 27, 27, 28, 27, 27, 27, 27, 27, 26,
];

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;

    static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_sock(name: &str) -> (PathBuf, PathBuf) {
        let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("kat-grpc-test-{name}-{}-{seq}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("containerd.sock");
        (dir, sock)
    }

    fn sha256_hex(data: &[u8]) -> String {
        Sha256::digest(data)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn huffman_encode(s: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut cur: u64 = 0;
        let mut nbits: u8 = 0;
        for &b in s {
            let code = HUFFMAN_CODES[b as usize] as u64;
            let len = HUFFMAN_CODE_LEN[b as usize];
            cur = (cur << len) | code;
            nbits += len;
            while nbits >= 8 {
                nbits -= 8;
                out.push((cur >> nbits) as u8);
                cur &= (1u64 << nbits) - 1;
            }
        }
        if nbits > 0 {
            let pad = 8 - nbits;
            out.push(((cur << pad) | ((1u64 << pad) - 1)) as u8);
        }
        out
    }

    fn encode_hpack_huffman_header(out: &mut Vec<u8>, name: &str, value: &str, index: bool) {
        out.push(if index { 0x40 } else { 0x00 });
        let n = huffman_encode(name.as_bytes());
        encode_hpack_int(out, 0x80, 7, n.len());
        out.extend_from_slice(&n);
        let v = huffman_encode(value.as_bytes());
        encode_hpack_int(out, 0x80, 7, v.len());
        out.extend_from_slice(&v);
    }

    fn send_grpc_ok(conn: &mut UnixStream, sid: u32, resp_proto: &[u8]) {
        let mut hdr = vec![0x88]; // :status 200
        encode_hpack_huffman_header(&mut hdr, "content-type", "application/grpc", false);
        write_frame(conn, FRAME_HEADERS, FLAG_END_HEADERS, sid, &hdr).unwrap();
        let framed = frame_grpc_message(resp_proto).unwrap();
        for chunk in framed.chunks(DEFAULT_MAX_FRAME_SIZE) {
            write_frame(conn, FRAME_DATA, 0, sid, chunk).unwrap();
        }
        let mut trl = Vec::new();
        encode_hpack_huffman_header(&mut trl, "grpc-status", "0", false);
        write_frame(
            conn,
            FRAME_HEADERS,
            FLAG_END_HEADERS | FLAG_END_STREAM,
            sid,
            &trl,
        )
        .unwrap();
        conn.flush().unwrap();
    }

    fn send_grpc_err(conn: &mut UnixStream, sid: u32, status: &str, msg: &str) {
        let mut trl = vec![0x88]; // :status 200
        encode_hpack_huffman_header(&mut trl, "content-type", "application/grpc", false);
        encode_hpack_huffman_header(&mut trl, "grpc-status", status, false);
        encode_hpack_huffman_header(&mut trl, "grpc-message", msg, false);
        write_frame(
            conn,
            FRAME_HEADERS,
            FLAG_END_HEADERS | FLAG_END_STREAM,
            sid,
            &trl,
        )
        .unwrap();
        conn.flush().unwrap();
    }

    fn decode_delete_request(mut data: &[u8]) -> (String, Option<String>) {
        let mut name = String::new();
        let mut target_digest = None;
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data).unwrap();
            let field = tag >> 3;
            let (val, next) = decode_len_delimited(rest).unwrap();
            data = next;
            match field {
                1 => name = String::from_utf8(val.to_vec()).unwrap(),
                3 => {
                    let desc = decode_descriptor(val).unwrap();
                    if !desc.digest.is_empty() {
                        target_digest = Some(desc.digest);
                    }
                }
                _ => {}
            }
        }
        (name, target_digest)
    }

    #[derive(Default)]
    struct DecodedWriteReq {
        action: u64,
        write_ref: String,
        total: i64,
        expected: String,
        offset: i64,
        data: Vec<u8>,
        labels: HashMap<String, String>,
    }

    fn decode_write_content_request(mut data: &[u8]) -> DecodedWriteReq {
        let mut req = DecodedWriteReq::default();
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data).unwrap();
            let field = tag >> 3;
            let wt = tag & 0x7;
            match (field, wt) {
                (1, 0) => {
                    let (v, next) = decode_varint(rest).unwrap();
                    req.action = v;
                    data = next;
                }
                (2, 2) => {
                    let (v, next) = decode_len_delimited(rest).unwrap();
                    req.write_ref = String::from_utf8(v.to_vec()).unwrap();
                    data = next;
                }
                (3, 0) => {
                    let (v, next) = decode_varint(rest).unwrap();
                    req.total = v as i64;
                    data = next;
                }
                (4, 2) => {
                    let (v, next) = decode_len_delimited(rest).unwrap();
                    req.expected = String::from_utf8(v.to_vec()).unwrap();
                    data = next;
                }
                (5, 0) => {
                    let (v, next) = decode_varint(rest).unwrap();
                    req.offset = v as i64;
                    data = next;
                }
                (6, 2) => {
                    let (v, next) = decode_len_delimited(rest).unwrap();
                    req.data = v.to_vec();
                    data = next;
                }
                (7, 2) => {
                    let (v, next) = decode_len_delimited(rest).unwrap();
                    let (k, val) = decode_string_map_entry(v).unwrap();
                    req.labels.insert(k, val);
                    data = next;
                }
                _ => data = skip_proto_field(wt, rest).unwrap(),
            }
        }
        req
    }

    fn decode_first_string(mut data: &[u8]) -> String {
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data).unwrap();
            let field = tag >> 3;
            let wt = tag & 0x7;
            if field == 1 && wt == 2 {
                let (v, _) = decode_len_delimited(rest).unwrap();
                return String::from_utf8(v.to_vec()).unwrap();
            }
            data = skip_proto_field(wt, rest).unwrap();
        }
        String::new()
    }

    fn decode_repeated_strings(mut data: &[u8]) -> Vec<String> {
        let mut out = Vec::new();
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data).unwrap();
            let field = tag >> 3;
            let wt = tag & 0x7;
            if field == 1 && wt == 2 {
                let (v, next) = decode_len_delimited(rest).unwrap();
                out.push(String::from_utf8(v.to_vec()).unwrap());
                data = next;
            } else {
                data = skip_proto_field(wt, rest).unwrap();
            }
        }
        out
    }

    fn decode_add_lease_resource_req(mut data: &[u8]) -> (String, String, String) {
        let mut lease_id = String::new();
        let mut res_id = String::new();
        let mut res_type = String::new();
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data).unwrap();
            let field = tag >> 3;
            let wt = tag & 0x7;
            match (field, wt) {
                (1, 2) => {
                    let (v, next) = decode_len_delimited(rest).unwrap();
                    lease_id = String::from_utf8(v.to_vec()).unwrap();
                    data = next;
                }
                (2, 2) => {
                    let (mut res, next) = decode_len_delimited(rest).unwrap();
                    while !res.is_empty() {
                        let (rt, rr) = decode_varint(res).unwrap();
                        match (rt >> 3, rt & 0x7) {
                            (1, 2) => {
                                let (v, n) = decode_len_delimited(rr).unwrap();
                                res_id = String::from_utf8(v.to_vec()).unwrap();
                                res = n;
                            }
                            (2, 2) => {
                                let (v, n) = decode_len_delimited(rr).unwrap();
                                res_type = String::from_utf8(v.to_vec()).unwrap();
                                res = n;
                            }
                            _ => res = skip_proto_field(rt & 0x7, rr).unwrap(),
                        }
                    }
                    data = next;
                }
                _ => data = skip_proto_field(wt, rest).unwrap(),
            }
        }
        (lease_id, res_id, res_type)
    }

    fn decode_create_lease_req(mut data: &[u8]) -> (String, HashMap<String, String>) {
        let mut id = String::new();
        let mut labels = HashMap::new();
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data).unwrap();
            let field = tag >> 3;
            let wt = tag & 0x7;
            match (field, wt) {
                (1, 2) => {
                    let (v, next) = decode_len_delimited(rest).unwrap();
                    id = String::from_utf8(v.to_vec()).unwrap();
                    data = next;
                }
                (3, 2) => {
                    let (v, next) = decode_len_delimited(rest).unwrap();
                    let (k, val) = decode_string_map_entry(v).unwrap();
                    labels.insert(k, val);
                    data = next;
                }
                _ => data = skip_proto_field(wt, rest).unwrap(),
            }
        }
        (id, labels)
    }

    fn decode_image_and_mask(mut data: &[u8]) -> (ContainerdImage, Vec<String>) {
        let mut img = ContainerdImage::default();
        let mut mask = Vec::new();
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data).unwrap();
            let field = tag >> 3;
            let wt = tag & 0x7;
            match (field, wt) {
                (1, 2) => {
                    let (v, next) = decode_len_delimited(rest).unwrap();
                    img = decode_image(v).unwrap();
                    data = next;
                }
                (2, 2) => {
                    let (mut m, next) = decode_len_delimited(rest).unwrap();
                    while !m.is_empty() {
                        let (mt, mr) = decode_varint(m).unwrap();
                        if (mt >> 3) == 1 && (mt & 0x7) == 2 {
                            let (p, mn) = decode_len_delimited(mr).unwrap();
                            mask.push(String::from_utf8(p.to_vec()).unwrap());
                            m = mn;
                        } else {
                            m = skip_proto_field(mt & 0x7, mr).unwrap();
                        }
                    }
                    data = next;
                }
                _ => data = skip_proto_field(wt, rest).unwrap(),
            }
        }
        (img, mask)
    }

    fn decode_content_update_req(
        mut data: &[u8],
    ) -> (String, HashMap<String, String>, Vec<String>) {
        let mut digest = String::new();
        let mut labels = HashMap::new();
        let mut mask = Vec::new();
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data).unwrap();
            let field = tag >> 3;
            let wt = tag & 0x7;
            match (field, wt) {
                (1, 2) => {
                    let (mut info, next) = decode_len_delimited(rest).unwrap();
                    while !info.is_empty() {
                        let (it, ir) = decode_varint(info).unwrap();
                        match (it >> 3, it & 0x7) {
                            (1, 2) => {
                                let (v, n) = decode_len_delimited(ir).unwrap();
                                digest = String::from_utf8(v.to_vec()).unwrap();
                                info = n;
                            }
                            (5, 2) => {
                                let (v, n) = decode_len_delimited(ir).unwrap();
                                let (k, val) = decode_string_map_entry(v).unwrap();
                                labels.insert(k, val);
                                info = n;
                            }
                            _ => info = skip_proto_field(it & 0x7, ir).unwrap(),
                        }
                    }
                    data = next;
                }
                (2, 2) => {
                    let (mut m, next) = decode_len_delimited(rest).unwrap();
                    while !m.is_empty() {
                        let (mt, mr) = decode_varint(m).unwrap();
                        if (mt >> 3) == 1 && (mt & 0x7) == 2 {
                            let (p, mn) = decode_len_delimited(mr).unwrap();
                            mask.push(String::from_utf8(p.to_vec()).unwrap());
                            m = mn;
                        } else {
                            m = skip_proto_field(mt & 0x7, mr).unwrap();
                        }
                    }
                    data = next;
                }
                _ => data = skip_proto_field(wt, rest).unwrap(),
            }
        }
        (digest, labels, mask)
    }

    #[test]
    fn formats_rfc3339_utc_dates() {
        assert_eq!(format_rfc3339_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_rfc3339_utc(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(format_rfc3339_utc(1_795_651_205), "2026-11-26T00:00:05Z");
        assert_eq!(format_rfc3339_utc(1_795_737_605), "2026-11-27T00:00:05Z");
    }

    #[test]
    fn delete_image_times_out_when_server_stalls() {
        let (dir, sock) = temp_sock("stall");
        let listener = UnixListener::bind(&sock).unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let server = thread::spawn(move || {
            let (_conn, _) = listener.accept().unwrap();
            let _ = done_rx.recv_timeout(Duration::from_secs(5));
        });

        let start = Instant::now();
        let err = delete_image_if_target_matches(
            &sock,
            "k8s.io",
            "docker.io/library/app:dev",
            "sha256:abc",
            Duration::from_millis(80),
        )
        .unwrap_err();
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "stalled server exceeded timeout: {:?}",
            start.elapsed()
        );
        assert!(
            err.contains("timed out")
                || err.contains("Resource temporarily unavailable")
                || err.contains("WouldBlock"),
            "{err}"
        );

        let _ = done_tx.send(());
        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn encodes_delete_image_request_with_target_digest() {
        let bytes = encode_delete_image_request("docker.io/library/app:dev", "sha256:abc123");
        let (name, digest) = decode_delete_request(&bytes);
        assert_eq!(name, "docker.io/library/app:dev");
        assert_eq!(digest.as_deref(), Some("sha256:abc123"));
    }

    #[test]
    fn hpack_decodes_rfc7541_huffman_and_dynamic_table() {
        let rfc_vec = [
            0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
        ];
        assert_eq!(huffman_decode(&rfc_vec).unwrap(), b"www.example.com");

        let mut block = Vec::new();
        block.push(0x88); // static index 8 -> :status: 200
        encode_hpack_huffman_header(&mut block, "grpc-status", "5", true);
        encode_hpack_huffman_header(&mut block, "grpc-message", "image%20not%20found", false);
        block.push(0xbe); // dynamic index 62 -> grpc-status: 5

        let mut dec = HpackDecoder::new(4096);
        let hdrs = dec.decode(&block).unwrap();
        assert_eq!(hdrs[0], (":status".into(), "200".into()));
        assert_eq!(hdrs[1], ("grpc-status".into(), "5".into()));
        assert_eq!(
            hdrs[2],
            ("grpc-message".into(), "image%20not%20found".into())
        );
        assert_eq!(hdrs[3], ("grpc-status".into(), "5".into()));
    }

    /// Regression test: if kubelet replaces a rewritten tag with a native pull
    /// between `list_ctr_images` and `retire_rewritten_image`, containerd's
    /// conditional `Images.Delete` RPC (`DeleteImageRequest.target`) returns
    /// NOT_FOUND and leaves the newly pulled native image intact.
    #[test]
    fn conditional_delete_preserves_native_replacement_on_digest_mismatch() {
        let (dir, sock) = temp_sock("delete");
        let listener = UnixListener::bind(&sock).unwrap();

        let store: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::from([
            (
                "docker.io/library/retire-me:dev".into(),
                "sha256:rewritten1".into(),
            ),
            (
                "docker.io/library/replaced:dev".into(),
                "sha256:native2".into(),
            ),
        ])));

        let server_store = Arc::clone(&store);
        let server = thread::spawn(move || {
            for _ in 0..3 {
                let (mut conn, _) = listener.accept().unwrap();
                let mut preface = [0u8; 24];
                conn.read_exact(&mut preface).unwrap();
                assert_eq!(&preface, CLIENT_PREFACE);

                write_frame(&mut conn, FRAME_SETTINGS, 0, 0, &[]).unwrap();
                conn.flush().unwrap();

                let mut req_headers_map = HashMap::new();
                let mut req_body = Vec::new();
                let mut dec = HpackDecoder::new(4096);
                loop {
                    let (ft, flags, sid, payload) = read_frame(&mut conn).unwrap();
                    match ft {
                        FRAME_SETTINGS => {
                            if flags & FLAG_ACK == 0 {
                                write_frame(&mut conn, FRAME_SETTINGS, FLAG_ACK, 0, &[]).unwrap();
                                conn.flush().unwrap();
                            }
                        }
                        FRAME_HEADERS if sid == 1 => {
                            for (k, v) in dec.decode(&payload).unwrap() {
                                req_headers_map.insert(k, v);
                            }
                        }
                        FRAME_DATA if sid == 1 => {
                            req_body.extend_from_slice(&payload);
                            if flags & FLAG_END_STREAM != 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                assert_eq!(
                    req_headers_map.get(":path").map(String::as_str),
                    Some("/containerd.services.images.v1.Images/Delete")
                );
                assert_eq!(
                    req_headers_map
                        .get("containerd-namespace")
                        .map(String::as_str),
                    Some("k8s.io")
                );
                let msg_len =
                    u32::from_be_bytes([req_body[1], req_body[2], req_body[3], req_body[4]])
                        as usize;
                let (name, target_digest) = decode_delete_request(&req_body[5..5 + msg_len]);

                if name == "docker.io/library/fail-me:dev" {
                    send_grpc_err(&mut conn, 1, "13", "bolt%20err");
                    continue;
                }

                let mut map = server_store.lock().unwrap();
                let matched = target_digest
                    .as_deref()
                    .is_some_and(|want| map.get(&name).is_some_and(|cur| cur == want));
                if matched {
                    map.remove(&name);
                    send_grpc_ok(&mut conn, 1, &[]);
                } else {
                    send_grpc_err(&mut conn, 1, "5", "target%20digest%20mismatch");
                }
            }
        });

        let r1 = delete_image_if_target_matches(
            &sock,
            "k8s.io",
            "docker.io/library/retire-me:dev",
            "sha256:rewritten1",
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(r1, DeleteOutcome::Deleted);
        assert!(
            !store
                .lock()
                .unwrap()
                .contains_key("docker.io/library/retire-me:dev")
        );

        let r2 = delete_image_if_target_matches(
            &sock,
            "k8s.io",
            "docker.io/library/replaced:dev",
            "sha256:rewritten2",
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(r2, DeleteOutcome::NotFoundOrChanged);
        assert_eq!(
            store
                .lock()
                .unwrap()
                .get("docker.io/library/replaced:dev")
                .map(String::as_str),
            Some("sha256:native2")
        );

        let err = delete_image_if_target_matches(
            &sock,
            "k8s.io",
            "docker.io/library/fail-me:dev",
            "sha256:any",
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(err.contains("grpc-status 13"), "{err}");
        assert!(err.contains("bolt err"), "{err}");

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: `Images.List` decodes structured protobuf label maps so
    /// a comma-separated ownership marker embedded inside another label's value
    /// cannot impersonate `io.katamaran.k8s.source-digest`, and enforces
    /// `max_response_bytes` explicitly instead of silently truncating.
    #[test]
    fn list_images_decodes_structured_labels_with_commas_and_enforces_limit() {
        let (dir, sock) = temp_sock("list");
        let listener = UnixListener::bind(&sock).unwrap();

        let mut list_resp = Vec::new();
        let unrelated = ContainerdImage {
            name: "docker.io/library/unrelated:latest".into(),
            labels: HashMap::from([
                ("io.cri-containerd.image".into(), "managed".into()),
                (
                    "note".into(),
                    "user-tag,io.katamaran.k8s.source-digest=sha256:forged".into(),
                ),
            ]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:1111".into(),
                size: 512,
                ..Default::default()
            },
        };
        let genuine = ContainerdImage {
            name: "docker.io/library/mirror:dev".into(),
            labels: HashMap::from([
                ("io.cri-containerd.image".into(), "managed".into()),
                (
                    "io.katamaran.k8s.source-digest".into(),
                    "sha256:real-source".into(),
                ),
            ]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:2222".into(),
                size: 768,
                ..Default::default()
            },
        };
        encode_proto_bytes(&mut list_resp, 1, &encode_image(&unrelated));
        encode_proto_bytes(&mut list_resp, 1, &encode_image(&genuine));
        // Add enough filler images to exceed HTTP/2's default 64 KiB window.
        for i in 0..200 {
            let filler = ContainerdImage {
                name: format!("docker.io/library/filler-{i:03}:latest"),
                labels: HashMap::from([("pad".into(), "x".repeat(300))]),
                target: Descriptor {
                    media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                    digest: format!("sha256:{i:064x}"),
                    size: 100,
                    ..Default::default()
                },
            };
            encode_proto_bytes(&mut list_resp, 1, &encode_image(&filler));
        }
        assert!(list_resp.len() > 65_535);

        let resp_bytes = list_resp.clone();
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut conn, _) = listener.accept().unwrap();
                let mut preface = [0u8; 24];
                conn.read_exact(&mut preface).unwrap();
                write_frame(&mut conn, FRAME_SETTINGS, 0, 0, &[]).unwrap();
                conn.flush().unwrap();

                let mut dec = HpackDecoder::new(4096);
                let mut hdrs = HashMap::new();
                loop {
                    let (ft, flags, sid, payload) = read_frame(&mut conn).unwrap();
                    match ft {
                        FRAME_SETTINGS if flags & FLAG_ACK == 0 => {
                            write_frame(&mut conn, FRAME_SETTINGS, FLAG_ACK, 0, &[]).unwrap();
                            conn.flush().unwrap();
                        }
                        FRAME_HEADERS if sid == 1 => {
                            for (k, v) in dec.decode(&payload).unwrap() {
                                hdrs.insert(k, v);
                            }
                        }
                        FRAME_DATA if sid == 1 && (flags & FLAG_END_STREAM != 0) => break,
                        _ => {}
                    }
                }
                assert_eq!(
                    hdrs.get(":path").map(String::as_str),
                    Some("/containerd.services.images.v1.Images/List")
                );
                let mut hdr = vec![0x88];
                encode_hpack_huffman_header(&mut hdr, "content-type", "application/grpc", false);
                let _ = write_frame(&mut conn, FRAME_HEADERS, FLAG_END_HEADERS, 1, &hdr);
                let framed = frame_grpc_message(&resp_bytes).unwrap();
                for chunk in framed.chunks(DEFAULT_MAX_FRAME_SIZE) {
                    if write_frame(&mut conn, FRAME_DATA, 0, 1, chunk).is_err() {
                        break;
                    }
                }
                let mut trl = Vec::new();
                encode_hpack_huffman_header(&mut trl, "grpc-status", "0", false);
                let _ = write_frame(
                    &mut conn,
                    FRAME_HEADERS,
                    FLAG_END_HEADERS | FLAG_END_STREAM,
                    1,
                    &trl,
                );
                let _ = conn.flush();
            }
        });

        let images = list_images(&sock, "k8s.io", 1 << 20, Duration::from_secs(5)).unwrap();
        assert_eq!(images.len(), 202);
        let got_unrelated = images
            .iter()
            .find(|i| i.name == "docker.io/library/unrelated:latest")
            .unwrap();
        assert_eq!(
            got_unrelated.labels.get("io.katamaran.k8s.source-digest"),
            None,
            "comma inside another label's value must never spoof ownership marker"
        );
        assert_eq!(
            got_unrelated.labels.get("note").map(String::as_str),
            Some("user-tag,io.katamaran.k8s.source-digest=sha256:forged")
        );
        let got_genuine = images
            .iter()
            .find(|i| i.name == "docker.io/library/mirror:dev")
            .unwrap();
        assert_eq!(
            got_genuine
                .labels
                .get("io.katamaran.k8s.source-digest")
                .map(String::as_str),
            Some("sha256:real-source")
        );

        // Tighter response cap must fail explicitly rather than returning a partial list.
        let err = list_images(&sock, "k8s.io", 4096, Duration::from_secs(5)).unwrap_err();
        assert!(err.contains("exceeded"), "{err}");

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[derive(Default)]
    struct MockContainerdState {
        /// Shared underlying content store: digest -> size (represents blobs on
        /// disk; huge layer sizes need zero RAM allocation here).
        shared_store: HashMap<String, i64>,
        /// Per-namespace committed content metadata: (ns, digest) -> (size, labels).
        ns_content: HashMap<(String, String), (i64, HashMap<String, String>)>,
        /// Per-namespace images: (ns, name) -> ContainerdImage.
        images: HashMap<(String, String), ContainerdImage>,
        /// Active leases: (ns, lease_id) -> labels.
        leases: HashMap<(String, String), HashMap<String, String>>,
        /// Pinned resources recorded via `Leases.AddResource`: (ns, lease_id, resource_type, resource_id).
        lease_resources: Vec<(String, String, String, String)>,
        /// Server-side counter for generating unique lease IDs when `CreateRequest.id` is omitted.
        next_lease_seq: u64,
        /// Simulated remaining disk headroom on the shared data disk (bytes).
        remaining_disk_bytes: i64,
        /// Total raw payload bytes written into ingest files across all Content.Write calls.
        total_disk_bytes_written: i64,
        /// Optional hook: mutate `moby` target right after manifest write commits.
        mutate_moby_after_manifest: Option<(String, String)>,
        /// Optional hook: insert or replace a `k8s.io` image right after manifest write commits.
        mutate_k8s_after_manifest: Option<ContainerdImage>,
        /// Optional hook: fail the next `k8s.io` `Images.Create` RPC with gRPC INTERNAL.
        fail_next_k8s_create: bool,
        /// Optional hook: after committing a `k8s.io` `Images.Delete`, drop the
        /// connection without replying (making Delete outcome uncertain) and
        /// optionally insert a competing `k8s.io` image.
        drop_after_k8s_delete: Option<Option<ContainerdImage>>,
        /// Optional hook: on a `k8s.io` `Images.Delete`, drop Connection 1
        /// WITHOUT deleting yet, then delete the matching tag asynchronously
        /// after this delay (modelling an in-flight BoltDB delete that commits
        /// after recovery starts).
        delayed_k8s_delete: Option<Duration>,
        /// Optional hook: once `k8s.io` `Images.Delete` commits, fail subsequent
        /// `k8s.io` `Images.Get` RPCs (to verify recovery keeps the lease pinned
        /// when recovery itself fails).
        fail_k8s_get_after_delete: bool,
        k8s_delete_committed: bool,
        k8s_get_calls: usize,
        last_delete_grpc_timeout: Option<String>,
        /// Count of `Images.Update` RPCs received (must remain 0 under CAS publish).
        images_update_calls: usize,
        /// Optional image name that triggers a gRPC INTERNAL error on `Images.Get`.
        fail_get_image: Option<String>,
        /// Call counters for verifying direct `Content.Info` vs `Content.List`.
        content_info_calls: usize,
        content_list_calls: usize,
    }

    fn spawn_mock_containerd(
        listener: UnixListener,
        state: Arc<Mutex<MockContainerdState>>,
        conns: usize,
        initial_stream_window: u32,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            for _ in 0..conns {
                let Ok((mut conn, _)) = listener.accept() else {
                    break;
                };
                let mut preface = [0u8; 24];
                if conn.read_exact(&mut preface).is_err() {
                    continue;
                }
                assert_eq!(&preface, CLIENT_PREFACE);

                let mut srv_settings = Vec::new();
                srv_settings.extend_from_slice(&SETTINGS_INITIAL_WINDOW_SIZE.to_be_bytes());
                srv_settings.extend_from_slice(&initial_stream_window.to_be_bytes());
                write_frame(&mut conn, FRAME_SETTINGS, 0, 0, &srv_settings).unwrap();
                conn.flush().unwrap();

                let mut dec = HpackDecoder::new(4096);
                let mut active_headers: HashMap<u32, HashMap<String, String>> = HashMap::new();
                let mut active_data: HashMap<u32, Vec<u8>> = HashMap::new();
                let mut active_write: HashMap<u32, (String, String, i64, Vec<u8>)> = HashMap::new();

                while let Ok((ft, flags, sid, payload)) = read_frame(&mut conn) {
                    match ft {
                        FRAME_SETTINGS => {
                            if flags & FLAG_ACK == 0 {
                                let _ = write_frame(&mut conn, FRAME_SETTINGS, FLAG_ACK, 0, &[]);
                                let _ = conn.flush();
                            }
                        }
                        FRAME_PING => {
                            if flags & FLAG_ACK == 0 && payload.len() == 8 {
                                let _ = write_frame(&mut conn, FRAME_PING, FLAG_ACK, 0, &payload);
                                let _ = conn.flush();
                            }
                        }
                        FRAME_WINDOW_UPDATE => {}
                        FRAME_HEADERS if sid > 0 => {
                            let mut hdrs = HashMap::new();
                            for (k, v) in dec.decode(&payload).unwrap() {
                                hdrs.insert(k, v);
                            }
                            active_headers.insert(sid, hdrs);
                            active_data.insert(sid, Vec::new());
                        }
                        FRAME_DATA if sid > 0 => {
                            let flow_inc = payload.len() as u32;
                            if flow_inc > 0 {
                                let _ = write_frame(
                                    &mut conn,
                                    FRAME_WINDOW_UPDATE,
                                    0,
                                    0,
                                    &flow_inc.to_be_bytes(),
                                );
                                if flags & FLAG_END_STREAM == 0 {
                                    let _ = write_frame(
                                        &mut conn,
                                        FRAME_WINDOW_UPDATE,
                                        0,
                                        sid,
                                        &flow_inc.to_be_bytes(),
                                    );
                                }
                                let _ = conn.flush();
                            }
                            let buf = active_data.entry(sid).or_default();
                            buf.extend_from_slice(&payload);

                            let path = active_headers
                                .get(&sid)
                                .and_then(|h| h.get(":path"))
                                .cloned()
                                .unwrap_or_default();
                            let ns = active_headers
                                .get(&sid)
                                .and_then(|h| h.get("containerd-namespace"))
                                .cloned()
                                .unwrap_or_default();

                            if path == "/containerd.services.content.v1.Content/Write" {
                                // Process every complete gRPC WriteContentRequest message as it arrives.
                                while buf.len() >= 5 {
                                    let mlen = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]])
                                        as usize;
                                    if buf.len() < 5 + mlen {
                                        break;
                                    }
                                    let msg = buf[5..5 + mlen].to_vec();
                                    buf.drain(..5 + mlen);
                                    let wreq = decode_write_content_request(&msg);
                                    let entry = active_write.entry(sid).or_insert_with(|| {
                                        (
                                            wreq.write_ref.clone(),
                                            wreq.expected.clone(),
                                            wreq.total,
                                            Vec::new(),
                                        )
                                    });
                                    if !wreq.expected.is_empty() {
                                        entry.1 = wreq.expected.clone();
                                    }
                                    if wreq.total > 0 {
                                        entry.2 = wreq.total;
                                    }

                                    let expected = entry.1.clone();
                                    let mut st = state.lock().unwrap();
                                    if st.ns_content.contains_key(&(ns.clone(), expected.clone())) {
                                        send_grpc_err(&mut conn, sid, "6", "already%20exists");
                                        active_write.remove(&sid);
                                        break;
                                    }

                                    // Emulate containerd's shared-content fast path:
                                    // only when `expected` exists in shared_store with matching
                                    // size AND `offset == size` AND `data.is_empty()` does
                                    // containerd skip `createAndCopy` / ingest writes.
                                    let shared_size = st.shared_store.get(&expected).copied();
                                    let is_zero_copy_shared = shared_size.is_some_and(|ss| {
                                        (wreq.total == 0 || wreq.total == ss)
                                            && wreq.offset == ss
                                            && wreq.data.is_empty()
                                            && entry.3.is_empty()
                                    });

                                    if !is_zero_copy_shared {
                                        // Non-shared write or Truncate/createAndCopy consumes disk headroom.
                                        let write_bytes = if let Some(ss) = shared_size
                                            && wreq.offset == 0
                                            && wreq.data.is_empty()
                                        {
                                            ss // models wr.Truncate(0) -> createAndCopy!
                                        } else {
                                            wreq.data.len() as i64
                                        };
                                        if write_bytes > st.remaining_disk_bytes {
                                            send_grpc_err(
                                                &mut conn,
                                                sid,
                                                "8",
                                                "no%20space%20left%20on%20device",
                                            );
                                            active_write.remove(&sid);
                                            break;
                                        }
                                        st.remaining_disk_bytes -= write_bytes;
                                        st.total_disk_bytes_written += write_bytes;
                                        assert_eq!(
                                            wreq.offset as usize,
                                            entry.3.len(),
                                            "out-of-order write offset"
                                        );
                                        entry.3.extend_from_slice(&wreq.data);
                                    }

                                    if wreq.action == WRITE_ACTION_COMMIT {
                                        let final_size = if is_zero_copy_shared {
                                            shared_size.unwrap()
                                        } else {
                                            let actual_digest =
                                                format!("sha256:{}", sha256_hex(&entry.3));
                                            assert_eq!(actual_digest, expected);
                                            entry.3.len() as i64
                                        };
                                        st.shared_store.insert(expected.clone(), final_size);
                                        st.ns_content.insert(
                                            (ns.clone(), expected.clone()),
                                            (final_size, wreq.labels.clone()),
                                        );
                                        if wreq
                                            .labels
                                            .contains_key("containerd.io/gc.ref.content.config")
                                        {
                                            if let Some((moby_name, new_digest)) =
                                                st.mutate_moby_after_manifest.take()
                                                && let Some(img) =
                                                    st.images.get_mut(&("moby".into(), moby_name))
                                            {
                                                img.target.digest = new_digest;
                                            }
                                            if let Some(k8s_img) =
                                                st.mutate_k8s_after_manifest.take()
                                            {
                                                st.images.insert(
                                                    ("k8s.io".into(), k8s_img.name.clone()),
                                                    k8s_img,
                                                );
                                            }
                                        }
                                        drop(st);
                                        let mut resp_msg = Vec::new();
                                        encode_proto_varint(&mut resp_msg, 1, WRITE_ACTION_COMMIT);
                                        encode_proto_varint(&mut resp_msg, 4, final_size as u64);
                                        encode_proto_bytes(&mut resp_msg, 6, expected.as_bytes());
                                        if entry.3.len() <= CONTENT_WRITE_CHUNK {
                                            send_grpc_ok(&mut conn, sid, &resp_msg);
                                        } else {
                                            let framed = frame_grpc_message(&resp_msg).unwrap();
                                            write_frame(&mut conn, FRAME_DATA, 0, sid, &framed)
                                                .unwrap();
                                            let mut trl = Vec::new();
                                            encode_hpack_huffman_header(
                                                &mut trl,
                                                "grpc-status",
                                                "0",
                                                false,
                                            );
                                            write_frame(
                                                &mut conn,
                                                FRAME_HEADERS,
                                                FLAG_END_HEADERS | FLAG_END_STREAM,
                                                sid,
                                                &trl,
                                            )
                                            .unwrap();
                                            conn.flush().unwrap();
                                        }
                                        active_write.remove(&sid);
                                    } else {
                                        // Intermediate WRITE response.
                                        drop(st);
                                        if entry.3.len() == wreq.data.len() {
                                            let mut hdr = vec![0x88];
                                            encode_hpack_huffman_header(
                                                &mut hdr,
                                                "content-type",
                                                "application/grpc",
                                                false,
                                            );
                                            write_frame(
                                                &mut conn,
                                                FRAME_HEADERS,
                                                FLAG_END_HEADERS,
                                                sid,
                                                &hdr,
                                            )
                                            .unwrap();
                                        }
                                        let mut resp_msg = Vec::new();
                                        encode_proto_varint(&mut resp_msg, 1, WRITE_ACTION_WRITE);
                                        encode_proto_varint(&mut resp_msg, 4, entry.3.len() as u64);
                                        let framed = frame_grpc_message(&resp_msg).unwrap();
                                        write_frame(&mut conn, FRAME_DATA, 0, sid, &framed)
                                            .unwrap();
                                        conn.flush().unwrap();
                                    }
                                }
                                continue;
                            }

                            if flags & FLAG_END_STREAM == 0 {
                                continue;
                            }
                            let msg_len =
                                u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                            let req_proto = buf[5..5 + msg_len].to_vec();
                            let mut st = state.lock().unwrap();
                            match path.as_str() {
                                "/containerd.services.images.v1.Images/Get" => {
                                    let name = decode_first_string(&req_proto);
                                    if ns == "k8s.io" {
                                        st.k8s_get_calls += 1;
                                    }
                                    if st.fail_get_image.as_deref() == Some(name.as_str())
                                        || (ns == "k8s.io"
                                            && st.fail_k8s_get_after_delete
                                            && st.k8s_delete_committed)
                                    {
                                        send_grpc_err(&mut conn, sid, "13", "bolt%20get%20error");
                                    } else if let Some(img) = st.images.get(&(ns.clone(), name)) {
                                        let mut out = Vec::new();
                                        encode_proto_bytes(&mut out, 1, &encode_image(img));
                                        send_grpc_ok(&mut conn, sid, &out);
                                    } else {
                                        send_grpc_err(&mut conn, sid, "5", "not%20found");
                                    }
                                }
                                "/containerd.services.images.v1.Images/Delete" => {
                                    let (name, target_digest) = decode_delete_request(&req_proto);
                                    if let Some(t) = active_headers
                                        .get(&sid)
                                        .and_then(|h| h.get("grpc-timeout"))
                                        .cloned()
                                    {
                                        st.last_delete_grpc_timeout = Some(t);
                                    }
                                    let key = (ns.clone(), name);
                                    let matched = match target_digest.as_deref() {
                                        Some(want) => st
                                            .images
                                            .get(&key)
                                            .is_some_and(|cur| cur.target.digest == want),
                                        None => st.images.contains_key(&key),
                                    };
                                    if matched {
                                        if ns == "k8s.io"
                                            && let Some(delay) = st.delayed_k8s_delete.take()
                                        {
                                            let bg_state = Arc::clone(&state);
                                            let bg_key = key;
                                            let bg_want = target_digest;
                                            drop(st);
                                            thread::spawn(move || {
                                                thread::sleep(delay);
                                                let mut s = bg_state.lock().unwrap();
                                                let still_matches = match bg_want.as_deref() {
                                                    Some(want) => {
                                                        s.images.get(&bg_key).is_some_and(|cur| {
                                                            cur.target.digest == want
                                                        })
                                                    }
                                                    None => s.images.contains_key(&bg_key),
                                                };
                                                if still_matches {
                                                    s.images.remove(&bg_key);
                                                    s.k8s_delete_committed = true;
                                                }
                                            });
                                            // Drop socket without replying while the delete is still in flight.
                                            break;
                                        }
                                        st.images.remove(&key);
                                        if ns == "k8s.io" {
                                            st.k8s_delete_committed = true;
                                        }
                                        if ns == "k8s.io"
                                            && let Some(competing) = st.drop_after_k8s_delete.take()
                                        {
                                            if let Some(img) = competing {
                                                st.images.insert(
                                                    ("k8s.io".into(), img.name.clone()),
                                                    img,
                                                );
                                            }
                                            drop(st);
                                            // Drop socket without replying so client sees uncertain delete outcome.
                                            break;
                                        }
                                        send_grpc_ok(&mut conn, sid, &[]);
                                    } else {
                                        send_grpc_err(
                                            &mut conn,
                                            sid,
                                            "5",
                                            "target%20digest%20mismatch",
                                        );
                                    }
                                }
                                "/containerd.services.images.v1.Images/Create" => {
                                    let (img, _) = decode_image_and_mask(&req_proto);
                                    if ns == "k8s.io" && st.fail_next_k8s_create {
                                        st.fail_next_k8s_create = false;
                                        send_grpc_err(
                                            &mut conn,
                                            sid,
                                            "13",
                                            "simulated%20create%20failure",
                                        );
                                        continue;
                                    }
                                    let key = (ns.clone(), img.name.clone());
                                    if let std::collections::hash_map::Entry::Vacant(e) =
                                        st.images.entry(key)
                                    {
                                        let mut out = Vec::new();
                                        encode_proto_bytes(&mut out, 1, &encode_image(&img));
                                        e.insert(img);
                                        send_grpc_ok(&mut conn, sid, &out);
                                    } else {
                                        send_grpc_err(&mut conn, sid, "6", "already%20exists");
                                    }
                                }
                                "/containerd.services.images.v1.Images/Update" => {
                                    st.images_update_calls += 1;
                                    let (img, _) = decode_image_and_mask(&req_proto);
                                    let key = (ns.clone(), img.name.clone());
                                    if let std::collections::hash_map::Entry::Occupied(mut e) =
                                        st.images.entry(key)
                                    {
                                        let mut out = Vec::new();
                                        encode_proto_bytes(&mut out, 1, &encode_image(&img));
                                        e.insert(img);
                                        send_grpc_ok(&mut conn, sid, &out);
                                    } else {
                                        send_grpc_err(&mut conn, sid, "5", "not%20found");
                                    }
                                }
                                "/containerd.services.content.v1.Content/Info" => {
                                    st.content_info_calls += 1;
                                    let digest = decode_first_string(&req_proto);
                                    if digest == "sha256:trigger-rpc-error" {
                                        send_grpc_err(&mut conn, sid, "13", "bolt%20read%20error");
                                    } else if let Some(&(size, _)) =
                                        st.ns_content.get(&(ns.clone(), digest.clone()))
                                    {
                                        let mut info = Vec::new();
                                        encode_proto_bytes(&mut info, 1, digest.as_bytes());
                                        encode_proto_varint(&mut info, 2, size as u64);
                                        let mut out = Vec::new();
                                        encode_proto_bytes(&mut out, 1, &info);
                                        send_grpc_ok(&mut conn, sid, &out);
                                    } else {
                                        send_grpc_err(&mut conn, sid, "5", "not%20found");
                                    }
                                }
                                "/containerd.services.content.v1.Content/Update" => {
                                    let (digest, labels, mask) =
                                        decode_content_update_req(&req_proto);
                                    if let Some((_, existing_labels)) =
                                        st.ns_content.get_mut(&(ns.clone(), digest))
                                    {
                                        for p in mask {
                                            if let Some(k) = p.strip_prefix("labels.")
                                                && let Some(v) = labels.get(k)
                                            {
                                                existing_labels.insert(k.to_string(), v.clone());
                                            }
                                        }
                                        send_grpc_ok(&mut conn, sid, &[]);
                                    } else {
                                        send_grpc_err(&mut conn, sid, "5", "not%20found");
                                    }
                                }
                                "/containerd.services.content.v1.Content/List" => {
                                    let filters = decode_repeated_strings(&req_proto);
                                    st.content_list_calls += 1;

                                    let mut wanted: Option<HashSet<String>> = None;
                                    let mut parse_err: Option<&str> = None;
                                    let mut trigger_err = false;
                                    if !filters.is_empty() {
                                        let mut set = HashSet::new();
                                        for f in &filters {
                                            let Some(val) = f.strip_prefix("digest==") else {
                                                parse_err = Some("unsupported%20filter");
                                                break;
                                            };
                                            let unquoted = val
                                                .strip_prefix('"')
                                                .and_then(|s| s.strip_suffix('"'))
                                                .unwrap_or(val);
                                            if unquoted == "sha256:trigger-rpc-error" {
                                                trigger_err = true;
                                            }
                                            set.insert(unquoted.to_string());
                                        }
                                        wanted = Some(set);
                                    }

                                    if let Some(msg) = parse_err {
                                        drop(st);
                                        send_grpc_err(&mut conn, sid, "3", msg);
                                        continue;
                                    }
                                    if trigger_err {
                                        drop(st);
                                        send_grpc_err(&mut conn, sid, "13", "bolt%20list%20error");
                                        continue;
                                    }

                                    // Stream multiple batched ListContentResponse messages like containerd,
                                    // or trailers-only with zero DATA frames when no blobs match.
                                    let mut items: Vec<(String, i64, HashMap<String, String>)> = st
                                        .ns_content
                                        .iter()
                                        .filter(|((n, d), _)| {
                                            n == &ns
                                                && wanted.as_ref().is_none_or(|set| set.contains(d))
                                        })
                                        .map(|((_, d), (sz, lbls))| (d.clone(), *sz, lbls.clone()))
                                        .collect();
                                    items.sort_by(|a, b| a.0.cmp(&b.0));
                                    drop(st);
                                    if items.is_empty() {
                                        let mut trl = vec![0x88];
                                        encode_hpack_huffman_header(
                                            &mut trl,
                                            "content-type",
                                            "application/grpc",
                                            false,
                                        );
                                        encode_hpack_huffman_header(
                                            &mut trl,
                                            "grpc-status",
                                            "0",
                                            false,
                                        );
                                        let _ = write_frame(
                                            &mut conn,
                                            FRAME_HEADERS,
                                            FLAG_END_HEADERS | FLAG_END_STREAM,
                                            sid,
                                            &trl,
                                        );
                                        let _ = conn.flush();
                                        continue;
                                    }
                                    let mut hdr = vec![0x88];
                                    encode_hpack_huffman_header(
                                        &mut hdr,
                                        "content-type",
                                        "application/grpc",
                                        false,
                                    );
                                    let _ = write_frame(
                                        &mut conn,
                                        FRAME_HEADERS,
                                        FLAG_END_HEADERS,
                                        sid,
                                        &hdr,
                                    );
                                    for batch in items.chunks(2) {
                                        let mut resp_msg = Vec::new();
                                        for (digest, size, lbls) in batch {
                                            let mut info = Vec::new();
                                            encode_proto_bytes(&mut info, 1, digest.as_bytes());
                                            encode_proto_varint(&mut info, 2, *size as u64);
                                            for (k, v) in lbls {
                                                encode_proto_map_entry(&mut info, 5, k, v);
                                            }
                                            encode_proto_bytes(&mut resp_msg, 1, &info);
                                        }
                                        let framed = frame_grpc_message(&resp_msg).unwrap();
                                        let mut failed = false;
                                        for chunk in framed.chunks(DEFAULT_MAX_FRAME_SIZE) {
                                            if write_frame(&mut conn, FRAME_DATA, 0, sid, chunk)
                                                .is_err()
                                            {
                                                failed = true;
                                                break;
                                            }
                                        }
                                        if failed {
                                            break;
                                        }
                                    }
                                    let mut trl = Vec::new();
                                    encode_hpack_huffman_header(
                                        &mut trl,
                                        "grpc-status",
                                        "0",
                                        false,
                                    );
                                    let _ = write_frame(
                                        &mut conn,
                                        FRAME_HEADERS,
                                        FLAG_END_HEADERS | FLAG_END_STREAM,
                                        sid,
                                        &trl,
                                    );
                                    let _ = conn.flush();
                                }
                                "/containerd.services.content.v1.Content/Abort" => {
                                    send_grpc_ok(&mut conn, sid, &[]);
                                }
                                "/containerd.services.leases.v1.Leases/Create" => {
                                    let (req_id, labels) = decode_create_lease_req(&req_proto);
                                    let id = if req_id.is_empty() {
                                        loop {
                                            st.next_lease_seq += 1;
                                            let cand = format!("srv-lease-{}", st.next_lease_seq);
                                            if !st.leases.contains_key(&(ns.clone(), cand.clone()))
                                            {
                                                break cand;
                                            }
                                        }
                                    } else {
                                        req_id
                                    };
                                    let key = (ns.clone(), id.clone());
                                    if let std::collections::hash_map::Entry::Vacant(e) =
                                        st.leases.entry(key)
                                    {
                                        e.insert(labels);
                                        let mut lease = Vec::new();
                                        encode_proto_bytes(&mut lease, 1, id.as_bytes());
                                        let mut out = Vec::new();
                                        encode_proto_bytes(&mut out, 1, &lease);
                                        send_grpc_ok(&mut conn, sid, &out);
                                    } else {
                                        send_grpc_err(&mut conn, sid, "6", "lease%20exists");
                                    }
                                }
                                "/containerd.services.leases.v1.Leases/AddResource" => {
                                    let (lease_id, res_id, res_type) =
                                        decode_add_lease_resource_req(&req_proto);
                                    if st.leases.contains_key(&(ns.clone(), lease_id.clone())) {
                                        st.lease_resources.push((
                                            ns.clone(),
                                            lease_id,
                                            res_type,
                                            res_id,
                                        ));
                                        send_grpc_ok(&mut conn, sid, &[]);
                                    } else {
                                        send_grpc_err(&mut conn, sid, "5", "lease%20not%20found");
                                    }
                                }
                                "/containerd.services.leases.v1.Leases/Delete" => {
                                    let id = decode_first_string(&req_proto);
                                    st.leases.remove(&(ns.clone(), id));
                                    send_grpc_ok(&mut conn, sid, &[]);
                                }
                                other => panic!("unexpected RPC {other}"),
                            }
                        }
                        _ => {}
                    }
                }
            }
        })
    }

    fn sample_plan(layer_digest: &str, config_bytes: Vec<u8>) -> RewritePlan {
        let config_hex = sha256_hex(&config_bytes);
        let manifest_bytes = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": format!("sha256:{config_hex}"),
                "size": config_bytes.len(),
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest,
                "size": 10i64 << 30,
            }],
        }))
        .unwrap();
        let manifest_hex = sha256_hex(&manifest_bytes);
        RewritePlan {
            source_digest: "sha256:source-amd64".into(),
            selected_platform: "linux/amd64".into(),
            layer_digests: vec![layer_digest.into()],
            new_config_hex: config_hex,
            new_config_bytes: config_bytes,
            new_manifest_hex: manifest_hex,
            new_manifest_bytes: manifest_bytes,
            manifest_media_type: "application/vnd.oci.image.manifest.v1+json".into(),
            native_arch: "arm64".into(),
        }
    }

    /// Regression test: mirroring a 10 GiB amd64 image when remaining disk
    /// headroom is only 1 MiB (enough for rewritten metadata, far less than a
    /// second copy of the layer) must succeed by linking the shared layer blob
    /// with zero bytes copied, setting GC child labels, and cleaning up its
    /// temporary lease. Also exercises idempotent re-publish and guarded CAS
    /// replacement without calling `Images.Update`.
    #[test]
    fn publish_rewritten_image_links_10gib_layer_zero_copy_under_1mib_disk_headroom() {
        let (dir, sock) = temp_sock("zero-copy");
        let listener = UnixListener::bind(&sock).unwrap();

        let huge_layer_digest = format!("sha256:{}", "a".repeat(64));
        let huge_layer_size: i64 = 10 << 30; // 10 GiB
        let plan = sample_plan(
            &huge_layer_digest,
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );

        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(huge_layer_digest.clone(), huge_layer_size)]),
            ns_content: HashMap::from([(
                ("moby".into(), huge_layer_digest.clone()),
                (huge_layer_size, HashMap::new()),
            )]),
            images: HashMap::from([(
                ("moby".into(), "docker.io/library/huge:dev".into()),
                ContainerdImage {
                    name: "docker.io/library/huge:dev".into(),
                    labels: HashMap::new(),
                    target: Descriptor {
                        media_type: "application/vnd.oci.image.index.v1+json".into(),
                        digest: "sha256:source-amd64".into(),
                        size: 256,
                        ..Default::default()
                    },
                },
            )]),
            // Only 1 MiB of free disk space: writing a second copy of the 10 GiB
            // layer would fail immediately with RESOURCE_EXHAUSTED ("no space").
            remaining_disk_bytes: 1 << 20,
            ..Default::default()
        }));

        let server = spawn_mock_containerd(listener, Arc::clone(&state), 3, 65_535);

        let rewritten = publish_rewritten_image(
            &sock,
            "docker.io/library/huge:dev",
            "io.katamaran.k8s.source-digest",
            None,
            &plan,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(
            rewritten.manifest,
            format!("sha256:{}", plan.new_manifest_hex)
        );
        assert_eq!(rewritten.config, format!("sha256:{}", plan.new_config_hex));

        {
            let st = state.lock().unwrap();
            let expected_meta_bytes =
                (plan.new_config_bytes.len() + plan.new_manifest_bytes.len()) as i64;
            assert_eq!(
                st.total_disk_bytes_written, expected_meta_bytes,
                "only rewritten config and manifest bytes may be written to disk"
            );
            assert!(
                st.leases.is_empty(),
                "temporary lease must be deleted after publish"
            );
            // Layer is linked into k8s.io with its full 10 GiB size.
            assert_eq!(
                st.ns_content
                    .get(&("k8s.io".into(), huge_layer_digest.clone()))
                    .map(|(sz, _)| *sz),
                Some(huge_layer_size)
            );
            // Manifest carries GC labels keeping config + layer pinned.
            let (_, m_labels) = &st.ns_content[&("k8s.io".into(), rewritten.manifest.clone())];
            assert_eq!(
                m_labels
                    .get("containerd.io/gc.ref.content.config")
                    .map(String::as_str),
                Some(rewritten.config.as_str())
            );
            assert_eq!(
                m_labels
                    .get("containerd.io/gc.ref.content.l.0")
                    .map(String::as_str),
                Some(huge_layer_digest.as_str())
            );
            // k8s.io image points to the rewritten manifest with source-digest label.
            let k8s_img = &st.images[&("k8s.io".into(), "docker.io/library/huge:dev".into())];
            assert_eq!(k8s_img.target.digest, rewritten.manifest);
            assert_eq!(
                k8s_img
                    .labels
                    .get("io.katamaran.k8s.source-digest")
                    .map(String::as_str),
                Some("sha256:source-amd64")
            );
        }

        // Second publish when manifest/config/layers already exist in k8s.io
        // exercises ALREADY_EXISTS + Content.Update + idempotent k8s.io check.
        publish_rewritten_image(
            &sock,
            "docker.io/library/huge:dev",
            "io.katamaran.k8s.source-digest",
            Some(&rewritten.manifest),
            &plan,
            Duration::from_secs(5),
        )
        .unwrap();

        // Third publish replacing an older Katamaran mirror digest uses atomic
        // compare-and-delete + Create (never unconditional Images.Update).
        {
            let mut st = state.lock().unwrap();
            let k8s_img = st
                .images
                .get_mut(&("k8s.io".into(), "docker.io/library/huge:dev".into()))
                .unwrap();
            k8s_img.target.digest = "sha256:stale-mirror".into();
        }
        publish_rewritten_image(
            &sock,
            "docker.io/library/huge:dev",
            "io.katamaran.k8s.source-digest",
            Some("sha256:stale-mirror"),
            &plan,
            Duration::from_secs(5),
        )
        .unwrap();
        {
            let st = state.lock().unwrap();
            assert_eq!(st.images_update_calls, 0);
            assert_eq!(
                st.images[&("k8s.io".into(), "docker.io/library/huge:dev".into())]
                    .target
                    .digest,
                rewritten.manifest
            );
        }

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: a concurrent native kubelet pull in `k8s.io` must NEVER
    /// be overwritten by `publish_rewritten_image` (neither when `k8s.io` was
    /// initially absent nor when replacing a prior mirror digest).
    #[test]
    fn publish_rewritten_image_preserves_concurrent_native_k8s_tag_without_update() {
        let (dir, sock) = temp_sock("dest-changed");
        let listener = UnixListener::bind(&sock).unwrap();

        let layer_digest = format!("sha256:{}", "e".repeat(64));
        let plan = sample_plan(
            &layer_digest,
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );
        let native_kubelet = ContainerdImage {
            name: "docker.io/library/app:dev".into(),
            labels: HashMap::from([("io.cri-containerd.image".into(), "managed".into())]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:native-kubelet-arm64".into(),
                size: 512,
                ..Default::default()
            },
        };

        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(layer_digest.clone(), 4096)]),
            ns_content: HashMap::from([(("moby".into(), layer_digest), (4096, HashMap::new()))]),
            images: HashMap::from([(
                ("moby".into(), "docker.io/library/app:dev".into()),
                ContainerdImage {
                    name: "docker.io/library/app:dev".into(),
                    labels: HashMap::new(),
                    target: Descriptor {
                        media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                        digest: "sha256:source-amd64".into(),
                        size: 128,
                        ..Default::default()
                    },
                },
            )]),
            remaining_disk_bytes: 1 << 20,
            mutate_k8s_after_manifest: Some(native_kubelet.clone()),
            ..Default::default()
        }));

        let server = spawn_mock_containerd(listener, Arc::clone(&state), 2, 65_535);

        // Case 1: k8s.io tag was absent when sync listed it, and kubelet pulled
        // a native arm64 tag before Images.Create ran -> DestinationChanged!
        let err = publish_rewritten_image(
            &sock,
            "docker.io/library/app:dev",
            "io.katamaran.k8s.source-digest",
            None,
            &plan,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(
            matches!(err, PublishError::DestinationChanged { .. }),
            "expected DestinationChanged, got {err:?}"
        );
        {
            let st = state.lock().unwrap();
            assert_eq!(st.images_update_calls, 0);
            assert!(st.leases.is_empty());
            assert_eq!(
                st.images[&("k8s.io".into(), "docker.io/library/app:dev".into())]
                    .target
                    .digest,
                "sha256:native-kubelet-arm64",
                "concurrent native kubelet pull must remain untouched"
            );
        }

        // Case 2: sync expected to replace `sha256:old-mirror`, but kubelet
        // already replaced it with `sha256:native-kubelet-arm64` -> DestinationChanged!
        let err = publish_rewritten_image(
            &sock,
            "docker.io/library/app:dev",
            "io.katamaran.k8s.source-digest",
            Some("sha256:old-mirror"),
            &plan,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert_eq!(
            err,
            PublishError::DestinationChanged {
                expected: "sha256:old-mirror".into(),
                actual: "sha256:native-kubelet-arm64".into(),
            }
        );
        {
            let st = state.lock().unwrap();
            assert_eq!(st.images_update_calls, 0);
            assert!(st.leases.is_empty());
            assert_eq!(
                st.images[&("k8s.io".into(), "docker.io/library/app:dev".into())]
                    .target
                    .digest,
                "sha256:native-kubelet-arm64"
            );
        }

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: if Docker retags the `moby` source image before the
    /// pre-publish guard runs, `publish_rewritten_image` returns `SourceChanged`,
    /// leaves `k8s.io` images untouched, and deletes its temporary lease.
    #[test]
    fn publish_rewritten_image_aborts_on_concurrent_source_change_and_cleans_lease() {
        let (dir, sock) = temp_sock("source-changed");
        let listener = UnixListener::bind(&sock).unwrap();

        let layer_digest = format!("sha256:{}", "b".repeat(64));
        let plan = sample_plan(
            &layer_digest,
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );
        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(layer_digest.clone(), 4096)]),
            ns_content: HashMap::from([(("moby".into(), layer_digest), (4096, HashMap::new()))]),
            images: HashMap::from([(
                ("moby".into(), "docker.io/library/app:dev".into()),
                ContainerdImage {
                    name: "docker.io/library/app:dev".into(),
                    labels: HashMap::new(),
                    target: Descriptor {
                        media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                        digest: "sha256:source-amd64".into(),
                        size: 128,
                        ..Default::default()
                    },
                },
            )]),
            remaining_disk_bytes: 1 << 20,
            mutate_moby_after_manifest: Some((
                "docker.io/library/app:dev".into(),
                "sha256:new-native-retag".into(),
            )),
            ..Default::default()
        }));

        let server = spawn_mock_containerd(listener, Arc::clone(&state), 1, 65_535);
        let err = publish_rewritten_image(
            &sock,
            "docker.io/library/app:dev",
            "io.katamaran.k8s.source-digest",
            None,
            &plan,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert_eq!(
            err,
            PublishError::SourceChanged {
                expected: "sha256:source-amd64".into(),
                actual: "sha256:new-native-retag".into(),
            }
        );
        let st = state.lock().unwrap();
        assert!(
            !st.images
                .contains_key(&("k8s.io".into(), "docker.io/library/app:dev".into())),
            "k8s.io tag must not be published after source digest changed"
        );
        assert!(
            st.leases.is_empty(),
            "temporary lease must be deleted on SourceChanged abort"
        );

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: multi-chunk metadata blobs exceeding HTTP/2's 64 KiB
    /// flow-control window upload cleanly over `Content.Write`.
    #[test]
    fn publish_rewritten_image_streams_large_config_with_http2_flow_control() {
        let (dir, sock) = temp_sock("large-cfg");
        let listener = UnixListener::bind(&sock).unwrap();

        let layer_digest = format!("sha256:{}", "c".repeat(64));
        let large_config = serde_json::to_vec(&serde_json::json!({
            "architecture": "arm64",
            "os": "linux",
            "config": {"Labels": {"big": "z".repeat(200_000)}},
        }))
        .unwrap();
        let plan = sample_plan(&layer_digest, large_config);
        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(layer_digest.clone(), 1234)]),
            ns_content: HashMap::from([(("moby".into(), layer_digest), (1234, HashMap::new()))]),
            images: HashMap::from([(
                ("moby".into(), "docker.io/library/bigcfg:dev".into()),
                ContainerdImage {
                    name: "docker.io/library/bigcfg:dev".into(),
                    labels: HashMap::new(),
                    target: Descriptor {
                        media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                        digest: "sha256:source-amd64".into(),
                        size: 128,
                        ..Default::default()
                    },
                },
            )]),
            remaining_disk_bytes: 1 << 20,
            ..Default::default()
        }));

        // Small initial window (40 KiB) forces multiple HTTP/2 WINDOW_UPDATE round-trips.
        let server = spawn_mock_containerd(listener, Arc::clone(&state), 1, 40_960);
        let rewritten = publish_rewritten_image(
            &sock,
            "docker.io/library/bigcfg:dev",
            "io.katamaran.k8s.source-digest",
            None,
            &plan,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(rewritten.config, format!("sha256:{}", plan.new_config_hex));
        assert!(state.lock().unwrap().leases.is_empty());

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn publish_rejects_digest_pinned_reference() {
        let (_, sock) = temp_sock("digest-ref");
        let plan = sample_plan(
            &format!("sha256:{}", "d".repeat(64)),
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );
        let err = publish_rewritten_image(
            &sock,
            &format!("docker.io/library/app@sha256:{}", "e".repeat(64)),
            "io.katamaran.k8s.source-digest",
            None,
            &plan,
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(err.to_string().contains("digest-qualified"), "{err}");
    }

    /// Regression test: an unexpired lease left behind by an earlier guest boot
    /// (for example after an unexpected VM restart while `/var/lib/containerd`
    /// survived) must not collide with a subsequent rewrite publish or be
    /// deleted by it.
    #[test]
    fn publish_rewritten_image_uses_server_generated_lease_id_across_restarts() {
        let (dir, sock) = temp_sock("lease-restart");
        let listener = UnixListener::bind(&sock).unwrap();

        let layer_digest = format!("sha256:{}", "f".repeat(64));
        let plan = sample_plan(
            &layer_digest,
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );
        let prior_boot_lease = ("k8s.io".to_string(), "srv-lease-1".to_string());
        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(layer_digest.clone(), 4096)]),
            ns_content: HashMap::from([(("moby".into(), layer_digest), (4096, HashMap::new()))]),
            images: HashMap::from([(
                ("moby".into(), "docker.io/library/app:dev".into()),
                ContainerdImage {
                    name: "docker.io/library/app:dev".into(),
                    labels: HashMap::new(),
                    target: Descriptor {
                        media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                        digest: "sha256:source-amd64".into(),
                        size: 128,
                        ..Default::default()
                    },
                },
            )]),
            leases: HashMap::from([(
                prior_boot_lease.clone(),
                HashMap::from([(
                    "containerd.io/gc.expire".into(),
                    "2099-01-01T00:00:00Z".into(),
                )]),
            )]),
            remaining_disk_bytes: 1 << 20,
            ..Default::default()
        }));

        let server = spawn_mock_containerd(listener, Arc::clone(&state), 1, 65_535);
        publish_rewritten_image(
            &sock,
            "docker.io/library/app:dev",
            "io.katamaran.k8s.source-digest",
            None,
            &plan,
            Duration::from_secs(5),
        )
        .unwrap();

        let st = state.lock().unwrap();
        assert_eq!(
            st.leases.len(),
            1,
            "publisher must delete only its own server-generated lease"
        );
        assert!(
            st.leases.contains_key(&prior_boot_lease),
            "pre-existing lease from prior boot must remain untouched"
        );

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn content_info_client_distinguishes_found_not_found_and_rpc_error() {
        let (dir, sock) = temp_sock("content-info");
        let listener = UnixListener::bind(&sock).unwrap();

        let mut ns_content = HashMap::new();
        // Populate thousands of unrelated blobs with large labels to mirror a
        // busy `moby` store; per-digest `Content.Info` only touches queried keys.
        for i in 0..500u64 {
            ns_content.insert(
                ("moby".into(), format!("sha256:{i:064x}")),
                (
                    ((i + 1) * 1024) as i64,
                    HashMap::from([("pad".into(), "x".repeat(256))]),
                ),
            );
        }
        let target = format!("sha256:{:064x}", 42u64);
        let missing = format!("sha256:{}", "9".repeat(64));
        let state = Arc::new(Mutex::new(MockContainerdState {
            ns_content,
            ..Default::default()
        }));

        let server = spawn_mock_containerd(listener, Arc::clone(&state), 1, 65_535);

        let mut client = ContentInfoClient::connect(&sock, Duration::from_secs(5)).unwrap();
        assert_eq!(client.blob_size("moby", &target).unwrap(), Some(43 * 1024));
        assert_eq!(client.blob_size("moby", &missing).unwrap(), None);
        let err = client
            .blob_size("moby", "sha256:trigger-rpc-error")
            .unwrap_err();
        assert!(err.contains("grpc-status 13"), "{err}");
        assert!(err.contains("bolt read error"), "{err}");
        drop(client);

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: `ContentInfoClient::batch_blob_sizes` uses direct
    /// digest-keyed `Content.Info` lookups (NEVER `Content.List`) for both small
    /// batches (such as a one-layer manifest's config + layer) and large runs of
    /// 511+ absent index siblings, preserving deduplication, `NOT_FOUND`
    /// omission, and explicit propagation of non-`NOT_FOUND` RPC errors.
    #[test]
    fn content_info_client_batch_blob_sizes_uses_direct_info_without_content_list() {
        let (dir, sock) = temp_sock("batch");
        let listener = UnixListener::bind(&sock).unwrap();

        let mut ns_content = HashMap::new();
        // Populate 500 unrelated blobs with padded labels; direct Content.Info
        // lookups must never scan them or call Content.List.
        for i in 0..500u64 {
            ns_content.insert(
                ("moby".into(), format!("sha256:0000{i:060x}")),
                (
                    ((i + 1) * 512) as i64,
                    HashMap::from([("pad".into(), "x".repeat(512))]),
                ),
            );
        }
        let hit_a = format!("sha256:{}", "a".repeat(64));
        let hit_b = format!("sha256:{}", "b".repeat(64));
        let hit_c = format!("sha256:{}", "c".repeat(64));
        ns_content.insert(("moby".into(), hit_a.clone()), (111, HashMap::new()));
        ns_content.insert(("moby".into(), hit_b.clone()), (222, HashMap::new()));
        ns_content.insert(
            ("moby".into(), hit_c.clone()),
            (333, HashMap::from([("big".into(), "y".repeat(4096))])),
        );

        let state = Arc::new(Mutex::new(MockContainerdState {
            ns_content,
            ..Default::default()
        }));
        let server = spawn_mock_containerd(listener, Arc::clone(&state), 1, 65_535);

        let mut client = ContentInfoClient::connect(&sock, Duration::from_secs(5)).unwrap();

        // 1. Small multi-digest batch (e.g. a 1-layer manifest's config + layer):
        //    resolves via direct Content.Info with zero Content.List calls.
        let small = client
            .batch_blob_sizes("moby", &[hit_a.as_str(), hit_b.as_str()])
            .unwrap();
        assert_eq!(small.len(), 2);
        assert_eq!(small.get(&hit_a).copied(), Some(111));
        assert_eq!(small.get(&hit_b).copied(), Some(222));
        {
            let st = state.lock().unwrap();
            assert_eq!(
                st.content_list_calls, 0,
                "small batch must never call Content.List"
            );
            assert_eq!(st.content_info_calls, 2);
        }

        // 2. Large batch with 511 absent digests + 3 present digests + duplicate
        //    + invalid digest strings -> 514 unique valid Content.Info calls,
        //    zero Content.List calls.
        let absent: Vec<String> = (0..511u64)
            .map(|i| format!("sha256:eeee{i:060x}"))
            .collect();
        let mut query: Vec<&str> = Vec::new();
        query.push("not-a-valid-digest");
        query.push("sha256:bad\"quote");
        for d in &absent[..256] {
            query.push(d.as_str());
        }
        query.push(&hit_a);
        query.push(&hit_b);
        query.push(&hit_b); // duplicate should be deduplicated
        for d in &absent[256..] {
            query.push(d.as_str());
        }
        query.push(&hit_c);

        let got = client.batch_blob_sizes("moby", &query).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got.get(&hit_a).copied(), Some(111));
        assert_eq!(got.get(&hit_b).copied(), Some(222));
        assert_eq!(got.get(&hit_c).copied(), Some(333));

        // 3. All-absent batch returns empty map cleanly with zero Content.List calls.
        let absent_only: Vec<&str> = absent[..4].iter().map(String::as_str).collect();
        assert!(
            client
                .batch_blob_sizes("moby", &absent_only)
                .unwrap()
                .is_empty()
        );

        // 4. Non-NOT_FOUND RPC errors propagate explicitly.
        let err = client
            .batch_blob_sizes("moby", &[hit_a.as_str(), "sha256:trigger-rpc-error"])
            .unwrap_err();
        assert!(err.contains("grpc-status 13"), "{err}");
        assert!(err.contains("bolt read error"), "{err}");
        drop(client);

        let st = state.lock().unwrap();
        assert_eq!(
            st.content_list_calls, 0,
            "batch_blob_sizes must never trigger a full-namespace Content.List scan"
        );
        // 2 (small) + 514 (large unique valid) + 4 (absent_only) + 2 (error test)
        assert_eq!(st.content_info_calls, 522);
        drop(st);

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: `retire_image_if_unchanged` checks BOTH `moby`'s source
    /// digest and `k8s.io`'s target digest before deleting, so a concurrent
    /// Docker retag (or recreated tag during orphan cleanup, or native kubelet
    /// replacement) never deletes a working mirror or native image.
    #[test]
    fn retire_image_if_unchanged_verifies_moby_source_and_k8s_target() {
        let (dir, sock) = temp_sock("retire");
        let listener = UnixListener::bind(&sock).unwrap();

        let mk_img = |name: &str, digest: &str| ContainerdImage {
            name: name.into(),
            labels: HashMap::from([(
                "io.katamaran.k8s.source-digest".into(),
                "sha256:prev-good".into(),
            )]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: digest.into(),
                size: 128,
                ..Default::default()
            },
        };

        let state = Arc::new(Mutex::new(MockContainerdState {
            images: HashMap::from([
                // 1. Source retagged concurrently in moby; k8s still holds working mirror.
                (
                    ("moby".into(), "docker.io/library/retagged:dev".into()),
                    mk_img("docker.io/library/retagged:dev", "sha256:new-complete"),
                ),
                (
                    ("k8s.io".into(), "docker.io/library/retagged:dev".into()),
                    mk_img("docker.io/library/retagged:dev", "sha256:working-mirror"),
                ),
                // 2. Source deleted concurrently from moby while retiring for a known source digest.
                (
                    ("k8s.io".into(), "docker.io/library/vanished:dev".into()),
                    mk_img("docker.io/library/vanished:dev", "sha256:working-mirror"),
                ),
                // 3. Source still incomplete in moby AND k8s mirror unchanged -> retire!
                (
                    ("moby".into(), "docker.io/library/stale:dev".into()),
                    mk_img("docker.io/library/stale:dev", "sha256:bad-incomplete"),
                ),
                (
                    ("k8s.io".into(), "docker.io/library/stale:dev".into()),
                    mk_img("docker.io/library/stale:dev", "sha256:old-mirror"),
                ),
                // 4. Source still incomplete, but kubelet replaced k8s tag with native pull.
                (
                    ("moby".into(), "docker.io/library/kubelet:dev".into()),
                    mk_img("docker.io/library/kubelet:dev", "sha256:bad-incomplete"),
                ),
                (
                    ("k8s.io".into(), "docker.io/library/kubelet:dev".into()),
                    mk_img("docker.io/library/kubelet:dev", "sha256:native-pull"),
                ),
                // 5. Orphan cleanup: absent from moby and unchanged in k8s -> delete!
                (
                    ("k8s.io".into(), "docker.io/library/orphan:dev".into()),
                    mk_img("docker.io/library/orphan:dev", "sha256:orphan-mirror"),
                ),
                // 6. Orphan cleanup raced with tag recreation in moby -> preserve!
                (
                    ("moby".into(), "docker.io/library/reborn:dev".into()),
                    mk_img("docker.io/library/reborn:dev", "sha256:new-native"),
                ),
                (
                    ("k8s.io".into(), "docker.io/library/reborn:dev".into()),
                    mk_img("docker.io/library/reborn:dev", "sha256:old-mirror"),
                ),
                // 7. RPC failure checking moby -> propagate error and preserve k8s mirror.
                (
                    ("k8s.io".into(), "docker.io/library/err:dev".into()),
                    mk_img("docker.io/library/err:dev", "sha256:keep-mirror"),
                ),
            ]),
            fail_get_image: Some("docker.io/library/err:dev".into()),
            ..Default::default()
        }));

        let server = spawn_mock_containerd(listener, Arc::clone(&state), 7, 65_535);

        // 1. Concurrent retag in moby -> SourceChanged, working mirror untouched.
        assert_eq!(
            retire_image_if_unchanged(
                &sock,
                "docker.io/library/retagged:dev",
                Some("sha256:bad-incomplete"),
                "sha256:working-mirror",
                Duration::from_secs(5),
            )
            .unwrap(),
            RetireOutcome::SourceChanged
        );
        // 2. Concurrent delete in moby -> SourceChanged, mirror preserved for relist.
        assert_eq!(
            retire_image_if_unchanged(
                &sock,
                "docker.io/library/vanished:dev",
                Some("sha256:bad-incomplete"),
                "sha256:working-mirror",
                Duration::from_secs(5),
            )
            .unwrap(),
            RetireOutcome::SourceChanged
        );
        // 3. Unchanged moby + unchanged k8s -> Deleted.
        assert_eq!(
            retire_image_if_unchanged(
                &sock,
                "docker.io/library/stale:dev",
                Some("sha256:bad-incomplete"),
                "sha256:old-mirror",
                Duration::from_secs(5),
            )
            .unwrap(),
            RetireOutcome::Deleted
        );
        // 4. Unchanged moby + replaced k8s -> NotFoundOrChanged, native pull untouched.
        assert_eq!(
            retire_image_if_unchanged(
                &sock,
                "docker.io/library/kubelet:dev",
                Some("sha256:bad-incomplete"),
                "sha256:old-mirror",
                Duration::from_secs(5),
            )
            .unwrap(),
            RetireOutcome::NotFoundOrChanged
        );
        // 5. Orphan still absent in moby -> Deleted.
        assert_eq!(
            retire_image_if_unchanged(
                &sock,
                "docker.io/library/orphan:dev",
                None,
                "sha256:orphan-mirror",
                Duration::from_secs(5),
            )
            .unwrap(),
            RetireOutcome::Deleted
        );
        // 6. Orphan recreated in moby -> SourceChanged, k8s untouched.
        assert_eq!(
            retire_image_if_unchanged(
                &sock,
                "docker.io/library/reborn:dev",
                None,
                "sha256:old-mirror",
                Duration::from_secs(5),
            )
            .unwrap(),
            RetireOutcome::SourceChanged
        );
        // 7. RPC error checking moby -> Err, k8s untouched.
        let err = retire_image_if_unchanged(
            &sock,
            "docker.io/library/err:dev",
            Some("sha256:bad-incomplete"),
            "sha256:keep-mirror",
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(err.contains("grpc-status 13"), "{err}");

        {
            let st = state.lock().unwrap();
            assert!(
                st.images
                    .contains_key(&("k8s.io".into(), "docker.io/library/retagged:dev".into())),
                "working mirror must survive concurrent moby retag"
            );
            assert!(
                st.images
                    .contains_key(&("k8s.io".into(), "docker.io/library/vanished:dev".into()))
            );
            assert!(
                !st.images
                    .contains_key(&("k8s.io".into(), "docker.io/library/stale:dev".into()))
            );
            assert_eq!(
                st.images[&("k8s.io".into(), "docker.io/library/kubelet:dev".into())]
                    .target
                    .digest,
                "sha256:native-pull"
            );
            assert!(
                !st.images
                    .contains_key(&("k8s.io".into(), "docker.io/library/orphan:dev".into()))
            );
            assert!(
                st.images
                    .contains_key(&("k8s.io".into(), "docker.io/library/reborn:dev".into()))
            );
            assert!(
                st.images
                    .contains_key(&("k8s.io".into(), "docker.io/library/err:dev".into()))
            );
        }

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: when replacing an existing `k8s.io` mirror and
    /// `Images.Delete` commits but the following `Images.Create` fails,
    /// `publish_rewritten_image` pins the old target under the lease before
    /// deleting and restores the full previous image record (target,
    /// annotations, and all labels) over a fresh bounded create-only connection
    /// so pods never lose a previously working tag.
    #[test]
    fn publish_rewritten_image_restores_old_tag_when_replacement_create_fails() {
        let (dir, sock) = temp_sock("restore");
        let listener = UnixListener::bind(&sock).unwrap();

        let layer_digest = format!("sha256:{}", "7".repeat(64));
        let old_digest = format!("sha256:{}", "8".repeat(64));
        let plan = sample_plan(
            &layer_digest,
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );
        let old_image = ContainerdImage {
            name: "docker.io/library/app:dev".into(),
            labels: HashMap::from([
                ("io.cri-containerd.image".into(), "managed".into()),
                (
                    "io.katamaran.k8s.source-digest".into(),
                    "sha256:prev-source".into(),
                ),
            ]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: old_digest.clone(),
                size: 640,
                annotations: HashMap::from([(
                    "org.opencontainers.image.ref.name".into(),
                    "dev".into(),
                )]),
            },
        };

        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(layer_digest.clone(), 4096)]),
            ns_content: HashMap::from([(("moby".into(), layer_digest), (4096, HashMap::new()))]),
            images: HashMap::from([
                (
                    ("moby".into(), "docker.io/library/app:dev".into()),
                    ContainerdImage {
                        name: "docker.io/library/app:dev".into(),
                        labels: HashMap::new(),
                        target: Descriptor {
                            media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                            digest: "sha256:source-amd64".into(),
                            size: 128,
                            ..Default::default()
                        },
                    },
                ),
                (
                    ("k8s.io".into(), "docker.io/library/app:dev".into()),
                    old_image.clone(),
                ),
            ]),
            remaining_disk_bytes: 1 << 20,
            fail_next_k8s_create: true,
            ..Default::default()
        }));

        // Connection 1: publish_rewritten_image (delete commits, create fails)
        // Connection 2: recovery fresh dial (Images.Get -> NOT_FOUND -> Images.Create restores old_image)
        let server = spawn_mock_containerd(listener, Arc::clone(&state), 2, 65_535);

        let err = publish_rewritten_image(
            &sock,
            "docker.io/library/app:dev",
            "io.katamaran.k8s.source-digest",
            Some(&old_digest),
            &plan,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(
            matches!(err, PublishError::Failed(ref msg) if msg.contains("simulated create failure")),
            "expected simulated create failure, got {err:?}"
        );

        let st = state.lock().unwrap();
        assert_eq!(st.images_update_calls, 0);
        assert!(
            st.leases.is_empty(),
            "temporary lease must be deleted after recovery completes"
        );
        assert!(
            st.lease_resources
                .iter()
                .any(|(ns, _, typ, id)| ns == "k8s.io" && typ == "content" && id == &old_digest),
            "old target content must be pinned under lease before deleting: {:?}",
            st.lease_resources
        );
        assert_eq!(
            st.images
                .get(&("k8s.io".into(), "docker.io/library/app:dev".into())),
            Some(&old_image),
            "previous k8s.io tag (full target, annotations, and labels) must be restored"
        );
        drop(st);

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: if `Images.Delete`'s outcome is uncertain (connection
    /// drops after delete commits) AND a competing writer (e.g. a native
    /// kubelet pull) creates a new tag in `k8s.io`, recovery checks `k8s.io`
    /// over a fresh connection and leaves the competing tag untouched.
    #[test]
    fn publish_rewritten_image_recovery_preserves_competing_writer_after_uncertain_delete() {
        let (dir, sock) = temp_sock("uncertain");
        let listener = UnixListener::bind(&sock).unwrap();

        let layer_digest = format!("sha256:{}", "5".repeat(64));
        let old_digest = format!("sha256:{}", "6".repeat(64));
        let plan = sample_plan(
            &layer_digest,
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );
        let old_image = ContainerdImage {
            name: "docker.io/library/app:dev".into(),
            labels: HashMap::from([(
                "io.katamaran.k8s.source-digest".into(),
                "sha256:prev-source".into(),
            )]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: old_digest.clone(),
                size: 512,
                ..Default::default()
            },
        };
        let competing = ContainerdImage {
            name: "docker.io/library/app:dev".into(),
            labels: HashMap::from([("io.cri-containerd.image".into(), "managed".into())]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:competing-native-arm64".into(),
                size: 768,
                ..Default::default()
            },
        };

        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(layer_digest.clone(), 4096)]),
            ns_content: HashMap::from([(("moby".into(), layer_digest), (4096, HashMap::new()))]),
            images: HashMap::from([
                (
                    ("moby".into(), "docker.io/library/app:dev".into()),
                    ContainerdImage {
                        name: "docker.io/library/app:dev".into(),
                        labels: HashMap::new(),
                        target: Descriptor {
                            media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                            digest: "sha256:source-amd64".into(),
                            size: 128,
                            ..Default::default()
                        },
                    },
                ),
                (
                    ("k8s.io".into(), "docker.io/library/app:dev".into()),
                    old_image,
                ),
            ]),
            remaining_disk_bytes: 1 << 20,
            drop_after_k8s_delete: Some(Some(competing.clone())),
            ..Default::default()
        }));

        // Connection 1: publish (drops after delete commits, inserting competing image)
        // Connection 2: fresh recovery connection (Images.Get sees competing image, leaves untouched, deletes lease)
        let server = spawn_mock_containerd(listener, Arc::clone(&state), 2, 65_535);

        let err = publish_rewritten_image(
            &sock,
            "docker.io/library/app:dev",
            "io.katamaran.k8s.source-digest",
            Some(&old_digest),
            &plan,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(matches!(err, PublishError::Failed(_)), "{err:?}");

        let st = state.lock().unwrap();
        assert_eq!(st.images_update_calls, 0);
        assert!(st.leases.is_empty());
        assert_eq!(
            st.images
                .get(&("k8s.io".into(), "docker.io/library/app:dev".into())),
            Some(&competing),
            "recovery must never overwrite a competing writer's tag"
        );
        drop(st);

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: if `Images.Delete` returns an uncertain error (socket
    /// dropped) while the delete is STILL in flight on containerd and commits
    /// AFTER recovery's first `Images.Get` saw `old_image`, recovery must NOT
    /// return early or release the lease while the delete can still commit —
    /// it waits out the bounded `grpc-timeout` window + BoltDB write barrier,
    /// catches the delayed deletion, and restores the full `old_image` record.
    #[test]
    fn publish_rewritten_image_recovery_waits_for_delayed_uncertain_delete_and_restores_old_tag() {
        let (dir, sock) = temp_sock("delayed");
        let listener = UnixListener::bind(&sock).unwrap();

        let layer_digest = format!("sha256:{}", "3".repeat(64));
        let old_digest = format!("sha256:{}", "4".repeat(64));
        let plan = sample_plan(
            &layer_digest,
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );
        let old_image = ContainerdImage {
            name: "docker.io/library/app:dev".into(),
            labels: HashMap::from([
                ("io.cri-containerd.image".into(), "managed".into()),
                (
                    "io.katamaran.k8s.source-digest".into(),
                    "sha256:prev-source".into(),
                ),
            ]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: old_digest.clone(),
                size: 640,
                annotations: HashMap::from([(
                    "org.opencontainers.image.ref.name".into(),
                    "dev".into(),
                )]),
            },
        };

        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(layer_digest.clone(), 4096)]),
            ns_content: HashMap::from([(("moby".into(), layer_digest), (4096, HashMap::new()))]),
            images: HashMap::from([
                (
                    ("moby".into(), "docker.io/library/app:dev".into()),
                    ContainerdImage {
                        name: "docker.io/library/app:dev".into(),
                        labels: HashMap::new(),
                        target: Descriptor {
                            media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                            digest: "sha256:source-amd64".into(),
                            size: 128,
                            ..Default::default()
                        },
                    },
                ),
                (
                    ("k8s.io".into(), "docker.io/library/app:dev".into()),
                    old_image.clone(),
                ),
            ]),
            remaining_disk_bytes: 1 << 20,
            // Commit the delete 120ms AFTER dropping Connection 1 (well after
            // recovery's first `Images.Get` sees `old_image`, well within the
            // 350ms delete RPC timeout window).
            delayed_k8s_delete: Some(Duration::from_millis(120)),
            ..Default::default()
        }));

        // Connection 1: publish (drops on Images.Delete while delayed delete is in flight)
        // Connection 2: recovery polls through delayed delete, restores old_image, runs
        //               post-deadline Leases.AddResource barrier, and deletes lease.
        let server = spawn_mock_containerd(listener, Arc::clone(&state), 2, 65_535);

        let err = publish_rewritten_image_with_delete_timeout(
            &sock,
            "docker.io/library/app:dev",
            "io.katamaran.k8s.source-digest",
            Some(&old_digest),
            &plan,
            Duration::from_secs(5),
            Duration::from_millis(350),
        )
        .unwrap_err();
        assert!(matches!(err, PublishError::Failed(_)), "{err:?}");

        let st = state.lock().unwrap();
        assert!(
            st.k8s_delete_committed,
            "simulated delayed delete must have committed during recovery"
        );
        assert!(
            st.k8s_get_calls >= 3,
            "recovery must poll past initial Some(old_image) until delete settles, got {} calls",
            st.k8s_get_calls
        );
        let timeout_ms: u64 = st
            .last_delete_grpc_timeout
            .as_deref()
            .and_then(|s| s.strip_suffix('m'))
            .and_then(|n| n.parse().ok())
            .expect("Images.Delete must advertise tight per-RPC grpc-timeout in ms");
        assert!(
            (1..=350).contains(&timeout_ms),
            "Images.Delete grpc-timeout should be <= 350m, got {timeout_ms}m"
        );
        assert!(
            st.lease_resources
                .iter()
                .filter(|(ns, _, typ, id)| ns == "k8s.io" && typ == "content" && id == &old_digest)
                .count()
                >= 2,
            "expected both pre-delete pin and post-deadline BoltDB write barrier AddResource calls: {:?}",
            st.lease_resources
        );
        assert!(
            st.leases.is_empty(),
            "temporary lease should be deleted once recovery finishes cleanly"
        );
        assert_eq!(
            st.images
                .get(&("k8s.io".into(), "docker.io/library/app:dev".into())),
            Some(&old_image),
            "delayed uncertain delete must not leave the old k8s.io tag missing"
        );
        drop(st);

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression test: if recovery itself fails after deleting an existing tag,
    /// `publish_rewritten_image` MUST NOT delete the temporary lease (leaving
    /// `old_image.target.digest` pinned until `gc.expire`) and must report the
    /// recovery error.
    #[test]
    fn publish_rewritten_image_keeps_lease_pinned_when_recovery_fails() {
        let (dir, sock) = temp_sock("keep-lease");
        let listener = UnixListener::bind(&sock).unwrap();

        let layer_digest = format!("sha256:{}", "1".repeat(64));
        let old_digest = format!("sha256:{}", "2".repeat(64));
        let plan = sample_plan(
            &layer_digest,
            br#"{"architecture":"arm64","os":"linux"}"#.to_vec(),
        );
        let old_image = ContainerdImage {
            name: "docker.io/library/app:dev".into(),
            labels: HashMap::from([(
                "io.katamaran.k8s.source-digest".into(),
                "sha256:prev-source".into(),
            )]),
            target: Descriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: old_digest.clone(),
                size: 512,
                ..Default::default()
            },
        };

        let state = Arc::new(Mutex::new(MockContainerdState {
            shared_store: HashMap::from([(layer_digest.clone(), 4096)]),
            ns_content: HashMap::from([(("moby".into(), layer_digest), (4096, HashMap::new()))]),
            images: HashMap::from([
                (
                    ("moby".into(), "docker.io/library/app:dev".into()),
                    ContainerdImage {
                        name: "docker.io/library/app:dev".into(),
                        labels: HashMap::new(),
                        target: Descriptor {
                            media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                            digest: "sha256:source-amd64".into(),
                            size: 128,
                            ..Default::default()
                        },
                    },
                ),
                (
                    ("k8s.io".into(), "docker.io/library/app:dev".into()),
                    old_image,
                ),
            ]),
            remaining_disk_bytes: 1 << 20,
            fail_next_k8s_create: true,
            fail_k8s_get_after_delete: true,
            ..Default::default()
        }));

        let server = spawn_mock_containerd(listener, Arc::clone(&state), 2, 65_535);
        let err = publish_rewritten_image(
            &sock,
            "docker.io/library/app:dev",
            "io.katamaran.k8s.source-digest",
            Some(&old_digest),
            &plan,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(
            matches!(err, PublishError::Failed(ref msg) if msg.contains("failed to restore previous tag")),
            "expected recovery failure detail, got {err:?}"
        );

        let st = state.lock().unwrap();
        assert_eq!(
            st.leases.len(),
            1,
            "temporary lease must stay pinned when recovery fails so GC cannot collect old target"
        );
        drop(st);

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }
}
