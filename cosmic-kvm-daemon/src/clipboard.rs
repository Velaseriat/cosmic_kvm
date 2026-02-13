//! Clipboard synchronization
//!
//! Monitors the local Wayland clipboard using wl-paste and syncs
//! clipboard contents between server and client via the protocol.

use anyhow::{Context, Result};
use cosmic_kvm_protocol::ClipboardData;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Clipboard manager that watches for changes and allows setting clipboard
pub struct ClipboardManager {
    /// Channel to send outgoing clipboard changes
    outgoing_tx: mpsc::Sender<ClipboardData>,
    /// Flag to suppress echoed clipboard sets (prevent loops)
    suppress_next: Arc<AtomicBool>,
}

impl ClipboardManager {
    /// Start clipboard monitoring. Returns the manager and a receiver for clipboard changes.
    pub async fn new() -> Result<(Self, mpsc::Receiver<ClipboardData>)> {
        // Check that wl-paste is available
        Command::new("wl-paste")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .context("wl-paste not found. Install wl-clipboard: sudo apt install wl-clipboard")?;

        let (outgoing_tx, outgoing_rx) = mpsc::channel(16);
        let suppress_next = Arc::new(AtomicBool::new(false));

        let manager = Self {
            outgoing_tx: outgoing_tx.clone(),
            suppress_next: Arc::clone(&suppress_next),
        };

        // Spawn clipboard watcher
        let suppress = Arc::clone(&suppress_next);
        tokio::spawn(async move {
            if let Err(e) = Self::watch_clipboard(outgoing_tx, suppress).await {
                error!("Clipboard watcher failed: {}", e);
            }
        });

        info!("Clipboard sync initialized");
        Ok((manager, outgoing_rx))
    }

    /// Set the local clipboard to the given data (received from remote)
    pub async fn set_clipboard(&self, data: &ClipboardData) -> Result<()> {
        if data.data.is_empty() {
            debug!("Skipping empty clipboard data");
            return Ok(());
        }

        // Suppress the next watcher event to prevent echo loop
        self.suppress_next.store(true, Ordering::SeqCst);

        let mut child = Command::new("wl-copy")
            .arg("--type")
            .arg(&data.mime_type)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn wl-copy")?;

        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin.write_all(&data.data).await?;
            drop(stdin);
        }

        let status = child.wait().await?;
        if !status.success() {
            warn!("wl-copy exited with status: {}", status);
        } else {
            debug!(
                "Set local clipboard: {} ({} bytes)",
                data.mime_type,
                data.data.len()
            );
        }

        Ok(())
    }

    /// Watch the clipboard for changes using wl-paste --watch
    async fn watch_clipboard(
        tx: mpsc::Sender<ClipboardData>,
        suppress: Arc<AtomicBool>,
    ) -> Result<()> {
        info!("Starting clipboard watcher");

        // Use a polling approach since wl-paste --watch can be finicky
        let mut last_hash: u64 = 0;

        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Check if we should suppress (we just set the clipboard ourselves)
            if suppress.load(Ordering::SeqCst) {
                suppress.store(false, Ordering::SeqCst);
                // Skip this cycle to avoid echo
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }

            // Read current clipboard
            match Self::read_clipboard().await {
                Ok(Some(data)) => {
                    // Simple hash to detect changes
                    let hash = Self::hash_data(&data.data);
                    if hash != last_hash {
                        last_hash = hash;
                        debug!(
                            "Clipboard changed: {} ({} bytes)",
                            data.mime_type,
                            data.data.len()
                        );
                        if tx.send(data).await.is_err() {
                            debug!("Clipboard channel closed");
                            break;
                        }
                    }
                }
                Ok(None) => {
                    // Empty clipboard, reset hash
                    last_hash = 0;
                }
                Err(e) => {
                    debug!("Failed to read clipboard: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Read the current clipboard contents
    async fn read_clipboard() -> Result<Option<ClipboardData>> {
        // First get the MIME type
        let mime_output = Command::new("wl-paste")
            .arg("--list-types")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await?;

        if !mime_output.status.success() {
            return Ok(None);
        }

        let types = String::from_utf8_lossy(&mime_output.stdout);
        let mime_type = types
            .lines()
            .find(|t| {
                t.starts_with("text/")
                    || t.starts_with("image/")
                    || t == &"UTF8_STRING"
                    || t == &"STRING"
            })
            .unwrap_or("text/plain");

        // Prefer text/plain for text content
        let use_type = if types.lines().any(|t| t == "text/plain") {
            "text/plain"
        } else {
            mime_type
        };

        // Read the actual data
        let output = Command::new("wl-paste")
            .arg("--no-newline")
            .arg("--type")
            .arg(use_type)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await?;

        if !output.status.success() || output.stdout.is_empty() {
            return Ok(None);
        }

        // Limit clipboard size to 1MB to avoid huge transfers
        if output.stdout.len() > 1024 * 1024 {
            warn!(
                "Clipboard data too large ({} bytes), skipping",
                output.stdout.len()
            );
            return Ok(None);
        }

        Ok(Some(ClipboardData {
            mime_type: use_type.to_string(),
            data: output.stdout,
        }))
    }

    /// Simple hash for change detection
    fn hash_data(data: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        data.hash(&mut hasher);
        hasher.finish()
    }
}
