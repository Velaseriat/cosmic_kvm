//! Client implementation
//!
//! Connects to a KVM server, receives input events, and injects them locally

use crate::clipboard::ClipboardManager;
use crate::config::Config;
use anyhow::{Context, Result};
use cosmic_kvm_input::InputManager;
use cosmic_kvm_protocol::{
    AcceptMessage, Capabilities, HandshakeMessage, HelloMessage, Message,
    Payload,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

pub struct Client {
    config: Config,
    server_addr: String,
}

impl Client {
    pub fn new(config: Config, server_addr: String) -> Self {
        Self {
            config,
            server_addr,
        }
    }

    pub async fn run(self) -> Result<()> {
        info!("Connecting to server at {}", self.server_addr);

        // Connect to server
        let stream = TcpStream::connect(&self.server_addr)
            .await
            .with_context(|| format!("Failed to connect to {}", self.server_addr))?;

        info!("Connected to server");

        // For now, skip TLS and use plain TCP
        // TODO: Add TLS support later
        let mut connection = PlainConnection { stream };

        // Perform handshake
        self.handshake(&mut connection).await?;

        // Initialize input manager for injection
        let mut input_manager = InputManager::new()
            .context("Failed to initialize input manager. Do you have permission to access /dev/uinput?")?;

        // Initialize clipboard sync
        let (clipboard_mgr, clipboard_rx) = match ClipboardManager::new().await {
            Ok((mgr, rx)) => (Some(mgr), Some(rx)),
            Err(e) => {
                warn!("Clipboard sync unavailable: {}. Continuing without it.", e);
                (None, None)
            }
        };

        info!("Input injection ready. Waiting for events from server...");

        // Main event loop
        self.event_loop(&mut connection, &mut input_manager, clipboard_mgr, clipboard_rx)
            .await?;

        Ok(())
    }

    async fn handshake(&self, connection: &mut PlainConnection) -> Result<AcceptMessage> {
        // Send Hello
        let hello = Message::new(Payload::Handshake(HandshakeMessage::Hello(HelloMessage {
            protocol_version: cosmic_kvm_protocol::PROTOCOL_VERSION.to_string(),
            device_name: self.config.device_name.clone(),
            device_id: self.config.device_id.clone(),
            capabilities: Capabilities::default(),
        })));

        connection.send(&hello).await?;
        debug!("Sent Hello message");

        // Receive Challenge
        let response = connection.receive().await?;
        let challenge = match response.payload {
            Payload::Handshake(HandshakeMessage::Challenge(c)) => c,
            Payload::Handshake(HandshakeMessage::Reject(r)) => {
                anyhow::bail!("Server rejected connection: {}", r.reason);
            }
            _ => anyhow::bail!("Expected Challenge, got {:?}", response.payload),
        };

        debug!("Received challenge with fingerprint: {}", challenge.cert_fingerprint);

        // Send Response
        // For now, simple response without actual crypto
        // TODO: Implement proper challenge-response
        let response_msg = Message::new(Payload::Handshake(HandshakeMessage::Response(
            cosmic_kvm_protocol::ResponseMessage {
                signature: challenge.nonce.clone(), // Just echo for now
                cert_fingerprint: "client-fingerprint".to_string(),
            },
        )));

        connection.send(&response_msg).await?;
        debug!("Sent Response message");

        // Receive Accept or Reject
        let result = connection.receive().await?;
        match result.payload {
            Payload::Handshake(HandshakeMessage::Accept(accept)) => {
                info!(
                    "Successfully authenticated! Session ID: {}",
                    accept.session_id
                );
                Ok(accept)
            }
            Payload::Handshake(HandshakeMessage::Reject(reject)) => {
                anyhow::bail!("Authentication failed: {}", reject.reason);
            }
            _ => anyhow::bail!("Expected Accept/Reject, got {:?}", result.payload),
        }
    }

    async fn event_loop(
        &self,
        connection: &mut PlainConnection,
        input_manager: &mut InputManager,
        clipboard_mgr: Option<ClipboardManager>,
        clipboard_rx: Option<tokio::sync::mpsc::Receiver<cosmic_kvm_protocol::ClipboardData>>,
    ) -> Result<()> {
        let mut event_count = 0u64;
        let mut clipboard_rx = clipboard_rx;

        loop {
            tokio::select! {
                // Receive messages from server
                msg = connection.receive() => {
                    let message = match msg {
                        Ok(msg) => msg,
                        Err(e) => {
                            error!("Connection error: {}", e);
                            break;
                        }
                    };

                    match message.payload {
                        Payload::Input(event) => {
                            event_count += 1;

                            // Log ALL input events for debugging
                            match &event {
                                cosmic_kvm_protocol::InputEvent::Keyboard(kb) => {
                                    info!("KB: key={} pressed={} raw={}", kb.key, kb.pressed, kb.raw_value);
                                }
                                cosmic_kvm_protocol::InputEvent::MouseButton(mb) => {
                                    debug!("BTN: button={} pressed={}", mb.button, mb.pressed);
                                }
                                cosmic_kvm_protocol::InputEvent::MouseMove(_) => {}  // too noisy
                                cosmic_kvm_protocol::InputEvent::MouseWheel(w) => {
                                    debug!("WHEEL: dx={} dy={}", w.dx, w.dy);
                                }
                            }

                            if let Err(e) = input_manager.inject(&event) {
                                warn!("Failed to inject event: {}", e);
                            } else if event_count % 1000 == 0 {
                                debug!("Injected {} events", event_count);
                            }
                        }
                        Payload::Clipboard(data) => {
                            debug!("Received clipboard from server: {} ({} bytes)", data.mime_type, data.data.len());
                            if let Some(ref mgr) = clipboard_mgr {
                                if let Err(e) = mgr.set_clipboard(&data).await {
                                    warn!("Failed to set local clipboard: {}", e);
                                }
                            }
                        }
                        Payload::Control(control) => {
                            use cosmic_kvm_protocol::ControlMessage;
                            match control {
                                ControlMessage::Disconnect => {
                                    info!("Server requested disconnect");
                                    break;
                                }
                                ControlMessage::Error(msg) => {
                                    error!("Server error: {}", msg);
                                }
                                _ => {
                                    debug!("Received control message: {:?}", control);
                                }
                            }
                        }
                        Payload::Heartbeat => {
                            debug!("Received heartbeat");
                            let _ = connection.send(&Message::new(Payload::Heartbeat)).await;
                        }
                        _ => {
                            warn!("Unexpected message: {:?}", message.payload);
                        }
                    }
                }

                // Send local clipboard changes back to server
                clip = async {
                    match clipboard_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(data) = clip {
                        debug!("Sending clipboard to server: {} ({} bytes)", data.mime_type, data.data.len());
                        let message = Message::new(Payload::Clipboard(data));
                        if let Err(e) = connection.send(&message).await {
                            error!("Failed to send clipboard to server: {}", e);
                            break;
                        }
                    }
                }
            }
        }

        info!("Client event loop ended. Total events injected: {}", event_count);
        Ok(())
    }
}

/// Plain TCP connection (no TLS for now)
struct PlainConnection {
    stream: TcpStream,
}

impl PlainConnection {
    async fn send(&mut self, message: &Message) -> Result<()> {
        let bytes = message.to_bytes()?;
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Message> {
        // Read magic bytes and length
        let mut header = [0u8; 8];
        self.stream.read_exact(&mut header).await?;

        let length = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;

        // Validate length
        if length > cosmic_kvm_protocol::MAX_MESSAGE_SIZE {
            anyhow::bail!("Message too large: {} bytes", length);
        }

        // Read message body
        let mut body = vec![0u8; length];
        self.stream.read_exact(&mut body).await?;

        // Reconstruct full message
        let mut full_message = header.to_vec();
        full_message.extend_from_slice(&body);

        Ok(Message::from_bytes(&full_message)?)
    }
}
