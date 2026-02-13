//! Input event capture from local devices
//!
//! Captures keyboard and mouse events from /dev/input/event* devices
//! and converts them to protocol events for network transmission.
//!
//! Uses Scroll Lock as a toggle hotkey to switch between local and remote mode.

use anyhow::{Context, Result};
use cosmic_kvm_protocol::{
    InputEvent, KeyboardEvent, ModifierState, MouseButtonEvent, MouseMoveEvent, MouseMovement,
    MouseWheelEvent,
};
use evdev::{Device, EventType, InputEventKind, Key};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// The hotkey used to toggle between local and remote mode
const TOGGLE_KEY: Key = Key::KEY_SCROLLLOCK;

/// Input capture manager
pub struct InputCapture {
    devices: Vec<Device>,
    sender: broadcast::Sender<InputEvent>,
    /// Shared state: true = remote mode (grabbed), false = local mode (ungrabbed)
    remote_mode: Arc<AtomicBool>,
}

impl InputCapture {
    /// Create a new input capture manager
    pub fn new() -> Result<(Self, broadcast::Receiver<InputEvent>)> {
        let (sender, receiver) = broadcast::channel(1024);

        let capture = Self {
            devices: Vec::new(),
            sender,
            remote_mode: Arc::new(AtomicBool::new(false)),
        };

        Ok((capture, receiver))
    }

    /// Add a device to capture from
    pub fn add_device<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let device = Device::open(path.as_ref())
            .with_context(|| format!("Failed to open device: {:?}", path.as_ref()))?;

        info!(
            "Added capture device: {} ({})",
            device.name().unwrap_or("unknown"),
            path.as_ref().display()
        );

        self.devices.push(device);
        Ok(())
    }

    /// Auto-detect and add all keyboard and mouse devices
    pub fn add_all_devices(&mut self) -> Result<()> {
        let input_dir = Path::new("/dev/input");

        if !input_dir.exists() {
            anyhow::bail!("/dev/input directory not found");
        }

        // Scan for eventN devices
        for entry in std::fs::read_dir(input_dir)? {
            let entry = entry?;
            let path = entry.path();

            // Only process eventN files
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("event") {
                    // Try to open device to check if it's readable
                    match Device::open(&path) {
                        Ok(device) => {
                            // Check if device is keyboard or mouse
                            if is_keyboard_or_mouse(&device) {
                                info!(
                                    "Auto-detected device: {} at {}",
                                    device.name().unwrap_or("unknown"),
                                    path.display()
                                );
                                self.devices.push(device);
                            }
                        }
                        Err(e) => {
                            debug!("Skipping {}: {}", path.display(), e);
                        }
                    }
                }
            }
        }

        if self.devices.is_empty() {
            warn!("No input devices found. Make sure you have permission to access /dev/input/event*");
        } else {
            info!("Added {} input devices", self.devices.len());
        }

        Ok(())
    }

    /// Start capturing events
    pub async fn run(self) -> Result<()> {
        if self.devices.is_empty() {
            anyhow::bail!("No devices to capture from. Add devices first.");
        }

        info!("Starting input capture from {} devices", self.devices.len());
        info!("Press Scroll Lock to toggle between local and remote mode");
        info!("Currently in LOCAL mode (input stays on this machine)");

        // Create async tasks for each device
        let mut handles = Vec::new();

        for device in self.devices {
            let sender = self.sender.clone();
            let remote_mode = Arc::clone(&self.remote_mode);
            let handle = tokio::task::spawn_blocking(move || {
                Self::capture_device(device, sender, remote_mode)
            });
            handles.push(handle);
        }

        // Wait for all capture tasks
        for handle in handles {
            if let Err(e) = handle.await {
                warn!("Capture task failed: {}", e);
            }
        }

        Ok(())
    }

    /// Capture events from a single device (runs in blocking thread)
    fn capture_device(
        mut device: Device,
        sender: broadcast::Sender<InputEvent>,
        remote_mode: Arc<AtomicBool>,
    ) -> Result<()> {
        let device_name = device.name().unwrap_or("unknown").to_string();
        debug!("Capturing from device: {}", device_name);

        // Start ungrabbed (local mode)
        let mut is_grabbed = false;

        loop {
            // Fetch events (blocking call) — collect to release borrow on device
            let events: Vec<evdev::InputEvent> = match device.fetch_events() {
                Ok(events) => events.collect(),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                        continue;
                    }
                    warn!("Error reading from {}: {}", device_name, e);
                    break;
                }
            };

            let mut toggle_requested = false;

            for event in &events {
                // Log all key events at debug level so we can diagnose
                if let InputEventKind::Key(key) = event.kind() {
                    debug!("[{}] Key event: {:?} value={}", device_name, key, event.value());

                    // Check for toggle hotkey (key press, value=1)
                    if key == TOGGLE_KEY && event.value() == 1 {
                        info!("[{}] Toggle hotkey detected!", device_name);
                        toggle_requested = true;
                        continue;
                    }
                }

                // Only forward events when in remote mode
                if remote_mode.load(Ordering::Relaxed) {
                    if let Some(protocol_event) = convert_event(event) {
                        let _ = sender.send(protocol_event);
                    }
                }
            }

            // Handle toggle after processing all events (borrow on device is released)
            if toggle_requested {
                let was_remote = remote_mode.load(Ordering::SeqCst);
                let new_mode = !was_remote;
                remote_mode.store(new_mode, Ordering::SeqCst);

                if new_mode {
                    if !is_grabbed {
                        if let Err(e) = device.grab() {
                            warn!("Failed to grab {}: {}", device_name, e);
                        } else {
                            is_grabbed = true;
                        }
                    }
                    info!(">> REMOTE mode: input goes to client (toggled via {})", device_name);
                } else {
                    if is_grabbed {
                        if let Err(e) = device.ungrab() {
                            warn!("Failed to ungrab {}: {}", device_name, e);
                        } else {
                            is_grabbed = false;
                        }
                    }
                    info!("<< LOCAL mode: input stays on this machine (toggled via {})", device_name);
                }
            }

            // Sync grab state for devices that didn't receive the toggle key
            let should_grab = remote_mode.load(Ordering::Relaxed);
            if should_grab && !is_grabbed {
                if let Err(e) = device.grab() {
                    warn!("Failed to grab {}: {}", device_name, e);
                } else {
                    is_grabbed = true;
                    debug!("Grabbed {}", device_name);
                }
            } else if !should_grab && is_grabbed {
                if let Err(e) = device.ungrab() {
                    warn!("Failed to ungrab {}: {}", device_name, e);
                } else {
                    is_grabbed = false;
                    debug!("Ungrabbed {}", device_name);
                }
            }
        }

        // Release exclusive access on exit
        if is_grabbed {
            if let Err(e) = device.ungrab() {
                warn!("Failed to ungrab device {}: {}", device_name, e);
            } else {
                debug!("Released device: {}", device_name);
            }
        }

        Ok(())
    }
}

/// Check if device is a keyboard or mouse
fn is_keyboard_or_mouse(device: &Device) -> bool {
    let supported_events = device.supported_events();

    // Check if device supports KEY events (keyboard or mouse buttons)
    if supported_events.contains(EventType::KEY) {
        // Try to determine if it's a keyboard or mouse by checking for common keys
        if let Some(keys) = device.supported_keys() {
            // Has keyboard keys
            if keys.contains(Key::KEY_A)
                || keys.contains(Key::KEY_ENTER)
                || keys.contains(Key::KEY_SPACE)
            {
                return true;
            }

            // Has mouse buttons
            if keys.contains(Key::BTN_LEFT)
                || keys.contains(Key::BTN_RIGHT)
                || keys.contains(Key::BTN_MIDDLE)
            {
                return true;
            }
        }
    }

    // Check for mouse movement
    if supported_events.contains(EventType::RELATIVE) {
        return true;
    }

    false
}

/// Convert evdev event to protocol event
fn convert_event(event: &evdev::InputEvent) -> Option<InputEvent> {
    let time = event
        .timestamp()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    match event.kind() {
        InputEventKind::Key(key) => {
            // Keyboard or mouse button
            if is_mouse_button(key) {
                Some(InputEvent::MouseButton(MouseButtonEvent {
                    time,
                    button: key.code() as u32,
                    pressed: event.value() == 1,
                }))
            } else {
                // Only send press (1) and release (0), skip repeat (2)
                // Client kernel handles its own autorepeat
                if event.value() == 2 {
                    None
                } else {
                    Some(InputEvent::Keyboard(KeyboardEvent {
                        time,
                        key: key.code() as u32,
                        pressed: event.value() == 1,
                        raw_value: event.value(),
                        modifiers: ModifierState::default(),
                    }))
                }
            }
        }
        InputEventKind::RelAxis(axis) => {
            // Mouse movement or wheel
            use evdev::RelativeAxisType;

            match axis {
                RelativeAxisType::REL_X | RelativeAxisType::REL_Y => {
                    // Mouse movement - we need to accumulate X and Y
                    // For now, send individual axis movements
                    // TODO: Batch X and Y together
                    let (dx, dy) = if axis == RelativeAxisType::REL_X {
                        (event.value() as f64, 0.0)
                    } else {
                        (0.0, event.value() as f64)
                    };

                    Some(InputEvent::MouseMove(MouseMoveEvent {
                        time,
                        movement: MouseMovement::Relative { dx, dy },
                    }))
                }
                RelativeAxisType::REL_WHEEL | RelativeAxisType::REL_HWHEEL => {
                    let (dx, dy) = if axis == RelativeAxisType::REL_HWHEEL {
                        (event.value() as f64, 0.0)
                    } else {
                        (0.0, event.value() as f64)
                    };

                    Some(InputEvent::MouseWheel(MouseWheelEvent { time, dx, dy }))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Check if key is a mouse button
fn is_mouse_button(key: Key) -> bool {
    matches!(
        key,
        Key::BTN_LEFT
            | Key::BTN_RIGHT
            | Key::BTN_MIDDLE
            | Key::BTN_SIDE
            | Key::BTN_EXTRA
            | Key::BTN_FORWARD
            | Key::BTN_BACK
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_input_capture_creation() {
        let result = InputCapture::new();
        assert!(result.is_ok());
    }
}
