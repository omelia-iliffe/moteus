// Copyright 2026 mjbots Robotic Systems, LLC.  info@mjbots.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Single-bus convenience transports.
//!
//! Each type here wraps a single [`TransportDevice`](crate::transport::device::TransportDevice)
//! (or [`AsyncTransportDevice`](crate::transport::device::AsyncTransportDevice)) and
//! implements the [`Transport`](crate::transport::Transport) interface directly, so it
//! can be handed straight to a controller. A [`Router`](crate::transport::Router) exists
//! to multiplex requests across several buses; with only one device there is nothing to
//! route, so these types talk to the device without it.
//!
//! They mirror the Python library's `Fdcanusb`/`PythonCan` convenience
//! transports (which wrap a single `TransportDevice` in a `Transport`). For
//! multi-bus configurations, build a [`Router`](crate::transport::Router)
//! explicitly instead.

use crate::error::Result;
use moteus_protocol::CanFdFrame;
use std::time::Duration;

use crate::transport::transaction::Request;

#[cfg(target_os = "linux")]
use crate::transport::device::TransportDevice;
#[cfg(feature = "tokio")]
use crate::transport::device::AsyncTransportDevice;

/// Implement the blocking [`Transport`](crate::transport::Transport) trait for a
/// newtype `$t` whose single field is a [`TransportDevice`].
///
/// The forwarding is the single-device case the `Router` would otherwise run:
/// a cycle is just the device's transaction, and the only channel is `0`.
#[cfg(target_os = "linux")]
macro_rules! forward_device_transport {
    ($t:ty) => {
        impl $crate::transport::Transport for $t {
            fn cycle(&mut self, requests: &mut [Request]) -> Result<()> {
                let result = self.0.transaction(requests);
                // Stamp every response with our only channel index.
                for req in requests.iter() {
                    for mut frame in req.responses.take() {
                        frame.channel = Some(0);
                        req.responses.push(frame);
                    }
                }
                result
            }
            fn write(&mut self, frame: &CanFdFrame) -> Result<()> {
                self.0.write(frame)
            }
            fn read(&mut self, channel: Option<usize>) -> Result<Option<CanFdFrame>> {
                if let Some(idx) = channel {
                    if idx != 0 {
                        return Err($crate::error::Error::DeviceNotFound(format!(
                            "Channel {} not found",
                            idx
                        )));
                    }
                }
                match self.0.read()? {
                    Some(mut frame) => {
                        frame.channel = Some(0);
                        Ok(Some(frame))
                    }
                    None => Ok(None),
                }
            }
            fn flush_read(&mut self, channel: Option<usize>) -> Result<()> {
                if let Some(idx) = channel {
                    if idx != 0 {
                        return Err($crate::error::Error::DeviceNotFound(format!(
                            "Channel {} not found",
                            idx
                        )));
                    }
                }
                self.0.flush()
            }
            fn set_timeout(&mut self, timeout: Duration) {
                self.0.set_timeout(timeout)
            }
            fn timeout(&self) -> Duration {
                self.0.timeout()
            }
        }
    };
}

/// Implement the async [`AsyncTransport`](crate::transport::async_transport::AsyncTransport)
/// trait for a newtype `$t` whose single field is an [`AsyncTransportDevice`].
///
/// Cancel safety is preserved by calling the device's `recover()` before each
/// transaction, exactly as the `AsyncRouter` does.
#[cfg(feature = "tokio")]
macro_rules! forward_device_async_transport {
    ($t:ty) => {
        impl $crate::transport::async_transport::AsyncTransport for $t {
            fn cycle<'a>(
                &'a mut self,
                requests: &'a mut [Request],
            ) -> $crate::transport::async_transport::BoxFuture<'a, Result<()>> {
                Box::pin(async move {
                    self.0.recover().await?;
                    let result = self.0.transaction(requests).await;
                    for req in requests.iter() {
                        for mut frame in req.responses.take() {
                            frame.channel = Some(0);
                            req.responses.push(frame);
                        }
                    }
                    result
                })
            }
            fn write<'a>(
                &'a mut self,
                frame: &'a CanFdFrame,
            ) -> $crate::transport::async_transport::BoxFuture<'a, Result<()>> {
                Box::pin(self.0.write(frame))
            }
            fn read(
                &mut self,
                channel: Option<usize>,
            ) -> $crate::transport::async_transport::BoxFuture<'_, Result<Option<CanFdFrame>>> {
                Box::pin(async move {
                    if let Some(idx) = channel {
                        if idx != 0 {
                            return Err($crate::error::Error::DeviceNotFound(format!(
                                "Channel {} not found",
                                idx
                            )));
                        }
                    }
                    match self.0.read().await? {
                        Some(mut frame) => {
                            frame.channel = Some(0);
                            Ok(Some(frame))
                        }
                        None => Ok(None),
                    }
                })
            }
            fn flush_read(
                &mut self,
                channel: Option<usize>,
            ) -> $crate::transport::async_transport::BoxFuture<'_, Result<()>> {
                Box::pin(async move {
                    if let Some(idx) = channel {
                        if idx != 0 {
                            return Err($crate::error::Error::DeviceNotFound(format!(
                                "Channel {} not found",
                                idx
                            )));
                        }
                    }
                    self.0.flush().await
                })
            }
        }
    };
}

// ===========================================================================
// Blocking convenience transports (Linux-only: native serial / SocketCAN)
// ===========================================================================

/// A ready-to-use blocking transport over a single fdcanusb.
///
/// ```no_run
/// use moteus::{BlockingController, Fdcanusb};
///
/// # fn main() -> Result<(), moteus::Error> {
/// let mut ctrl = BlockingController::with_transport(1, Fdcanusb::open("/dev/fdcanusb")?);
/// ctrl.set_stop()?;
/// # Ok(())
/// # }
/// ```
#[cfg(target_os = "linux")]
pub struct Fdcanusb(
    crate::transport::fdcanusb::FdcanusbDevice<crate::transport::fdcanusb::LinuxSerialPort>,
);

#[cfg(target_os = "linux")]
impl Fdcanusb {
    /// Open an fdcanusb at the given serial port path.
    pub fn open(path: &str) -> Result<Self> {
        let device = crate::transport::fdcanusb::FdcanusbDevice::new(path)?;
        Ok(Self(device))
    }

    /// Open an fdcanusb with an explicit timeout and BRS setting.
    pub fn with_options(path: &str, timeout: Duration, disable_brs: bool) -> Result<Self> {
        let device =
            crate::transport::fdcanusb::FdcanusbDevice::with_options(path, timeout, disable_brs)?;
        Ok(Self(device))
    }
}

#[cfg(target_os = "linux")]
forward_device_transport!(Fdcanusb);

/// A ready-to-use blocking transport over a single SocketCAN interface.
///
/// ```no_run
/// use moteus::{BlockingController, SocketCan};
///
/// # fn main() -> Result<(), moteus::Error> {
/// let mut ctrl = BlockingController::with_transport(1, SocketCan::open("can0")?);
/// ctrl.set_stop()?;
/// # Ok(())
/// # }
/// ```
#[cfg(target_os = "linux")]
pub struct SocketCan(crate::transport::socketcan::SocketCanDevice);

#[cfg(target_os = "linux")]
impl SocketCan {
    /// Open a SocketCAN interface by name (e.g. `"can0"`).
    pub fn open(interface: &str) -> Result<Self> {
        let device = crate::transport::socketcan::SocketCanDevice::new(interface)?;
        Ok(Self(device))
    }

    /// Open a SocketCAN interface with an explicit timeout and BRS setting.
    pub fn with_options(interface: &str, timeout: Duration, disable_brs: bool) -> Result<Self> {
        let device = crate::transport::socketcan::SocketCanDevice::with_options(
            interface,
            timeout,
            disable_brs,
        )?;
        Ok(Self(device))
    }
}

#[cfg(target_os = "linux")]
forward_device_transport!(SocketCan);

// ===========================================================================
// Async convenience transports
// ===========================================================================

/// A ready-to-use async transport over a single fdcanusb.
///
/// Available on all platforms (uses `tokio-serial`).
#[cfg(feature = "tokio")]
pub struct AsyncFdcanusb(crate::transport::async_fdcanusb::AsyncFdcanusbDevice);

#[cfg(feature = "tokio")]
impl AsyncFdcanusb {
    /// Open an fdcanusb at the given serial port path.
    pub async fn open(path: &str) -> Result<Self> {
        let device = crate::transport::async_fdcanusb::AsyncFdcanusbDevice::open(path).await?;
        Ok(Self(device))
    }

    /// Open an fdcanusb with an explicit BRS setting.
    pub async fn open_with_brs(path: &str, disable_brs: bool) -> Result<Self> {
        let device =
            crate::transport::async_fdcanusb::AsyncFdcanusbDevice::open_with_brs(path, disable_brs)
                .await?;
        Ok(Self(device))
    }
}

#[cfg(feature = "tokio")]
forward_device_async_transport!(AsyncFdcanusb);

/// A ready-to-use async transport over a single SocketCAN interface.
#[cfg(all(feature = "tokio", target_os = "linux"))]
pub struct AsyncSocketCan(crate::transport::async_socketcan::AsyncSocketCanDevice);

#[cfg(all(feature = "tokio", target_os = "linux"))]
impl AsyncSocketCan {
    /// Open a SocketCAN interface by name (e.g. `"can0"`).
    pub async fn open(interface: &str) -> Result<Self> {
        let device =
            crate::transport::async_socketcan::AsyncSocketCanDevice::new(interface).await?;
        Ok(Self(device))
    }
}

#[cfg(all(feature = "tokio", target_os = "linux"))]
forward_device_async_transport!(AsyncSocketCan);
