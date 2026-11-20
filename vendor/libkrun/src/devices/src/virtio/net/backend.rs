use std::io;

#[cfg(unix)]
use std::os::fd::RawFd;
#[cfg(windows)]
use std::os::windows::io::RawSocket;

#[cfg(windows)]
use vm_memory::GuestMemoryMmap;

#[cfg(unix)]
pub type SysError = nix::Error;
#[cfg(windows)]
pub type SysError = io::Error;

#[allow(dead_code)]
#[derive(Debug)]
pub enum ConnectError {
    InvalidAddress(SysError),
    CreateSocket(SysError),
    Binding(SysError),
    #[cfg(windows)]
    Worker(SysError),
    #[cfg(not(target_os = "windows"))]
    SendingMagic(nix::Error),
    // Tap backend errors.
    #[cfg(not(target_os = "windows"))]
    OpenNetTun(nix::Error),
    #[cfg(not(target_os = "windows"))]
    TunSetIff(io::Error),
    #[cfg(not(target_os = "windows"))]
    TunSetVnetHdrSz(io::Error),
    #[cfg(not(target_os = "windows"))]
    TunSetOffload(io::Error),
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum ReadError {
    /// Nothing was written
    NothingRead,
    /// The guest queue ran out of available descriptors
    #[cfg(windows)]
    DescriptorStarvation,
    #[cfg(windows)]
    ProcessNotRunning,
    #[cfg(windows)]
    Queue(crate::virtio::queue::Error),
    /// Another internal error occurred
    Internal(SysError),
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum WriteError {
    /// Nothing was written, you can drop the frame or try to resend it later
    NothingWritten,
    /// Part of the buffer was written, the write has to be finished using try_finish_write
    PartialWrite,
    /// Passt doesnt seem to be running (received EPIPE)
    ProcessNotRunning,
    /// Another internal error occurred
    Internal(SysError),
}

#[cfg(unix)]
pub trait NetBackend {
    fn read_frame(&mut self, buf: &mut [u8]) -> Result<usize, ReadError>;
    fn write_frame(&mut self, hdr_len: usize, buf: &mut [u8]) -> Result<(), WriteError>;
    fn has_unfinished_write(&self) -> bool;
    fn try_finish_write(&mut self, hdr_len: usize, buf: &[u8]) -> Result<(), WriteError>;
    fn raw_socket_fd(&self) -> RawFd;

    /// Write one frame gathered from `iov` (host addresses of guest memory)
    /// without staging it in a buffer. `None` if the backend can't, as a
    /// stream backend that may write part of a frame can't.
    fn write_frame_iov(&mut self, _iov: &[libc::iovec]) -> Option<Result<(), WriteError>> {
        None
    }

    /// A duplicate of the socket for vCPU threads to send small frames on
    /// with [`write_frame_iov`](Self::write_frame_iov)'s semantics, if the
    /// backend allows sending from two threads.
    fn inline_tx_socket(&self) -> Option<std::os::fd::OwnedFd> {
        None
    }

    /// Delay in microseconds before retrying after NothingWritten.
    /// Returns 0 if no delay-based retry is needed (e.g. on Linux where
    /// EAGAIN + EPOLLET handles retries via writable events).
    #[allow(dead_code)]
    fn write_retry_delay_us(&self) -> u64 {
        0
    }
}

#[cfg(windows)]
#[derive(Debug, PartialEq, Eq)]
pub enum WriteStatus {
    Complete,
    Pending,
}

#[cfg(windows)]
pub trait NetBackend {
    fn prepare_tx_buffer(&mut self) -> &mut [u8];

    fn start_tx(&mut self, total_bytes: usize) -> Result<WriteStatus, WriteError>;

    fn resume_tx(&mut self) -> Result<WriteStatus, WriteError>;

    fn read_frames_to_guest(
        &mut self,
        mem: &GuestMemoryMmap,
        rx_queue: &mut crate::virtio::queue::Queue,
    ) -> Result<u32, ReadError>;

    fn raw_socket_fd(&self) -> RawSocket;
}
