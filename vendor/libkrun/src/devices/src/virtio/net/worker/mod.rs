#[cfg(unix)]
mod inline_tx;
#[cfg(unix)]
mod inproc;
#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use inline_tx::InlineTx;
#[cfg(unix)]
pub use inproc::InprocWorker;
#[cfg(unix)]
pub use unix::NetWorker;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::NetWorker;
