//! Serve a Docker-style volumes directory over a Unix socket, for trying the
//! server with the Mac's own NFS client:
//!
//!     cargo run -p kat-nfs --example serve -- <volumes-dir> <socket>
//!     mount_nfs -o vers=3,proto=ticotsord,port=<socket>,mountport=<socket> '<>:/' <dir>

use std::os::unix::net::UnixListener;
use std::sync::Arc;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let [_, volumes, socket] = &args[..] else {
        eprintln!("usage: serve <volumes-dir> <socket>");
        std::process::exit(2);
    };
    let server = Arc::new(kat_nfs::Server::new(kat_nfs::Layout {
        volumes: volumes.into(),
        containers: Box::new(kat_nfs::layout::NoContainers),
        machines: "/nonexistent".into(),
    }));
    let _ = std::fs::remove_file(socket);
    let listener = UnixListener::bind(socket)?;
    eprintln!("serving {volumes} on {socket}");
    for conn in listener.incoming() {
        let (server, conn) = (server.clone(), conn?);
        std::thread::spawn(move || {
            if let Err(e) = kat_nfs::serve(server, conn) {
                eprintln!("connection: {e}");
            }
        });
    }
    Ok(())
}
