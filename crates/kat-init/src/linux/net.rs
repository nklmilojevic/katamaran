//! eth0 on the host's userspace network stack (gvproxy). The host passes the
//! addressing as `kat.net=<addr>/<prefix>,<gateway>,<host>,<mtu>`, matching
//! what gvproxy serves, so the guest configures itself statically with a few
//! ioctls instead of running DHCP.

use std::fs;
use std::io;
use std::mem;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::OnceLock;

const IFACE: &str = "eth0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetConfig {
    pub addr: Ipv4Addr,
    pub prefix_len: u8,
    /// Default route and DNS server.
    pub gateway: Ipv4Addr,
    /// Reaches the Mac's 127.0.0.1; containers see it as host.docker.internal.
    pub host: Ipv4Addr,
    pub mtu: u16,
}

static CONFIG: OnceLock<NetConfig> = OnceLock::new();

pub fn parse(cmdline: &str) -> Option<NetConfig> {
    let v = cmdline
        .split_whitespace()
        .find_map(|a| a.strip_prefix("kat.net="))?;
    let mut parts = v.split(',');
    let (addr, prefix_len) = parts.next()?.split_once('/')?;
    let cfg = NetConfig {
        addr: addr.parse().ok()?,
        prefix_len: prefix_len.parse().ok().filter(|p| (1..=30).contains(p))?,
        gateway: parts.next()?.parse().ok()?,
        host: parts.next()?.parse().ok()?,
        mtu: parts.next()?.parse().ok().filter(|m| *m >= 68)?,
    };
    parts.next().is_none().then_some(cfg)
}

/// Configure eth0, the default route, resolv.conf and hosts. Returns the
/// config, or `None` when the host gave no network.
pub fn setup(cmdline: &str) -> io::Result<Option<NetConfig>> {
    let Some(cfg) = parse(cmdline) else {
        return Ok(None);
    };
    let sock = inet_socket()?;
    set_mtu(&sock, cfg.mtu)?;
    set_addr(&sock, libc::SIOCSIFADDR, cfg.addr)?;
    set_addr(&sock, libc::SIOCSIFNETMASK, netmask(cfg.prefix_len))?;
    super::sys::link_up(IFACE)?;
    add_default_route(&sock, cfg.gateway)?;
    fs::write("/etc/resolv.conf", resolv_conf(&cfg))?;
    let hosts = fs::read_to_string("/etc/hosts").unwrap_or_default();
    fs::write("/etc/hosts", with_host_entry(&hosts, &cfg))?;
    disable_bridge_netfilter();
    let _ = CONFIG.set(cfg);
    Ok(Some(cfg))
}

/// Keep L2 bridged traffic between containers on the same bridge out of the
/// IPv4/IPv6 netfilter/conntrack hooks (L3 routed traffic, NAT, published
/// ports and inter-bridge isolation still go through netfilter normally).
fn disable_bridge_netfilter() {
    for path in [
        "/proc/sys/net/bridge/bridge-nf-call-iptables",
        "/proc/sys/net/bridge/bridge-nf-call-ip6tables",
        "/proc/sys/net/bridge/bridge-nf-call-arptables",
    ] {
        let _ = fs::write(path, "0\n");
    }
}

/// Extra dockerd flags for the configured network, if any.
pub fn dockerd_args() -> Vec<String> {
    CONFIG.get().map(docker_flags).unwrap_or_default()
}

/// `host-gateway` (as in `--add-host name:host-gateway`) becomes the address
/// that reaches the Mac. Bridges get eth0's MTU: gvproxy terminates every
/// TCP connection, so no path beyond it sees these frames, and per-packet
/// cost in its stack is what limits container throughput.
fn docker_flags(cfg: &NetConfig) -> Vec<String> {
    vec![
        format!("--host-gateway-ip={}", cfg.host),
        format!("--mtu={}", cfg.mtu),
        format!(
            "--default-network-opt=bridge=com.docker.network.driver.mtu={}",
            cfg.mtu
        ),
    ]
}

fn resolv_conf(cfg: &NetConfig) -> String {
    format!("nameserver {}\n", cfg.gateway)
}

fn with_host_entry(hosts: &str, cfg: &NetConfig) -> String {
    let mut out = hosts.to_string();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&format!(
        "{}\thost.docker.internal host.internal\n",
        cfg.host
    ));
    out
}

fn netmask(prefix_len: u8) -> Ipv4Addr {
    Ipv4Addr::from(u32::MAX << (32 - u32::from(prefix_len)))
}

fn inet_socket() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn ifreq() -> libc::ifreq {
    let mut req: libc::ifreq = unsafe { mem::zeroed() };
    for (dst, src) in req.ifr_name.iter_mut().zip(IFACE.bytes()) {
        *dst = src as libc::c_char;
    }
    req
}

fn sockaddr_in(addr: Ipv4Addr) -> libc::sockaddr {
    let sin = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: 0,
        sin_addr: libc::in_addr {
            s_addr: u32::from(addr).to_be(),
        },
        sin_zero: [0; 8],
    };
    unsafe { mem::transmute::<libc::sockaddr_in, libc::sockaddr>(sin) }
}

fn ioctl<T>(sock: &OwnedFd, req: libc::Ioctl, arg: &T, what: &str) -> io::Result<()> {
    if unsafe { libc::ioctl(sock.as_raw_fd(), req, arg as *const T) } < 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(e.kind(), format!("{what} on {IFACE}: {e}")));
    }
    Ok(())
}

fn set_mtu(sock: &OwnedFd, mtu: u16) -> io::Result<()> {
    let mut req = ifreq();
    req.ifr_ifru.ifru_mtu = mtu.into();
    ioctl(sock, libc::SIOCSIFMTU as _, &req, "set mtu")
}

fn set_addr(sock: &OwnedFd, op: libc::c_ulong, addr: Ipv4Addr) -> io::Result<()> {
    let mut req = ifreq();
    req.ifr_ifru.ifru_addr = sockaddr_in(addr);
    ioctl(sock, op as _, &req, "set address")
}

fn add_default_route(sock: &OwnedFd, gateway: Ipv4Addr) -> io::Result<()> {
    let mut rt: libc::rtentry = unsafe { mem::zeroed() };
    rt.rt_dst = sockaddr_in(Ipv4Addr::UNSPECIFIED);
    rt.rt_genmask = sockaddr_in(Ipv4Addr::UNSPECIFIED);
    rt.rt_gateway = sockaddr_in(gateway);
    rt.rt_flags = libc::RTF_UP | libc::RTF_GATEWAY;
    ioctl(sock, libc::SIOCADDRT as _, &rt, "add default route")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> NetConfig {
        NetConfig {
            addr: Ipv4Addr::new(192, 168, 127, 2),
            prefix_len: 24,
            gateway: Ipv4Addr::new(192, 168, 127, 1),
            host: Ipv4Addr::new(192, 168, 127, 254),
            mtu: 65520,
        }
    }

    #[test]
    fn parses_net_arg() {
        let c = "console=hvc0 kat.net=192.168.127.2/24,192.168.127.1,192.168.127.254,65520 quiet";
        assert_eq!(parse(c), Some(cfg()));
        assert_eq!(parse("console=hvc0"), None);
        for bad in [
            "kat.net=192.168.127.2,192.168.127.1,192.168.127.254,1500",
            "kat.net=192.168.127.2/33,192.168.127.1,192.168.127.254,1500",
            "kat.net=192.168.127.2/24,192.168.127.1,192.168.127.254",
            "kat.net=192.168.127.2/24,192.168.127.1,192.168.127.254,1500,x",
            "kat.net=192.168.127.2/24,gw,192.168.127.254,1500",
            "kat.net=192.168.127.2/24,192.168.127.1,192.168.127.254,10",
        ] {
            assert_eq!(parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn docker_flags_match_network() {
        assert_eq!(
            docker_flags(&cfg()),
            [
                "--host-gateway-ip=192.168.127.254",
                "--mtu=65520",
                "--default-network-opt=bridge=com.docker.network.driver.mtu=65520"
            ]
        );
    }

    #[test]
    fn computes_netmask() {
        assert_eq!(netmask(24), Ipv4Addr::new(255, 255, 255, 0));
        assert_eq!(netmask(1), Ipv4Addr::new(128, 0, 0, 0));
        assert_eq!(netmask(30), Ipv4Addr::new(255, 255, 255, 252));
    }

    #[test]
    fn writes_resolver_and_hosts() {
        assert_eq!(resolv_conf(&cfg()), "nameserver 192.168.127.1\n");
        assert_eq!(
            with_host_entry("127.0.0.1\tlocalhost", &cfg()),
            "127.0.0.1\tlocalhost\n192.168.127.254\thost.docker.internal host.internal\n"
        );
    }
}
