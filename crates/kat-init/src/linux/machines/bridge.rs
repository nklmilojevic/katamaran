//! The machine network: bridge `katbr0` (198.19.249.1/24) in the VM's root
//! namespace, NAT to eth0 (gvproxy) for internet access, dnsmasq handing
//! each machine its fixed address over DHCP, and DNAT rules that make a
//! machine's listening ports reachable on the Mac's localhost through the
//! existing port forwarding. Set up on the first machine start, so a VM
//! without machines pays nothing.

use std::fs;
use std::io;
use std::net::Ipv4Addr;
use std::process::Command;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Duration;

use super::super::service::Reaper;
use super::{cmd, store};

pub const BRIDGE: &str = "katbr0";
const RUN_DIR: &str = "/run/katamaran";
/// dnsmasq watches this directory with inotify: no signal needed.
const HOSTS_DIR: &str = "/run/katamaran/dhcp-hosts";
const HOSTS: &str = "/run/katamaran/dhcp-hosts/machines";
const CONF: &str = "/run/katamaran/dnsmasq.conf";
const T: Duration = Duration::from_secs(10);

pub struct Bridge {
    pub mtu: u32,
    dns: Option<Ipv4Addr>,
    dnsmasq_exited: Receiver<i32>,
}

/// eth0's MTU, which the machines' veths copy (gvproxy's 65520 by default).
fn uplink_mtu() -> u32 {
    fs::read_to_string("/sys/class/net/eth0/mtu")
        .ok()
        .and_then(|m| m.trim().parse().ok())
        .unwrap_or(1500)
}

fn sh(reaper: &Reaper, args: &[&str]) -> io::Result<String> {
    cmd::run(reaper, Command::new(args[0]).args(&args[1..]), T)
}

/// Add an iptables rule unless it is there already.
fn ensure_rule(
    reaper: &Reaper,
    table: &str,
    chain: &str,
    rule: &[&str],
    insert: bool,
) -> io::Result<()> {
    let mut check = vec!["iptables", "-w", "-t", table, "-C", chain];
    check.extend_from_slice(rule);
    if cmd::run_status(reaper, Command::new("iptables").args(&check[1..]), T)?.0 == 0 {
        return Ok(());
    }
    let mut add = vec![
        "iptables",
        "-w",
        "-t",
        table,
        if insert { "-I" } else { "-A" },
        chain,
    ];
    add.extend_from_slice(rule);
    sh(reaper, &add).map(|_| ())
}

impl Bridge {
    pub fn up(
        reaper: &Reaper,
        dns: Option<Ipv4Addr>,
        hosts: &[(String, Ipv4Addr)],
    ) -> io::Result<Bridge> {
        let gw = format!("{}/{}", store::gateway(), store::PREFIX);
        let subnet = format!(
            "{}.{}.{}.0/{}",
            store::SUBNET[0],
            store::SUBNET[1],
            store::SUBNET[2],
            store::PREFIX
        );
        if !std::path::Path::new(&format!("/sys/class/net/{BRIDGE}")).exists() {
            sh(reaper, &["ip", "link", "add", BRIDGE, "type", "bridge"])?;
        }
        sh(reaper, &["ip", "addr", "replace", &gw, "dev", BRIDGE])?;
        sh(reaper, &["ip", "link", "set", BRIDGE, "up"])?;
        fs::write("/proc/sys/net/ipv4/ip_forward", "1\n")?;
        ensure_rule(
            reaper,
            "nat",
            "POSTROUTING",
            &["-s", &subnet, "!", "-o", BRIDGE, "-j", "MASQUERADE"],
            false,
        )?;
        // dockerd sets the FORWARD policy to DROP.
        ensure_rule(
            reaper,
            "filter",
            "FORWARD",
            &["-i", BRIDGE, "-j", "ACCEPT"],
            true,
        )?;
        ensure_rule(
            reaper,
            "filter",
            "FORWARD",
            &["-o", BRIDGE, "-j", "ACCEPT"],
            true,
        )?;

        fs::create_dir_all(HOSTS_DIR)?;
        fs::write(HOSTS, dhcp_hosts(hosts))?;
        fs::write(CONF, dnsmasq_conf(dns.unwrap_or(store::gateway())))?;
        let dnsmasq_exited = spawn_dnsmasq(reaper)?;
        Ok(Bridge {
            mtu: uplink_mtu(),
            dns,
            dnsmasq_exited,
        })
    }

    /// Update the machine → address table (dnsmasq rereads it on change).
    pub fn set_hosts(&self, hosts: &[(String, Ipv4Addr)]) -> io::Result<()> {
        fs::write(HOSTS, dhcp_hosts(hosts))
    }

    /// Restart dnsmasq if it has exited.
    pub fn ensure_dnsmasq(&mut self, reaper: &Reaper) -> io::Result<()> {
        match self.dnsmasq_exited.try_recv() {
            Err(TryRecvError::Empty) => Ok(()),
            Ok(_) | Err(TryRecvError::Disconnected) => {
                fs::write(CONF, dnsmasq_conf(self.dns.unwrap_or(store::gateway())))?;
                self.dnsmasq_exited = spawn_dnsmasq(reaper)?;
                Ok(())
            }
        }
    }
}

fn spawn_dnsmasq(reaper: &Reaper) -> io::Result<Receiver<i32>> {
    let mut c = Command::new("dnsmasq");
    // --conf-file takes an optional value, so only `=` attaches it.
    c.args(["--keep-in-foreground", &format!("--conf-file={CONF}")])
        .env("PATH", cmd::PATH)
        .stdin(std::process::Stdio::null());
    let (_pid, exited) = reaper.spawn(&mut c)?;
    Ok(exited)
}

pub fn dnsmasq_conf(dns: Ipv4Addr) -> String {
    let [a, b, c] = store::SUBNET;
    format!(
        "port=0
interface={BRIDGE}
bind-dynamic
dhcp-authoritative
dhcp-range={a}.{b}.{c}.0,static,255.255.255.0,12h
dhcp-hostsdir={HOSTS_DIR}
dhcp-leasefile={RUN_DIR}/dnsmasq.leases
dhcp-option=option:router,{gw}
dhcp-option=option:dns-server,{dns}
log-facility=/var/log/dnsmasq.log
",
        gw = store::gateway()
    )
}

pub fn dhcp_hosts(hosts: &[(String, Ipv4Addr)]) -> String {
    hosts
        .iter()
        .map(|(name, ip)| format!("{},{ip},{name}\n", store::mac(*ip)))
        .collect()
}

/// DNAT eth0:<port> to a machine, so the forward of the Mac's
/// 127.0.0.1:<port> reaches it.
pub fn dnat(reaper: &Reaper, port: u16, ip: Ipv4Addr, add: bool) -> io::Result<()> {
    let port = port.to_string();
    let to = format!("{ip}:{port}");
    let rule = [
        "-i",
        "eth0",
        "-p",
        "tcp",
        "--dport",
        &port,
        "-j",
        "DNAT",
        "--to-destination",
        &to,
    ];
    if add {
        ensure_rule(reaper, "nat", "PREROUTING", &rule, true)
    } else {
        let mut del = vec!["iptables", "-w", "-t", "nat", "-D", "PREROUTING"];
        del.extend_from_slice(&rule);
        sh(reaper, &del).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_dnsmasq_files() {
        let c = dnsmasq_conf(Ipv4Addr::new(192, 168, 127, 1));
        assert!(c.contains("dhcp-range=198.19.249.0,static,255.255.255.0,12h\n"));
        assert!(c.contains("dhcp-option=option:router,198.19.249.1\n"));
        assert!(c.contains("dhcp-option=option:dns-server,192.168.127.1\n"));
        assert_eq!(
            dhcp_hosts(&[("dev".into(), Ipv4Addr::new(198, 19, 249, 2))]),
            "02:6b:61:13:f9:02,198.19.249.2,dev\n"
        );
    }
}
