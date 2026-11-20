"""Machine, engine and tool version information recorded in every result file."""

import platform
import subprocess

from .parse import parse_loadavg


def _out(argv):
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as e:
        return f"error: {e}"
    return (proc.stdout or proc.stderr).strip()


def _sysctl(name):
    return _out(["sysctl", "-n", name])


def loadavg_1m():
    return parse_loadavg(_sysctl("vm.loadavg"))


def machine():
    return {
        "model": _sysctl("hw.model"),
        "cpu": _sysctl("machdep.cpu.brand_string"),
        "cores": int(_sysctl("hw.ncpu")),
        "performance_cores": int(_sysctl("hw.perflevel0.physicalcpu")),
        "efficiency_cores": int(_sysctl("hw.perflevel1.physicalcpu")),
        "memory_bytes": int(_sysctl("hw.memsize")),
        "macos": _out(["sw_vers", "-productVersion"]),
        "macos_build": _out(["sw_vers", "-buildVersion"]),
        "darwin": platform.release(),
        "host_tools": {
            "git": _out(["git", "--version"]),
            "tar": _out(["/usr/bin/tar", "--version"]),
            "cc": _out(["cc", "--version"]).splitlines()[0],
        },
    }


def docker_info(docker):
    version = docker.json("version", "--format", "{{json .}}")
    info = docker.json("info", "--format", "{{json .}}")
    server = version.get("Server") or {}
    return {
        "context": docker.context,
        "client_version": (version.get("Client") or {}).get("Version"),
        "server_version": server.get("Version"),
        "api_version": server.get("ApiVersion"),
        "os": info.get("OperatingSystem"),
        "kernel": info.get("KernelVersion"),
        "ncpu": info.get("NCPU"),
        "mem_total_bytes": info.get("MemTotal"),
        "storage_driver": info.get("Driver"),
        "name": info.get("Name"),
        "running_containers_at_start": sorted(
            docker.run("ps", "--format", "{{.Names}}").stdout.split()
        ),
    }


def engine_identity(context, docker_server_version):
    """Product name and version of the engine behind a Docker context.

    The product is the context name ("Docker" for the default context) and the version is the
    server version that `docker version` reports.
    """
    product = "Docker" if context in (None, "", "default") else context
    return {"product": product, "version": docker_server_version, "raw": None}


def image_tools(container):
    script = (
        "echo git=$(git --version); echo node=$(node -v); echo npm=$(npm -v); echo pnpm=$(pnpm -v); "
        "echo rustc=$(rustc -V); echo cargo=$(cargo -V); echo fio=$(fio --version); "
        "echo inotifywait=$(inotifywait --help 2>&1 | head -1); echo gcc=$(gcc --version | head -1); "
        "echo tar=$(tar --version | head -1); echo os=$(. /etc/os-release && echo $PRETTY_NAME)"
    )
    return dict(line.split("=", 1) for line in container.sh(script).splitlines() if "=" in line)
