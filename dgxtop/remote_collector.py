#!/usr/bin/env python3
"""Remote node data collection via SSH for DGXTOP cluster monitoring"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cluster_config import NodeConfig


# Inline Python script executed on the remote host via `python3 -`.
# Uses only the standard library so it works on any DGX Spark out of the box.
# NOTE: this is a raw string — \n inside the script are literal characters that
# Python on the remote machine will interpret correctly.
COLLECTOR_SCRIPT = r"""
import subprocess, json, os, time


def sf(v, d=0.0):
    try:
        return float(v) if v not in ("[N/A]", "N/A", "") else d
    except (ValueError, TypeError):
        return d


result = {}

# ── GPU stats ──────────────────────────────────────────────────────────────
try:
    r = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=index,name,utilization.gpu,temperature.gpu,"
            "power.draw,power.limit,clocks.current.graphics,clocks.max.graphics",
            "--format=csv,noheader,nounits",
        ],
        capture_output=True, text=True, timeout=5,
    )
    if r.returncode == 0 and r.stdout.strip():
        p = [v.strip() for v in r.stdout.strip().split(",")]
        result["gpu"] = {
            "index": int(sf(p[0])),
            "name": p[1].strip() if len(p) > 1 else "Unknown",
            "util": sf(p[2]),
            "temp": sf(p[3]),
            "power": sf(p[4]),
            "power_limit": sf(p[5], 100.0),
            "clock": sf(p[6]) if len(p) > 6 else 0.0,
            "clock_max": sf(p[7]) if len(p) > 7 else 0.0,
        }
    else:
        result["gpu"] = {"util": 0, "temp": 0, "power": 0, "power_limit": 100, "name": "N/A"}
except Exception:
    result["gpu"] = {"util": 0, "temp": 0, "power": 0, "power_limit": 100, "name": "N/A"}

# ── GPU processes ──────────────────────────────────────────────────────────
try:
    import pwd
    r2 = subprocess.run(
        ["nvidia-smi", "--query-compute-apps=pid,gpu_uuid,used_memory",
         "--format=csv,noheader,nounits"],
        capture_output=True, text=True, timeout=5,
    )
    procs = []
    if r2.returncode == 0 and r2.stdout.strip():
        for line in r2.stdout.strip().split("\n"):
            p = [v.strip() for v in line.split(",")]
            if len(p) >= 3:
                try:
                    pid = int(p[0])
                    mem = sf(p[2])
                    try:
                        with open(f"/proc/{pid}/comm") as f:
                            cmd_name = f.read().strip()
                    except Exception:
                        cmd_name = "unknown"
                    try:
                        with open(f"/proc/{pid}/status") as f:
                            uid = 0
                            for sl in f:
                                if sl.startswith("Uid:"):
                                    uid = int(sl.split()[1])
                                    break
                            user = pwd.getpwuid(uid).pw_name
                    except Exception:
                        user = "unknown"
                    procs.append({
                        "pid": pid,
                        "user": user,
                        "gpu_mem_mb": mem,
                        "command": cmd_name,
                    })
                except Exception:
                    pass
    result["processes"] = procs
except Exception:
    result["processes"] = []

# ── CPU usage (two-point delta for accuracy) ───────────────────────────────
def read_cpu_times():
    try:
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("cpu "):
                    return [float(x) for x in line.split()[1:]]
    except Exception:
        pass
    return []


t1 = read_cpu_times()
time.sleep(0.2)
t2 = read_cpu_times()
if t1 and t2:
    d = [t2[i] - t1[i] for i in range(min(len(t1), len(t2)))]
    total = sum(d)
    idle = d[3] + (d[4] if len(d) > 4 else 0)
    result["cpu_usage"] = (total - idle) / total * 100 if total > 0 else 0.0
else:
    result["cpu_usage"] = 0.0

# ── CPU temperature ────────────────────────────────────────────────────────
temp = 0.0
try:
    for zid in range(10):
        tp = f"/sys/class/thermal/thermal_zone{zid}/temp"
        tt = f"/sys/class/thermal/thermal_zone{zid}/type"
        if os.path.exists(tp) and os.path.exists(tt):
            with open(tt) as f:
                zt = f.read().strip().lower()
            if "cpu" in zt or "soc" in zt:
                with open(tp) as f:
                    temp = int(f.read().strip()) / 1000.0
                break
    if temp == 0.0 and os.path.exists("/sys/class/thermal/thermal_zone0/temp"):
        with open("/sys/class/thermal/thermal_zone0/temp") as f:
            temp = int(f.read().strip()) / 1000.0
except Exception:
    pass
result["cpu_temp"] = temp

# ── Memory ─────────────────────────────────────────────────────────────────
try:
    mi = {}
    with open("/proc/meminfo") as f:
        for line in f:
            if ":" in line:
                k, v = line.split(":", 1)
                v = v.strip()
                if " " in v:
                    n, u = v.split(" ", 1)
                    try:
                        mi[k.strip()] = int(n) * (1024 if u.strip() == "kB" else 1)
                    except ValueError:
                        pass
    tot = mi.get("MemTotal", 0)
    free = mi.get("MemFree", 0)
    buf = mi.get("Buffers", 0)
    cac = mi.get("Cached", 0)
    used = tot - free - buf - cac
    if used < 0:
        used = tot - free
    result["memory"] = {
        "total_gb": tot / 1073741824.0,
        "used_gb": used / 1073741824.0,
        "percent": used / tot * 100.0 if tot > 0 else 0.0,
    }
except Exception:
    result["memory"] = {"total_gb": 0.0, "used_gb": 0.0, "percent": 0.0}

# ── Load average ───────────────────────────────────────────────────────────
try:
    with open("/proc/loadavg") as f:
        parts = f.read().split()
    result["loadavg"] = [float(parts[0]), float(parts[1]), float(parts[2])]
except Exception:
    result["loadavg"] = [0.0, 0.0, 0.0]

print(json.dumps(result))
"""


class RemoteCollector:
    """Collects system stats from a remote node via SSH (paramiko)"""

    def __init__(self, node: NodeConfig):
        self.node = node
        self._client = None
        self._last_snapshot: dict = {"status": "disconnected"}

    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._client.get_transport() is not None and \
               self._client.get_transport().is_active()

    def connect(self) -> None:
        """Open an SSH connection using keys from ~/.ssh/"""
        try:
            import paramiko
        except ImportError:
            raise ImportError(
                "paramiko is required for cluster mode. Install with: pip install paramiko"
            )

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=self.node.host,
            port=self.node.port,
            username=self.node.user,
            timeout=10,
            look_for_keys=True,
            allow_agent=True,
        )
        self._client = client

    def collect_snapshot(self) -> dict:
        """Run the inline collector script and return the parsed JSON result"""
        if not self.is_connected:
            return {"status": "disconnected"}

        try:
            stdin_ch, stdout, stderr = self._client.exec_command(
                "python3 -", timeout=15
            )
            stdin_ch.write(COLLECTOR_SCRIPT.encode())
            stdin_ch.close()

            raw = stdout.read().decode(errors="replace").strip()
            if not raw:
                err = stderr.read().decode(errors="replace").strip()
                return {"status": "error", "error": err or "empty output"}

            data = json.loads(raw)
            data["status"] = "ok"
            self._last_snapshot = data
            return data

        except json.JSONDecodeError as e:
            return {"status": "error", "error": f"JSON parse: {e}"}
        except Exception as e:
            self.disconnect()
            return {"status": "disconnected", "error": str(e)}

    def disconnect(self) -> None:
        """Close the SSH connection"""
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    @property
    def last_snapshot(self) -> dict:
        return self._last_snapshot
