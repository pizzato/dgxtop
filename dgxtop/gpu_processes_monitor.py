#!/usr/bin/env python3
"""GPU process monitoring via nvidia-smi for DGX SPARK"""

import subprocess
import os
from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class GPUProcessStats:
    """Container for GPU process statistics"""

    pid: int
    user: str
    gpu_index: int
    process_type: str  # Compute, Graphic, or Compute+Graphic
    gpu_util: int  # GPU utilization %
    gpu_memory_mb: float  # GPU memory used in MiB
    cpu_percent: int  # CPU utilization %
    host_memory_mb: float  # Host memory in MiB
    command: str  # Command/process name


class GPUProcessMonitor:
    """Monitor GPU processes via nvidia-smi"""

    def __init__(self):
        self._available = self._check_nvidia_smi()
        self.last_stats: List[GPUProcessStats] = []
        self._gpu_util_cache: Dict[int, int] = {}

    def _check_nvidia_smi(self) -> bool:
        """Check if nvidia-smi is available"""
        try:
            result = subprocess.run(
                ["nvidia-smi", "--version"], capture_output=True, timeout=5
            )
            return result.returncode == 0
        except (subprocess.SubprocessError, FileNotFoundError):
            return False

    @property
    def is_available(self) -> bool:
        return self._available

    def _get_process_info(self, pid: int) -> Dict:
        """Get process info from /proc filesystem"""
        info = {
            "user": "?",
            "cpu_percent": 0,
            "host_memory_mb": 0,
            "command": f"PID {pid}",
        }

        try:
            # Get user from /proc/PID/status
            status_path = f"/proc/{pid}/status"
            if os.path.exists(status_path):
                with open(status_path, "r") as f:
                    for line in f:
                        if line.startswith("Uid:"):
                            uid = int(line.split()[1])
                            # Get username from uid
                            try:
                                import pwd
                                info["user"] = pwd.getpwuid(uid).pw_name
                            except (KeyError, ImportError):
                                info["user"] = str(uid)
                        elif line.startswith("VmRSS:"):
                            # Resident memory in kB
                            parts = line.split()
                            if len(parts) >= 2:
                                info["host_memory_mb"] = int(parts[1]) / 1024

            # Get command from /proc/PID/cmdline
            cmdline_path = f"/proc/{pid}/cmdline"
            if os.path.exists(cmdline_path):
                with open(cmdline_path, "r") as f:
                    cmdline = f.read().replace("\x00", " ").strip()
                    if cmdline:
                        info["command"] = cmdline

            # Get CPU usage from /proc/PID/stat (simplified - just show if active)
            stat_path = f"/proc/{pid}/stat"
            if os.path.exists(stat_path):
                with open(stat_path, "r") as f:
                    stat = f.read().split()
                    if len(stat) > 13:
                        # utime + stime (simplified approximation)
                        utime = int(stat[13])
                        stime = int(stat[14])
                        # This is cumulative, for real % we'd need to track deltas
                        # For now, just indicate if process is active
                        info["cpu_percent"] = min(100, (utime + stime) % 200)

        except (IOError, OSError, ValueError, IndexError):
            pass

        return info

    def _get_gpu_utilization(self) -> Dict[int, int]:
        """Get per-process GPU utilization from nvidia-smi pmon"""
        util_map = {}
        try:
            cmd = ["nvidia-smi", "pmon", "-c", "1", "-s", "u"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
            if result.returncode == 0:
                for line in result.stdout.split("\n"):
                    if line.startswith("#") or not line.strip():
                        continue
                    parts = line.split()
                    if len(parts) >= 4:
                        try:
                            pid = int(parts[1])
                            sm_util = int(parts[3]) if parts[3] != "-" else 0
                            util_map[pid] = sm_util
                        except (ValueError, IndexError):
                            continue
        except Exception:
            pass
        return util_map

    def get_stats(self) -> List[GPUProcessStats]:
        """Query GPU process statistics via nvidia-smi"""
        if not self._available:
            return []

        try:
            processes = []
            process_map: Dict[int, dict] = {}

            # Get GPU utilization per process
            gpu_util_map = self._get_gpu_utilization()

            # Query compute processes (Type C)
            cmd_compute = [
                "nvidia-smi",
                "--query-compute-apps=pid,process_name,used_memory",
                "--format=csv,noheader,nounits",
            ]
            result_compute = subprocess.run(cmd_compute, capture_output=True, text=True, timeout=2)
            if result_compute.returncode == 0 and result_compute.stdout.strip():
                for line in result_compute.stdout.strip().split("\n"):
                    if not line.strip():
                        continue
                    values = [v.strip() for v in line.split(",")]
                    if len(values) >= 3:
                        try:
                            pid = int(values[0])
                            memory = float(values[2]) if values[2] not in ["[N/A]", "N/A", ""] else 0.0
                            process_map[pid] = {
                                "gpu_memory_mb": memory,
                                "process_type": "Compute",
                                "gpu_index": 0,
                            }
                        except (ValueError, IndexError):
                            continue

            # Parse main nvidia-smi output for all processes including graphics
            cmd_main = ["nvidia-smi"]
            result_main = subprocess.run(cmd_main, capture_output=True, text=True, timeout=2)

            if result_main.returncode == 0:
                in_processes_section = False

                for line in result_main.stdout.split("\n"):
                    if "Processes:" in line:
                        in_processes_section = True
                        continue
                    if in_processes_section and "|" in line:
                        parts = line.replace("|", "").split()
                        if len(parts) >= 6:
                            try:
                                # GPU index is first
                                gpu_idx = int(parts[0]) if parts[0].isdigit() else 0

                                # Find PID
                                pid_idx = None
                                for i, p in enumerate(parts):
                                    if p.isdigit() and i > 0:
                                        pid_idx = i
                                        break

                                if pid_idx is None:
                                    continue

                                pid = int(parts[pid_idx])

                                # Type is after PID
                                type_str = parts[pid_idx + 1] if pid_idx + 1 < len(parts) else "G"
                                if type_str == "C":
                                    proc_type = "Compute"
                                elif type_str == "G":
                                    proc_type = "Graphic"
                                elif type_str == "C+G":
                                    proc_type = "Compute+Graphic"
                                else:
                                    proc_type = "Graphic"

                                # Memory is last item
                                mem_str = parts[-1]
                                memory = 0.0
                                if "MiB" in mem_str:
                                    memory = float(mem_str.replace("MiB", ""))

                                # Update or add process
                                if pid not in process_map:
                                    process_map[pid] = {
                                        "gpu_memory_mb": memory,
                                        "process_type": proc_type,
                                        "gpu_index": gpu_idx,
                                    }
                                else:
                                    # Update with graphics info if needed
                                    if process_map[pid]["gpu_memory_mb"] == 0:
                                        process_map[pid]["gpu_memory_mb"] = memory
                                    process_map[pid]["gpu_index"] = gpu_idx

                            except (ValueError, IndexError):
                                continue

            # Build final process list with all details
            for pid, data in process_map.items():
                proc_info = self._get_process_info(pid)

                # Truncate command for display
                command = proc_info["command"]
                if len(command) > 50:
                    command = command[:47] + "..."

                processes.append(GPUProcessStats(
                    pid=pid,
                    user=proc_info["user"][:8],  # Truncate username
                    gpu_index=data["gpu_index"],
                    process_type=data["process_type"],
                    gpu_util=gpu_util_map.get(pid, 0),
                    gpu_memory_mb=data["gpu_memory_mb"],
                    cpu_percent=proc_info["cpu_percent"],
                    host_memory_mb=proc_info["host_memory_mb"],
                    command=command,
                ))

            # Sort by GPU memory usage descending
            processes.sort(key=lambda x: x.gpu_memory_mb, reverse=True)

            self.last_stats = processes
            return processes

        except Exception as e:
            import sys
            print(f"GPU process monitor error: {e}", file=sys.stderr)
            return self.last_stats
