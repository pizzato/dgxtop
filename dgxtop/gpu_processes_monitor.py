#!/usr/bin/env python3
"""GPU process monitoring via nvidia-smi for DGX SPARK"""

import subprocess
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class GPUProcessStats:
    """Container for GPU process statistics"""

    pid: int
    name: str
    gpu_memory_mb: float  # Memory used in MB
    process_type: str  # C = Compute, G = Graphics, C+G = Both


class GPUProcessMonitor:
    """Monitor GPU processes via nvidia-smi"""

    def __init__(self):
        self._available = self._check_nvidia_smi()
        self.last_stats: List[GPUProcessStats] = []

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

    def _parse_process_output(self, output: str, proc_type: str) -> List[GPUProcessStats]:
        """Parse nvidia-smi CSV output for processes"""
        processes = []
        if not output.strip():
            return processes

        for line in output.strip().split("\n"):
            if not line.strip():
                continue
            values = [v.strip() for v in line.split(",")]
            if len(values) >= 3:
                try:
                    pid = int(values[0])
                    name = values[1].strip()
                    # Truncate long process names
                    if len(name) > 25:
                        name = name[:22] + "..."
                    memory = float(values[2]) if values[2] not in ["[N/A]", "N/A", ""] else 0.0
                    processes.append(GPUProcessStats(
                        pid=pid,
                        name=name,
                        gpu_memory_mb=memory,
                        process_type=proc_type,
                    ))
                except (ValueError, IndexError):
                    continue
        return processes

    def get_stats(self) -> List[GPUProcessStats]:
        """Query GPU process statistics via nvidia-smi"""
        if not self._available:
            return []

        try:
            processes = []

            # Query compute processes (Type C)
            cmd_compute = [
                "nvidia-smi",
                "--query-compute-apps=pid,process_name,used_memory",
                "--format=csv,noheader,nounits",
            ]
            result_compute = subprocess.run(cmd_compute, capture_output=True, text=True, timeout=2)
            if result_compute.returncode == 0:
                processes.extend(self._parse_process_output(result_compute.stdout, "C"))

            # Query graphics processes (Type G)
            cmd_graphics = [
                "nvidia-smi",
                "--query-accounted-apps=pid,process_name,gpu_util",
                "--format=csv,noheader,nounits",
            ]
            # Note: accounted-apps may not work, fallback to parsing main output

            # Parse main nvidia-smi output for graphics processes
            cmd_main = ["nvidia-smi"]
            result_main = subprocess.run(cmd_main, capture_output=True, text=True, timeout=2)

            if result_main.returncode == 0:
                existing_pids = {p.pid for p in processes}
                in_processes_section = False

                for line in result_main.stdout.split("\n"):
                    if "Processes:" in line:
                        in_processes_section = True
                        continue
                    if in_processes_section and "|" in line:
                        # Parse lines like: |    0   N/A  N/A   2702   G   /usr/lib/xorg/Xorg   99MiB |
                        parts = line.replace("|", "").split()
                        if len(parts) >= 6:
                            try:
                                # Find PID (first number after GPU index)
                                pid_idx = None
                                for i, p in enumerate(parts):
                                    if p.isdigit() and i > 0:
                                        pid_idx = i
                                        break

                                if pid_idx is None:
                                    continue

                                pid = int(parts[pid_idx])
                                if pid in existing_pids:
                                    continue

                                # Type is after N/A N/A or similar
                                proc_type = parts[pid_idx + 1] if pid_idx + 1 < len(parts) else "G"
                                if proc_type not in ["C", "G", "C+G"]:
                                    proc_type = "G"

                                # Memory is last item, like "99MiB"
                                mem_str = parts[-1]
                                memory = 0.0
                                if "MiB" in mem_str:
                                    memory = float(mem_str.replace("MiB", ""))

                                # Process name is between type and memory
                                name_parts = parts[pid_idx + 2:-1]
                                name = " ".join(name_parts) if name_parts else f"PID {pid}"
                                # Get just the executable name
                                if "/" in name:
                                    name = name.split("/")[-1]
                                if len(name) > 25:
                                    name = name[:22] + "..."

                                processes.append(GPUProcessStats(
                                    pid=pid,
                                    name=name,
                                    gpu_memory_mb=memory,
                                    process_type=proc_type,
                                ))
                            except (ValueError, IndexError):
                                continue

            # Sort by memory usage descending
            processes.sort(key=lambda x: x.gpu_memory_mb, reverse=True)

            self.last_stats = processes
            return processes

        except Exception as e:
            import sys
            print(f"GPU process monitor error: {e}", file=sys.stderr)
            return self.last_stats
