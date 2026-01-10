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

    def get_stats(self) -> List[GPUProcessStats]:
        """Query GPU process statistics via nvidia-smi"""
        if not self._available:
            return []

        try:
            # Query compute and graphics processes
            cmd = [
                "nvidia-smi",
                "--query-compute-apps=pid,process_name,used_memory",
                "--format=csv,noheader,nounits",
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=2)

            processes = []

            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.strip().split("\n"):
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
                                process_type="C",
                            ))
                        except (ValueError, IndexError):
                            continue

            # Also check graphics processes (pmon for more details)
            cmd_pmon = [
                "nvidia-smi", "pmon", "-c", "1", "-s", "um"
            ]
            result_pmon = subprocess.run(cmd_pmon, capture_output=True, text=True, timeout=2)

            if result_pmon.returncode == 0 and result_pmon.stdout.strip():
                existing_pids = {p.pid for p in processes}
                for line in result_pmon.stdout.strip().split("\n"):
                    # Skip header lines
                    if line.startswith("#") or not line.strip():
                        continue
                    parts = line.split()
                    if len(parts) >= 8:
                        try:
                            pid = int(parts[1])
                            if pid == 0 or pid in existing_pids:
                                continue
                            proc_type = parts[2]  # C, G, or C+G
                            mem = float(parts[3]) if parts[3] != "-" else 0.0
                            # Get process name from the end
                            name = parts[-1] if len(parts) > 8 else f"PID {pid}"
                            if len(name) > 25:
                                name = name[:22] + "..."
                            processes.append(GPUProcessStats(
                                pid=pid,
                                name=name,
                                gpu_memory_mb=mem,
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
