#!/usr/bin/env python3
"""Background thread-based cluster monitor for DGXTOP"""

import threading
import time
import sys
import os
from dataclasses import dataclass, field
from typing import Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cluster_config import ClusterConfig, NodeConfig
from remote_collector import RemoteCollector


@dataclass
class NodeSnapshot:
    """Latest data snapshot for one cluster node"""
    node_name: str
    stats: dict = field(default_factory=dict)
    status: str = "connecting"  # "ok" | "disconnected" | "connecting" | "error"


class LocalCollector:
    """Collect stats from the local machine using the existing monitors.

    Returns data in the same dict format as RemoteCollector so ClusterUI
    can render all nodes uniformly.
    """

    def __init__(self):
        from gpu_monitor import GPUMonitor
        from gpu_processes_monitor import GPUProcessMonitor
        from system_monitor import SystemMonitor

        self._gpu = GPUMonitor()
        self._gpu_procs = GPUProcessMonitor()
        self._sys = SystemMonitor()

    def collect_snapshot(self) -> dict:
        sys_stats = self._sys.get_stats()
        gpu = self._gpu.get_stats()
        procs = self._gpu_procs.get_stats()

        result: dict = {"status": "ok"}

        # CPU
        cpu = sys_stats.get("cpu")
        result["cpu_usage"] = cpu.usage_percent if cpu else 0.0
        result["cpu_temp"] = cpu.temperature_celsius if cpu else 0.0

        # Memory
        mem = sys_stats.get("memory")
        if mem:
            result["memory"] = {
                "total_gb": mem.total / 1073741824.0,
                "used_gb": mem.used / 1073741824.0,
                "percent": mem.usage_percent,
            }
        else:
            result["memory"] = {"total_gb": 0.0, "used_gb": 0.0, "percent": 0.0}

        # GPU
        if gpu:
            result["gpu"] = {
                "index": gpu.index,
                "name": gpu.name,
                "util": gpu.utilization_gpu,
                "temp": gpu.temperature,
                "power": gpu.power_draw,
                "power_limit": gpu.power_limit,
                "clock": gpu.clock_graphics,
                "clock_max": gpu.clock_max,
            }
        else:
            result["gpu"] = {
                "util": 0.0, "temp": 0.0,
                "power": 0.0, "power_limit": 100.0,
                "name": "N/A",
            }

        # Top GPU processes (top 3 for cluster summary)
        result["processes"] = [
            {
                "pid": p.pid,
                "user": p.user,
                "gpu_mem_mb": p.gpu_memory_mb,
                "gpu_util": p.gpu_util,
                "command": p.command,
            }
            for p in procs[:3]
        ]

        # Load average
        try:
            with open("/proc/loadavg") as f:
                parts = f.read().split()
            result["loadavg"] = [float(parts[0]), float(parts[1]), float(parts[2])]
        except Exception:
            result["loadavg"] = [0.0, 0.0, 0.0]

        return result


class ClusterMonitor:
    """Manages background polling threads for all cluster nodes.

    One thread per node; local node uses LocalCollector, remote nodes use
    RemoteCollector (SSH + inline Python script).
    """

    def __init__(self, config: ClusterConfig):
        self.config = config
        self.update_interval: float = config.update_interval
        self._snapshots: Dict[str, NodeSnapshot] = {}
        self._lock = threading.Lock()
        self._threads = []
        self._running = False

    def start(self) -> None:
        """Start background polling threads for all nodes"""
        self._running = True

        for node in self.config.nodes:
            # Initialise with "connecting" status so the UI has something to show
            with self._lock:
                self._snapshots[node.name] = NodeSnapshot(
                    node_name=node.name, status="connecting"
                )

            if node.local:
                collector = LocalCollector()
                t = threading.Thread(
                    target=self._local_loop,
                    args=(node.name, collector),
                    daemon=True,
                    name=f"dgxtop-local-{node.name}",
                )
            else:
                collector = RemoteCollector(node)
                t = threading.Thread(
                    target=self._remote_loop,
                    args=(node.name, collector),
                    daemon=True,
                    name=f"dgxtop-remote-{node.name}",
                )

            t.start()
            self._threads.append(t)

    def stop(self) -> None:
        """Signal all threads to stop and disconnect remote collectors"""
        self._running = False

    def get_snapshots(self) -> Dict[str, NodeSnapshot]:
        """Thread-safe read of the latest snapshots for all nodes.

        Returns a new dict preserving insertion order (node config order).
        """
        with self._lock:
            # Preserve node order from config
            ordered = {}
            for node in self.config.nodes:
                if node.name in self._snapshots:
                    ordered[node.name] = self._snapshots[node.name]
            return ordered

    # ── Private thread targets ─────────────────────────────────────────────

    def _local_loop(self, node_name: str, collector: LocalCollector) -> None:
        while self._running:
            try:
                data = collector.collect_snapshot()
                with self._lock:
                    self._snapshots[node_name] = NodeSnapshot(
                        node_name=node_name,
                        stats=data,
                        status="ok",
                    )
            except Exception as e:
                with self._lock:
                    self._snapshots[node_name] = NodeSnapshot(
                        node_name=node_name,
                        stats={"status": "error", "error": str(e)},
                        status="error",
                    )
            time.sleep(self.update_interval)

    def _remote_loop(self, node_name: str, collector: RemoteCollector) -> None:
        retry_delay = 5.0

        while self._running:
            # Connect (or reconnect)
            if not collector.is_connected:
                try:
                    collector.connect()
                except Exception as e:
                    with self._lock:
                        self._snapshots[node_name] = NodeSnapshot(
                            node_name=node_name,
                            stats={"status": "disconnected", "error": str(e)},
                            status="disconnected",
                        )
                    time.sleep(retry_delay)
                    continue

            # Collect
            try:
                data = collector.collect_snapshot()
                status = data.get("status", "ok")
                with self._lock:
                    self._snapshots[node_name] = NodeSnapshot(
                        node_name=node_name,
                        stats=data,
                        status=status,
                    )
                if status == "disconnected":
                    collector.disconnect()
                    time.sleep(retry_delay)
                    continue
            except Exception as e:
                collector.disconnect()
                with self._lock:
                    self._snapshots[node_name] = NodeSnapshot(
                        node_name=node_name,
                        stats={"status": "disconnected", "error": str(e)},
                        status="disconnected",
                    )
                time.sleep(retry_delay)
                continue

            time.sleep(self.update_interval)
