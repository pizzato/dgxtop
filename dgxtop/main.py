#!/usr/bin/env python3
"""Main application for DGXTOP Ubuntu - DGX SPARK Edition

Uses rich library for SSH-compatible terminal UI.
"""

import time
import sys
import os
import signal
import termios
import tty
import select
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rich.live import Live
from rich.console import Console

from config import AppConfig
from gpu_monitor import GPUMonitor
from gpu_processes_monitor import GPUProcessMonitor
from system_monitor import SystemMonitor
from disk_monitor import DiskMonitor
from network_monitor import NetworkMonitor
from rich_ui import RichUI
from logger import get_logger, log_system_info


# Sort column options
SORT_COLUMNS = ["gpu_memory_mb", "gpu_util", "cpu_percent", "host_memory_mb", "pid"]
SORT_NAMES = ["GPU MEM", "GPU %", "CPU %", "HOST MEM", "PID"]


def _read_key(logger=None) -> str | None:
    """Read a single keypress from stdin (non-blocking).

    Returns a human-readable key name or None if no input.
    """
    if not select.select([sys.stdin], [], [], 0)[0]:
        return None

    try:
        ch = sys.stdin.read(1)
        if not ch:
            return None

        if ch == '\x1b':
            seq = ''
            while select.select([sys.stdin], [], [], 0.05)[0]:
                c = sys.stdin.read(1)
                if c:
                    seq += c
                if len(seq) > 10:
                    break

            if seq in ('[A', 'OA'):
                return 'UP'
            elif seq in ('[B', 'OB'):
                return 'DOWN'
            elif seq in ('[C', 'OC'):
                return 'RIGHT'
            elif seq in ('[D', 'OD'):
                return 'LEFT'
            elif seq == '[17~':
                return 'F6'
            elif seq == '[20~':
                return 'F9'
            elif seq == '[21~':
                return 'F10'

            return 'ESC'

        return ch

    except (IOError, OSError) as e:
        if logger:
            logger.log_error(e, "Key read error")
        return None


class DGXTop:
    """Main DGXTOP application for DGX SPARK"""

    def __init__(self):
        self.config = AppConfig()
        self.console = Console()
        self.gpu_monitor = GPUMonitor()
        self.gpu_processes_monitor = GPUProcessMonitor()
        self.system_monitor = SystemMonitor()
        self.disk_monitor = DiskMonitor()
        self.network_monitor = NetworkMonitor()
        self.ui = RichUI(self.config)
        self.logger = get_logger()
        self.running = True

        # Process selection and interaction state
        self.selected_process_idx = 0
        self.sort_column_idx = 0  # Default sort by GPU memory
        self.sort_ascending = False
        self.kill_mode = False
        self.sort_mode = False
        self.last_processes = []

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        log_system_info()
        self.logger.log_info("DGXTOP DGX SPARK initialized")

    def _handle_signal(self, signum, frame):
        """Handle termination signals gracefully"""
        self.running = False

    def _read_key(self) -> str | None:
        return _read_key(self.logger)

    def _handle_key(self, key: str):
        """Handle keyboard input"""
        if key is None:
            return

        # Handle mode-specific keys
        if self.kill_mode:
            if key == 'ESC' or key == 'K' or key == 'n':
                self.kill_mode = False
            elif key == '\n' or key == '\r' or key == 'y':
                self._kill_selected_process()
                self.kill_mode = False
            elif key == 'UP' or key == 'k':
                self.selected_process_idx = max(0, self.selected_process_idx - 1)
            elif key == 'DOWN' or key == 'j':
                max_idx = max(0, len(self.last_processes) - 1)
                self.selected_process_idx = min(max_idx, self.selected_process_idx + 1)
            return

        if self.sort_mode:
            if key == 'ESC' or key == 's':
                self.sort_mode = False
            elif key == 'UP' or key == 'k':
                self.sort_column_idx = (self.sort_column_idx - 1) % len(SORT_COLUMNS)
            elif key == 'DOWN' or key == 'j':
                self.sort_column_idx = (self.sort_column_idx + 1) % len(SORT_COLUMNS)
            elif key == '\n' or key == '\r':
                self.sort_mode = False
            elif key == '+' or key == '-':
                self.sort_ascending = not self.sort_ascending
            return

        # Normal mode keys
        if key == 'q' or key == 'F10':
            self.running = False
        elif key == '+' or key == '=':
            self.config.update_interval = max(0.1, self.config.update_interval - 0.1)
        elif key == '-':
            self.config.update_interval = min(5.0, self.config.update_interval + 0.1)
        elif key == 'UP' or key == 'k':
            self.selected_process_idx = max(0, self.selected_process_idx - 1)
        elif key == 'DOWN' or key == 'j':
            max_idx = max(0, len(self.last_processes) - 1)
            self.selected_process_idx = min(max_idx, self.selected_process_idx + 1)
        elif key == 'F6' or key == 's':
            self.sort_mode = True
        elif key == 'F9' or key == 'K':
            self.kill_mode = True

    def _kill_selected_process(self):
        """Send SIGTERM to selected process"""
        if not self.last_processes or self.selected_process_idx >= len(self.last_processes):
            return

        proc = self.last_processes[self.selected_process_idx]
        try:
            os.kill(proc.pid, signal.SIGTERM)
            self.logger.log_info(f"Sent SIGTERM to PID {proc.pid}")
        except ProcessLookupError:
            self.logger.log_info(f"Process {proc.pid} not found")
        except PermissionError:
            self.logger.log_error(None, f"Permission denied to kill PID {proc.pid}")

    def collect_stats(self) -> dict:
        """Collect all system statistics"""
        stats = self.system_monitor.get_stats()

        # GPU stats
        gpu_stats = self.gpu_monitor.get_stats()
        if gpu_stats:
            stats["gpu"] = gpu_stats

        # GPU processes
        gpu_processes = self.gpu_processes_monitor.get_stats()

        # Sort processes
        sort_key = SORT_COLUMNS[self.sort_column_idx]
        gpu_processes.sort(key=lambda x: getattr(x, sort_key), reverse=not self.sort_ascending)

        self.last_processes = gpu_processes
        stats["gpu_processes"] = gpu_processes

        # UI state
        stats["selected_idx"] = self.selected_process_idx
        stats["sort_column"] = SORT_NAMES[self.sort_column_idx]
        stats["sort_ascending"] = self.sort_ascending
        stats["kill_mode"] = self.kill_mode
        stats["sort_mode"] = self.sort_mode

        # Disk stats with latency
        disk_stats = self.disk_monitor.get_device_stats_for_display()
        stats["disk"] = disk_stats

        # Disk history for sparklines
        stats["disk_history"] = self.disk_monitor.get_history()

        # Network stats
        network_stats = self.network_monitor.get_interface_stats_for_display()
        stats["network_io"] = network_stats

        # Network history for sparklines (future use)
        stats["network_history"] = self.network_monitor.get_history()

        return stats

    def run(self):
        """Main application loop using rich Live display"""
        self.logger.log_info("Starting main loop")

        # Check if we have a TTY for keyboard input
        has_tty = sys.stdin.isatty()
        old_settings = None

        if has_tty:
            # Save terminal settings
            old_settings = termios.tcgetattr(sys.stdin)

        try:
            if has_tty:
                # Set terminal to cbreak mode (character-by-character input)
                tty.setcbreak(sys.stdin.fileno())

            # Use rich Live for real-time updates
            with Live(
                self.ui.get_renderable({}),
                console=self.console,
                refresh_per_second=1,
                screen=True,  # Use alternate screen buffer
            ) as live:
                while self.running:
                    start = time.time()

                    try:
                        # Check for keyboard input (only if TTY available)
                        if has_tty:
                            key = self._read_key()
                            if key:
                                self._handle_key(key)

                        # Collect stats
                        stats = self.collect_stats()

                        # Update the live display
                        live.update(self.ui.get_renderable(stats))

                    except Exception as e:
                        self.logger.log_error(e, "Stats collection")

                    # Maintain update interval
                    elapsed = time.time() - start
                    sleep_time = max(0, self.config.update_interval - elapsed)
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            pass
        finally:
            # Restore terminal settings if we modified them
            if has_tty and old_settings:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
            self.logger.log_info("DGXTOP shutdown")


# ── Cluster mode ───────────────────────────────────────────────────────────


class DGXTopCluster:
    """Cluster-mode DGXTOP: monitors multiple nodes side by side.

    Key bindings:
      1 / 2 / 3 — drill into node N (full view for local, detail panel for remote)
      ESC        — return to cluster summary
      +  / -     — adjust poll interval for all nodes
      q  / F10   — quit
    """

    def __init__(self, cluster_config):
        from cluster_config import ClusterConfig
        from cluster_monitor import ClusterMonitor
        from cluster_ui import ClusterUI

        self.cluster_config = cluster_config
        self.config = AppConfig()
        self.config.update_interval = cluster_config.update_interval
        self.console = Console()
        self.running = True

        # active_view: "cluster" or a node name
        self.active_view = "cluster"

        # Background data collection for all nodes
        self.monitor = ClusterMonitor(cluster_config)

        # Cluster summary / remote drill-down renderer
        self.cluster_ui = ClusterUI(self.config)

        # Full single-node renderer for the local node (preserves sparkline history)
        self._local_node_name: str | None = None
        self._local_app: DGXTop | None = None
        for node in cluster_config.nodes:
            if node.local:
                self._local_node_name = node.name
                self._local_app = DGXTop()
                # Suppress the local app's own signal handlers — we manage them here
                signal.signal(signal.SIGINT, self._handle_signal)
                signal.signal(signal.SIGTERM, self._handle_signal)
                break

        if self._local_app is None:
            # No local node in config; still need signal handlers
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        self.running = False

    def _read_key(self) -> str | None:
        return _read_key()

    def _handle_key(self, key: str) -> None:
        if key is None:
            return

        # In local drill-down, pass sort/kill/navigation to the local app
        if self.active_view == self._local_node_name and self._local_app:
            if key == 'ESC':
                self.active_view = "cluster"
            elif key == 'q' or key == 'F10':
                self.running = False
            else:
                self._local_app._handle_key(key)
            return

        # Global / cluster-view keys
        if key == 'q' or key == 'F10':
            self.running = False
        elif key == 'ESC':
            self.active_view = "cluster"
        elif key == '+' or key == '=':
            self.config.update_interval = max(0.1, self.config.update_interval - 0.1)
            self.monitor.update_interval = self.config.update_interval
        elif key == '-':
            self.config.update_interval = min(5.0, self.config.update_interval + 0.1)
            self.monitor.update_interval = self.config.update_interval
        else:
            # Number keys → drill into node
            for i, node in enumerate(self.cluster_config.nodes, 1):
                if key == str(i):
                    self.active_view = node.name
                    break

    def run(self) -> None:
        """Main loop — same structure as DGXTop.run()"""
        self.monitor.start()

        has_tty = sys.stdin.isatty()
        old_settings = None
        if has_tty:
            old_settings = termios.tcgetattr(sys.stdin)

        try:
            if has_tty:
                tty.setcbreak(sys.stdin.fileno())

            with Live(
                self.cluster_ui.render_cluster_summary({}),
                console=self.console,
                refresh_per_second=1,
                screen=True,
            ) as live:
                while self.running:
                    start = time.time()

                    try:
                        if has_tty:
                            key = self._read_key()
                            if key:
                                self._handle_key(key)

                        renderable = self._build_renderable()
                        live.update(renderable)

                    except Exception:
                        pass  # Keep running; errors logged by sub-components

                    elapsed = time.time() - start
                    sleep_time = max(0, self.config.update_interval - elapsed)
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            pass
        finally:
            self.monitor.stop()
            if has_tty and old_settings:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)

    def _build_renderable(self):
        """Choose what to render based on active_view"""
        snapshots = self.monitor.get_snapshots()

        if self.active_view == "cluster":
            return self.cluster_ui.render_cluster_summary(snapshots)

        # Drill-down for local node: use full RichUI with live dataclass stats
        if self.active_view == self._local_node_name and self._local_app:
            stats = self._local_app.collect_stats()
            return self._local_app.ui.get_renderable(stats)

        # Drill-down for remote node: use snapshot-based detail panel
        if self.active_view in snapshots:
            return self.cluster_ui.render_node_detail(snapshots[self.active_view])

        # Fallback
        return self.cluster_ui.render_cluster_summary(snapshots)


# ── Entry point ────────────────────────────────────────────────────────────


def main():
    """Entry point"""
    from dgxtop import __version__

    parser = argparse.ArgumentParser(
        prog="dgxtop",
        description=(
            "System monitor for NVIDIA DGX Spark — "
            "real-time CPU, GPU, memory, disk, and network monitoring. "
            "Run without cluster flags for single-node mode."
        ),
    )
    parser.add_argument(
        "-i", "--interval",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Update interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "-v", "--version",
        action="version",
        version=f"dgxtop {__version__}",
    )

    # Cluster flags
    cluster_group = parser.add_argument_group("cluster mode")
    cluster_group.add_argument(
        "--cluster",
        nargs="+",
        metavar="HOST",
        help=(
            "Ad-hoc cluster: list of hostnames to monitor side by side. "
            "The first host is assumed to be the local machine."
        ),
    )
    cluster_group.add_argument(
        "--cluster-config",
        metavar="PATH",
        help="Path to cluster.toml (default: ~/.config/dgxtop/cluster.toml)",
    )
    cluster_group.add_argument(
        "--cluster-init",
        action="store_true",
        help="Write an example cluster.toml to the default location and exit.",
    )

    args = parser.parse_args()
    console = Console()

    # ── --cluster-init ─────────────────────────────────────────────────────
    if args.cluster_init:
        from cluster_config import default_config_path, write_example_config
        path = args.cluster_config or default_config_path()
        try:
            write_example_config(path)
            console.print(f"[green]Example cluster config written to:[/green] {path}")
        except Exception as e:
            console.print(f"[red]Error writing config: {e}[/red]")
            sys.exit(1)
        return

    # ── Determine whether to run in cluster mode ───────────────────────────
    cluster_cfg = None

    if args.cluster:
        # Ad-hoc cluster from --cluster HOST [HOST ...]
        from cluster_config import config_from_hostnames
        cluster_cfg = config_from_hostnames(args.cluster)

    elif args.cluster_config:
        # Explicit config file path
        from cluster_config import load_cluster_config
        try:
            cluster_cfg = load_cluster_config(args.cluster_config)
        except FileNotFoundError:
            console.print(f"[red]Cluster config not found: {args.cluster_config}[/red]")
            sys.exit(1)
        except Exception as e:
            console.print(f"[red]Error loading cluster config: {e}[/red]")
            sys.exit(1)

    else:
        # Auto-detect ~/.config/dgxtop/cluster.toml
        from cluster_config import default_config_path, load_cluster_config
        default_path = default_config_path()
        if os.path.exists(default_path):
            try:
                cluster_cfg = load_cluster_config(default_path)
            except Exception as e:
                console.print(
                    f"[yellow]Warning: could not load {default_path}: {e}[/yellow]\n"
                    "[yellow]Falling back to single-node mode.[/yellow]"
                )

    # Override update interval if provided
    if args.interval is not None:
        if cluster_cfg is not None:
            cluster_cfg.update_interval = args.interval

    # ── Launch ─────────────────────────────────────────────────────────────
    try:
        if cluster_cfg is not None and cluster_cfg.nodes:
            app = DGXTopCluster(cluster_cfg)
            app.run()
        else:
            # Single-node mode — unchanged behaviour
            app = DGXTop()
            if args.interval is not None:
                app.config.update_interval = args.interval
            app.run()

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
