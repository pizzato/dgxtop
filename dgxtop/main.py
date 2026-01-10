#!/usr/bin/env python3
"""Main application for DGXTOP Ubuntu - DGX SPARK Edition

Uses rich library for SSH-compatible terminal UI.
"""

import time
import sys
import os
import signal
import select
import termios
import tty
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

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        log_system_info()
        self.logger.log_info("DGXTOP DGX SPARK initialized")

    def _handle_signal(self, signum, frame):
        """Handle termination signals gracefully"""
        self.running = False

    def _check_keyboard(self) -> str | None:
        """Check for keyboard input without blocking"""
        if select.select([sys.stdin], [], [], 0)[0]:
            return sys.stdin.read(1)
        return None

    def _handle_key(self, key: str):
        """Handle keyboard input"""
        if key == 'q':
            self.running = False
        elif key == '+' or key == '=':
            # Speed up (decrease interval), minimum 0.1 seconds
            self.config.update_interval = max(0.1, self.config.update_interval - 0.1)
        elif key == '-':
            # Slow down (increase interval), maximum 5 seconds
            self.config.update_interval = min(5.0, self.config.update_interval + 0.1)

    def collect_stats(self) -> dict:
        """Collect all system statistics"""
        stats = self.system_monitor.get_stats()

        # GPU stats
        gpu_stats = self.gpu_monitor.get_stats()
        if gpu_stats:
            stats["gpu"] = gpu_stats

        # GPU processes
        gpu_processes = self.gpu_processes_monitor.get_stats()
        stats["gpu_processes"] = gpu_processes

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
            # Save terminal settings and set to raw mode for keyboard input
            old_settings = termios.tcgetattr(sys.stdin)

        try:
            if has_tty:
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
                            key = self._check_keyboard()
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


def main():
    """Entry point"""
    from dgxtop import __version__

    parser = argparse.ArgumentParser(
        prog="dgxtop",
        description="System monitor for NVIDIA DGX Spark - real-time CPU, GPU, memory, disk, and network monitoring",
    )
    parser.add_argument(
        "-i", "--interval",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="Update interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "-v", "--version",
        action="version",
        version=f"dgxtop {__version__}",
    )

    args = parser.parse_args()

    console = Console()

    try:
        app = DGXTop()
        app.config.update_interval = args.interval
        app.run()
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
