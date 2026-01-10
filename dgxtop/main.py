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
        """Read keyboard input using select for non-blocking check"""
        # Check if there's input available
        if not select.select([sys.stdin], [], [], 0)[0]:
            return None

        try:
            ch = sys.stdin.read(1)
            if not ch:
                return None

            # Check for escape sequence (arrows, function keys)
            if ch == '\x1b':
                # Wait briefly for more characters
                seq = ''
                while select.select([sys.stdin], [], [], 0.05)[0]:
                    c = sys.stdin.read(1)
                    if c:
                        seq += c
                    if len(seq) > 10:
                        break

                # Parse escape sequences
                if seq == '[A' or seq == 'OA':
                    return 'UP'
                elif seq == '[B' or seq == 'OB':
                    return 'DOWN'
                elif seq == '[C' or seq == 'OC':
                    return 'RIGHT'
                elif seq == '[D' or seq == 'OD':
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
            self.logger.log_error(e, "Key read error")
            return None

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
