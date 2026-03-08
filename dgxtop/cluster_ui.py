#!/usr/bin/env python3
"""Cluster view UI for DGXTOP — side-by-side node panels with drill-down"""

import sys
import os
from typing import Dict, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rich.console import Group
from rich.layout import Layout
from rich.panel import Panel
from rich.style import Style
from rich.table import Table
from rich.text import Text

from config import AppConfig
from cluster_monitor import NodeSnapshot

# Block/shade characters re-used from rich_ui
_BAR_FULL = "█"
_BAR_EMPTY = "░"


def _theme(config: AppConfig) -> dict:
    themes = {
        "green": {"primary": "green", "bar_complete": "green", "bar_empty": "bright_black"},
        "amber": {"primary": "yellow", "bar_complete": "yellow", "bar_empty": "bright_black"},
        "blue":  {"primary": "cyan",   "bar_complete": "cyan",   "bar_empty": "bright_black"},
    }
    return themes.get(config.color_theme, themes["green"])


def _make_bar(percent: float, width: int, theme: dict) -> Text:
    filled = max(0, min(width, int(width * percent / 100)))
    empty = width - filled
    bar = Text()
    bar.append(_BAR_FULL * filled, style=theme["bar_complete"])
    bar.append(_BAR_EMPTY * empty, style=theme["bar_empty"])
    return bar


class ClusterUI:
    """Renders the cluster summary view and per-node drill-down panels.

    The routing between cluster / drill-down is handled externally
    (in DGXTopCluster.run()); this class only renders what it is asked to.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self._theme = _theme(config)

    # ── Public render entry-points ─────────────────────────────────────────

    def render_cluster_summary(self, snapshots: Dict[str, NodeSnapshot]) -> Any:
        """Three equal-width node panels side by side"""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=1),
            Layout(name="nodes"),
            Layout(name="footer", size=3),
        )

        # Header
        layout["header"].update(
            Text(
                "DGXTOP — Cluster View",
                style=f"bold {self._theme['primary']}",
                justify="center",
            )
        )

        # Node panels
        node_panels = [
            self._build_node_panel(name, snap)
            for name, snap in snapshots.items()
        ]

        if len(node_panels) == 1:
            layout["nodes"].update(node_panels[0])
        elif len(node_panels) >= 2:
            sub = Layout()
            sub.split_row(*[Layout(p) for p in node_panels])
            layout["nodes"].update(sub)
        else:
            layout["nodes"].update(Text("No nodes configured", style="dim"))

        # Footer — key hints
        hint = Text()
        for i, name in enumerate(snapshots.keys(), 1):
            hint.append(str(i), style="reverse")
            hint.append(f":{name}  ", style="dim")
        hint.append("ESC", style="reverse")
        hint.append("Cluster  ", style="dim")
        hint.append("+/-", style="reverse")
        hint.append("Speed  ", style="dim")
        hint.append("q", style="reverse")
        hint.append("Quit", style="dim")

        layout["footer"].update(
            Panel(hint, border_style=self._theme["primary"], padding=(0, 1))
        )

        return layout

    def render_node_detail(self, snapshot: NodeSnapshot) -> Any:
        """Expanded single-node view rendered from a snapshot dict.

        Used for remote node drill-down (local drill-down goes through the
        full RichUI path in DGXTopCluster).
        """
        stats = snapshot.stats
        node_name = snapshot.node_name
        status = snapshot.status

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=1),
            Layout(name="top", size=8),
            Layout(name="middle", size=6),
            Layout(name="procs", size=10),
            Layout(name="footer", size=3),
        )

        layout["header"].update(
            Text(
                f"DGXTOP — {node_name}",
                style=f"bold {self._theme['primary']}",
                justify="center",
            )
        )

        if status == "disconnected" or not stats or status == "connecting":
            msg = Text(
                f"Node '{node_name}' is {status}",
                style="bold red",
                justify="center",
            )
            layout["top"].update(Panel(msg, border_style="red"))
            layout["middle"].update(Text(""))
            layout["procs"].update(Text(""))
        else:
            # Top: GPU | CPU+Mem
            layout["top"].split_row(
                Layout(name="gpu"),
                Layout(name="cpumem"),
            )
            layout["top"]["gpu"].update(self._build_detail_gpu_panel(stats))
            layout["top"]["cpumem"].update(self._build_detail_cpumem_panel(stats))

            # Middle: load + mem detail
            layout["middle"].update(self._build_detail_mem_panel(stats))

            # Processes
            layout["procs"].update(self._build_detail_proc_panel(stats))

        # Footer
        hint = Text()
        hint.append("ESC", style="reverse")
        hint.append("Back to cluster  ", style="dim")
        hint.append("+/-", style="reverse")
        hint.append("Speed  ", style="dim")
        hint.append("q", style="reverse")
        hint.append("Quit", style="dim")
        layout["footer"].update(
            Panel(hint, border_style=self._theme["primary"], padding=(0, 1))
        )

        return layout

    # ── Cluster-summary node panel ─────────────────────────────────────────

    def _build_node_panel(self, node_name: str, snapshot: NodeSnapshot) -> Panel:
        stats = snapshot.stats
        status = snapshot.status

        if status in ("disconnected", "connecting") or not stats:
            label = status.upper()
            style = "bold red" if status == "disconnected" else "bold yellow"
            content = Text(f"\n  {label}\n", style=style, justify="center")
            return Panel(
                content,
                title=f"[bold]{node_name}[/bold]",
                border_style="red" if status == "disconnected" else "yellow",
                padding=(0, 1),
            )

        lines = []

        # Status badge
        badge = Text()
        badge.append(" OK ", style="bold black on green")
        lines.append(badge)

        # GPU
        gpu = stats.get("gpu", {})
        gpu_util = gpu.get("util", 0.0)
        gpu_temp = gpu.get("temp", 0.0)
        gpu_pwr = gpu.get("power", 0.0)
        gpu_lim = gpu.get("power_limit", 100.0)
        gpu_name = gpu.get("name", "GPU")

        gpu_text = Text()
        gpu_text.append(f"GPU {gpu_util:5.1f}% ", style=self._theme["primary"])
        gpu_text.append("[")
        gpu_text.append(_make_bar(gpu_util, 14, self._theme))
        gpu_text.append(f"] {gpu_temp:.0f}°C")
        lines.append(gpu_text)

        pwr_text = Text()
        pwr_text.append(f"Pwr {gpu_pwr:5.1f}W / {gpu_lim:.0f}W", style="dim")
        lines.append(pwr_text)

        # CPU
        cpu_pct = stats.get("cpu_usage", 0.0)
        cpu_temp = stats.get("cpu_temp", 0.0)

        cpu_text = Text()
        cpu_text.append(f"CPU {cpu_pct:5.1f}% ", style=self._theme["primary"])
        cpu_text.append("[")
        cpu_text.append(_make_bar(cpu_pct, 14, self._theme))
        cpu_text.append(f"] {cpu_temp:.0f}°C")
        lines.append(cpu_text)

        # Memory
        mem = stats.get("memory", {})
        mem_pct = mem.get("percent", 0.0)
        mem_used = mem.get("used_gb", 0.0)
        mem_tot = mem.get("total_gb", 0.0)

        mem_text = Text()
        mem_text.append(f"MEM {mem_pct:5.1f}% ", style=self._theme["primary"])
        mem_text.append("[")
        mem_text.append(_make_bar(mem_pct, 14, self._theme))
        mem_text.append(f"] {mem_used:.1f}/{mem_tot:.0f}G")
        lines.append(mem_text)

        # Load average
        load = stats.get("loadavg", [0.0, 0.0, 0.0])
        load_text = Text()
        load_text.append(
            f"Load {load[0]:.2f} {load[1]:.2f} {load[2]:.2f}",
            style="dim",
        )
        lines.append(load_text)

        # Top 3 processes
        processes = stats.get("processes", [])
        if processes:
            lines.append(Text("Processes:", style="dim"))
            for proc in processes[:3]:
                pid = proc.get("pid", 0)
                user = (proc.get("user") or "?")[:8]
                mem_mb = proc.get("gpu_mem_mb", 0.0)
                cmd = (proc.get("command") or "?")[:12]
                pt = Text()
                pt.append(f"  {pid:6d} ", style="dim")
                pt.append(f"{user:<8} ", style=self._theme["primary"])
                pt.append(f"{mem_mb:6.0f}M ", style="dim")
                pt.append(cmd, style="dim")
                lines.append(pt)
        else:
            lines.append(Text("  (no GPU processes)", style="dim"))

        content = Group(*lines)
        return Panel(
            content,
            title=f"[bold]{node_name}[/bold]",
            border_style=self._theme["primary"],
            padding=(0, 1),
        )

    # ── Detail panel builders (remote drill-down) ──────────────────────────

    def _build_detail_gpu_panel(self, stats: dict) -> Panel:
        gpu = stats.get("gpu", {})
        util = gpu.get("util", 0.0)
        temp = gpu.get("temp", 0.0)
        pwr = gpu.get("power", 0.0)
        lim = gpu.get("power_limit", 100.0)
        name = gpu.get("name", "GPU")
        clock = gpu.get("clock", 0.0)
        clock_max = gpu.get("clock_max", 0.0)

        lines = []

        # Util bar
        ut = Text()
        ut.append(f"Usage: {util:5.1f}% ", style=self._theme["primary"])
        ut.append("[")
        ut.append(_make_bar(util, 25, self._theme))
        ut.append("]")
        lines.append(ut)

        # Temp
        tt = Text()
        tt.append(f"Temp:  {temp:5.1f}°C", style=self._theme["primary"])
        lines.append(tt)

        # Power
        pt = Text()
        pt.append(f"Power: {pwr:5.1f}W / {lim:.0f}W", style=self._theme["primary"])
        lines.append(pt)

        # Frequency
        if clock_max > 0:
            ft = Text()
            ft.append(f"Freq:  {clock:5.0f} / {clock_max:.0f} MHz", style=self._theme["primary"])
            lines.append(ft)

        title = f"[bold]GPU ({name})[/bold]" if name else "[bold]GPU[/bold]"
        return Panel(Group(*lines), title=title, border_style=self._theme["primary"], padding=(0, 1))

    def _build_detail_cpumem_panel(self, stats: dict) -> Panel:
        cpu_pct = stats.get("cpu_usage", 0.0)
        cpu_temp = stats.get("cpu_temp", 0.0)

        lines = []

        ut = Text()
        ut.append(f"Usage: {cpu_pct:5.1f}% ", style=self._theme["primary"])
        ut.append("[")
        ut.append(_make_bar(cpu_pct, 20, self._theme))
        ut.append("]")
        lines.append(ut)

        tt = Text()
        tt.append(f"Temp:  {cpu_temp:5.1f}°C", style=self._theme["primary"])
        lines.append(tt)

        load = stats.get("loadavg", [0.0, 0.0, 0.0])
        lt = Text()
        lt.append(f"Load:  {load[0]:.2f}  {load[1]:.2f}  {load[2]:.2f}", style="dim")
        lines.append(lt)

        return Panel(
            Group(*lines),
            title="[bold]CPU[/bold]",
            border_style=self._theme["primary"],
            padding=(0, 1),
        )

    def _build_detail_mem_panel(self, stats: dict) -> Panel:
        mem = stats.get("memory", {})
        pct = mem.get("percent", 0.0)
        used = mem.get("used_gb", 0.0)
        tot = mem.get("total_gb", 0.0)

        lines = []

        mt = Text()
        mt.append(f"RAM: {used:5.1f} / {tot:.0f} GB ", style=self._theme["primary"])
        mt.append("[")
        mt.append(_make_bar(pct, 30, self._theme))
        mt.append(f"] {pct:.1f}%")
        lines.append(mt)

        return Panel(
            Group(*lines),
            title="[bold]Memory[/bold]",
            border_style=self._theme["primary"],
            padding=(0, 1),
        )

    def _build_detail_proc_panel(self, stats: dict) -> Panel:
        processes = stats.get("processes", [])

        table = Table(
            show_header=True,
            header_style=f"bold {self._theme['primary']}",
            box=None,
            padding=(0, 1),
        )
        table.add_column("PID", justify="right", style="bold")
        table.add_column("User")
        table.add_column("GPU MEM", justify="right")
        table.add_column("GPU %", justify="right")
        table.add_column("Command")

        for proc in processes[:6]:
            pid = str(proc.get("pid", "?"))
            user = (proc.get("user") or "?")[:10]
            mem_mb = proc.get("gpu_mem_mb", 0.0)
            gpu_util = proc.get("gpu_util", proc.get("gpu_util", 0))
            cmd = (proc.get("command") or "?")[:40]
            table.add_row(pid, user, f"{mem_mb:.0f} MiB", f"{gpu_util}%", cmd)

        if not processes:
            table.add_row("-", "-", "-", "-", "No GPU processes running")

        return Panel(
            table,
            title="[bold]GPU Processes[/bold]",
            border_style=self._theme["primary"],
            padding=(0, 1),
        )
