#!/usr/bin/env python3
"""Cluster configuration for DGXTOP multi-node monitoring"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class NodeConfig:
    """Configuration for a single cluster node"""
    name: str
    host: str
    user: str = "dgx"
    port: int = 22
    local: bool = False


@dataclass
class ClusterConfig:
    """Configuration for the full cluster"""
    nodes: List[NodeConfig] = field(default_factory=list)
    update_interval: float = 1.0


def default_config_path() -> str:
    """Return the default cluster config file path"""
    return os.path.expanduser("~/.config/dgxtop/cluster.toml")


def load_cluster_config(path: str) -> ClusterConfig:
    """Load cluster config from a TOML file"""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            raise ImportError(
                "TOML support requires Python 3.11+ or 'tomli' package. "
                "Install with: pip install tomli"
            )

    with open(path, "rb") as f:
        data = tomllib.load(f)

    cluster_data = data.get("cluster", {})
    update_interval = float(cluster_data.get("update_interval", 1.0))

    nodes = []
    for node_data in cluster_data.get("nodes", []):
        nodes.append(NodeConfig(
            name=node_data.get("name", node_data.get("host", "unknown")),
            host=node_data.get("host", "localhost"),
            user=node_data.get("user", "dgx"),
            port=int(node_data.get("port", 22)),
            local=bool(node_data.get("local", False)),
        ))

    return ClusterConfig(nodes=nodes, update_interval=update_interval)


def config_from_hostnames(hostnames: List[str]) -> ClusterConfig:
    """Build a ClusterConfig from a list of hostnames.

    The first hostname is assumed to be the local node.
    """
    nodes = []
    for i, host in enumerate(hostnames):
        nodes.append(NodeConfig(
            name=host,
            host=host,
            user="dgx",
            port=22,
            local=(i == 0),
        ))
    return ClusterConfig(nodes=nodes)


EXAMPLE_CLUSTER_TOML = """\
[cluster]
update_interval = 1.0

[[cluster.nodes]]
name = "spark-1"
host = "spark-1"
user = "dgx"
local = true

[[cluster.nodes]]
name = "spark-2"
host = "spark-2"
user = "dgx"

[[cluster.nodes]]
name = "spark-3"
host = "spark-3"
user = "dgx"
"""


def write_example_config(path: str) -> None:
    """Write an example cluster config to the given path"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(EXAMPLE_CLUSTER_TOML)
