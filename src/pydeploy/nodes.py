"""Node types for the PyDeploy DAG framework.

Token   — A token/ref that resolves to a real value at deploy time.
Asset   — Abstract base class for all asset kinds.
Table — Concrete asset with name, schema, params, and resource_id.
AssetNode / JobNode — Internal DAG node wrappers.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable


# ── Token ────────────────────────────────────────────────────

_UNRESOLVED = object()


class Token:
    """A lazy reference to a value that is resolved at deploy time.

    During synthesis (plan), a Token holds a reference string like
    ``${raw_users.resource_id}``. After deployment, the Token is
    resolved to a concrete value.

    Usage::

        out = Token("${my_table.resource_id}")
        str(out)        # → "${my_table.resource_id}"  (unresolved)

        out.resolve("abc-123")
        out.value       # → "abc-123"
        str(out)        # → "abc-123"
    """

    def __init__(self, ref: str = "") -> None:
        self.ref = ref
        self._value: Any = _UNRESOLVED

    @property
    def is_resolved(self) -> bool:
        """True if this output has been resolved to a concrete value."""
        return self._value is not _UNRESOLVED

    @property
    def value(self) -> Any:
        """Return the resolved value.

        Raises:
            RuntimeError: if the output has not been resolved yet.
        """
        if not self.is_resolved:
            raise RuntimeError(
                f"Token '{self.ref}' has not been resolved yet. "
                "Has the asset been deployed?"
            )
        return self._value

    def resolve(self, value: Any) -> None:
        """Resolve this output to a concrete value."""
        self._value = value

    def __str__(self) -> str:
        return str(self._value) if self.is_resolved else self.ref

    def __repr__(self) -> str:
        if self.is_resolved:
            return f"Token({self.ref!r}, value={self._value!r})"
        return f"Token({self.ref!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Token):
            return self.ref == other.ref and self._value == other._value
        if self.is_resolved:
            return self._value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.ref)


# ── User-facing types ───────────────────────────────────────────────


class Asset(ABC):
    """Abstract base class for all asset types.

    Subclass this to define new asset kinds ( Table, Model, etc.).
    Must implement ``to_manifest()`` for plan-phase serialization.
    """

    @abstractmethod
    def to_manifest(self) -> dict[str, Any]:
        """Serialize this asset to a manifest-ready dict."""
        ...


@dataclass
class Table(Asset):
    """A data resource with a name, schema, and team.

    Attributes:
        name: identifier for this table (auto-set by @dag.asset).
        schema: column/field definitions, e.g. {"id": "int", "name": "str"}.
        team: a Token resolved at deploy time identifying which team owns this table.
        resource_id: a Token resolved at deploy time with the provisioned resource ID.
    """

    name: str = ""
    schema: dict[str, str] = field(default_factory=dict)
    team: Token = field(default_factory=Token)
    resource_id: Token = field(default_factory=Token, repr=False)

    def ingest(self, data: Any) -> None:
        """Ingest data into this table.

        Args:
            data: the data to ingest (e.g., a pandas DataFrame).

        Raises:
            RuntimeError: if the table has not been deployed yet.
        """
        if not self.resource_id.is_resolved:
            raise RuntimeError(
                f"Table '{self.name}' has no resource_id. "
                "Has it been deployed?"
            )
        rid = self.resource_id.value
        print(f"  📥 Ingesting into '{self.name}' (resource: {rid})")
        if hasattr(data, "shape"):
            print(f"     DataFrame with shape {data.shape}")
        elif isinstance(data, list):
            print(f"     {len(data)} records")
        else:
            print(f"     Data type: {type(data).__name__}")

    def to_manifest(self) -> dict[str, Any]:
        result: dict[str, Any] = {"type": "table"}
        if self.name:
            result["name"] = self.name
        if self.schema:
            result["schema"] = self.schema
        result["inputs"] = {
            "team": str(self.team),
        }
        result["outputs"] = {
            "resource_id": str(self.resource_id),
        }
        return result


# ── Internal DAG nodes ──────────────────────────────────────────────


class AssetNode:
    """Internal wrapper for a user-defined Asset instance."""

    node_type: str = "asset"

    def __init__(
        self,
        id: str,  # noqa: A002
        *,
        asset: Asset,
        depends_on: list[AssetNode] | None = None,
    ) -> None:
        self.id = id
        self.asset = asset
        self.depends_on: list[AssetNode] = depends_on or []
        self.result: Any = None

    def to_manifest(self) -> dict[str, Any]:
        manifest = self.asset.to_manifest()
        manifest["name"] = self.id
        if self.depends_on:
            manifest["depends_on"] = [dep.id for dep in self.depends_on]
        return manifest


class JobNode:
    """Internal wrapper for a job handler and its asset dependencies.

    Jobs can only depend on assets, not other jobs.
    """

    node_type: str = "job"

    def __init__(
        self,
        id: str,  # noqa: A002
        *,
        handler: Callable[..., Any] | None = None,
        depends_on: list[AssetNode] | None = None,
    ) -> None:
        self.id = id
        self.handler = handler
        self.depends_on: list[AssetNode] = depends_on or []
        self.result: Any = None

    def to_manifest(self) -> dict[str, Any]:
        return {
            "name": self.id,
            "type": self.node_type,
            "depends_on": [dep.id for dep in self.depends_on],
        }


# ── Dependency resolution ──────────────────────────────────────────


def _resolve_asset_dependencies(
    fn: Callable[..., Any],
    node_registry: dict[str, AssetNode | JobNode],
) -> list[AssetNode]:
    """Resolve deps for an asset — all params must reference other assets."""
    sig = inspect.signature(fn)
    deps: list[AssetNode] = []
    for param_name in sig.parameters:
        if param_name not in node_registry:
            raise ValueError(
                f"Parameter '{param_name}' in '{fn.__name__}' does not match "
                f"any registered node. Available: {list(node_registry.keys())}"
            )
        node = node_registry[param_name]
        if not isinstance(node, AssetNode):
            raise TypeError(
                f"Asset '{fn.__name__}' depends on '{param_name}', which is a "
                f"job. Assets can only depend on other assets."
            )
        deps.append(node)
    return deps


def _resolve_job_dependencies(
    fn: Callable[..., Any],
    node_registry: dict[str, AssetNode | JobNode],
) -> list[AssetNode]:
    """Resolve deps for a job — all params must reference assets, not jobs."""
    sig = inspect.signature(fn)
    deps: list[AssetNode] = []
    for param_name in sig.parameters:
        if param_name not in node_registry:
            raise ValueError(
                f"Parameter '{param_name}' in '{fn.__name__}' does not match "
                f"any registered node. Available: {list(node_registry.keys())}"
            )
        node = node_registry[param_name]
        if not isinstance(node, AssetNode):
            raise TypeError(
                f"Job '{fn.__name__}' depends on '{param_name}', which is a "
                f"job. Jobs can only depend on assets."
            )
        deps.append(node)
    return deps
