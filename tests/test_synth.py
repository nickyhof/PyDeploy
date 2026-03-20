"""Tests for the synthesis (plan) phase."""

import pytest

from pydeploy import DAG, Table
from pydeploy.nodes import AssetNode, JobNode
from pydeploy.synth import CyclicDependencyError


def test_synth_asset_chain():
    """Asset dependencies produce correct topological order."""
    dag = DAG("asset-chain")

    @dag.asset
    def raw() -> Table:
        return Table(schema={"id": "int"})

    @dag.asset
    def cleaned(raw) -> Table:
        return Table(schema={"id": "int", "valid": "bool"})

    @dag.asset
    def enriched(cleaned) -> Table:
        return Table(schema={"id": "int", "valid": "bool", "score": "float"})

    order = dag.synth()["execution_order"]
    assert order.index("raw") < order.index("cleaned") < order.index("enriched")


def test_synth_job_depends_on_asset():
    """Jobs appear after their asset dependencies."""
    dag = DAG("job-asset")

    @dag.asset
    def data() -> Table:
        return Table(schema={"id": "int"})

    @dag.job()
    def process(data):
        pass

    order = dag.synth()["execution_order"]
    assert order.index("data") < order.index("process")


def test_synth_diamond_assets():
    """Diamond asset pattern produces valid order."""
    dag = DAG("diamond")

    @dag.asset
    def source() -> Table:
        return Table()

    @dag.asset
    def left(source) -> Table:
        return Table()

    @dag.asset
    def right(source) -> Table:
        return Table()

    @dag.asset
    def merged(left, right) -> Table:
        return Table()

    order = dag.synth()["execution_order"]
    assert order[0] == "source"
    assert order[-1] == "merged"


def test_synth_cycle_detection():
    """Cyclic dependencies raise CyclicDependencyError."""
    dag = DAG("cyclic")

    a1 = AssetNode("a1", asset=Table(), depends_on=[])
    a2 = AssetNode("a2", asset=Table(), depends_on=[a1])
    a1.depends_on = [a2]
    dag._nodes["a1"] = a1
    dag._nodes["a2"] = a2

    with pytest.raises(CyclicDependencyError):
        dag.synth()


def test_manifest_asset_dependencies():
    """Manifest includes depends_on for assets with dependencies."""
    dag = DAG("manifest-test")

    @dag.asset
    def source() -> Table:
        return Table(schema={"id": "int"})

    @dag.asset
    def derived(source) -> Table:
        return Table(schema={"id": "int", "extra": "str"})

    @dag.job()
    def load(derived):
        pass

    nodes_by_name = {n["name"]: n for n in dag.synth()["nodes"]}

    assert "depends_on" not in nodes_by_name["source"]
    assert nodes_by_name["derived"]["depends_on"] == ["source"]
    assert nodes_by_name["load"]["depends_on"] == ["derived"]
