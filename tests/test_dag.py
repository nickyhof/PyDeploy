"""Tests for DAG construction and node registration."""

import pytest

from pydeploy import DAG, Table
from pydeploy.nodes import AssetNode, JobNode


def test_asset_decorator_registers_table():
    """@dag.asset registers a Table and wraps it in an AssetNode."""
    dag = DAG("test-dag")

    @dag.asset
    def my_data() -> Table:
        return Table(schema={"id": "int", "name": "str"})

    node = dag.get_node("my_data")
    assert isinstance(node, AssetNode)
    assert isinstance(node.asset, Table)
    assert node.asset.schema == {"id": "int", "name": "str"}


def test_asset_depends_on_asset():
    """Assets can depend on other assets via function params."""
    dag = DAG("test-dag")

    @dag.asset
    def source() -> Table:
        return Table(schema={"id": "int"})

    @dag.asset
    def derived(source) -> Table:
        # source is the Table instance
        return Table(schema={**source.schema, "score": "float"})

    node = dag.get_node("derived")
    assert len(node.depends_on) == 1
    assert node.depends_on[0].id == "source"
    assert node.asset.schema == {"id": "int", "score": "float"}


def test_asset_name_auto_set():
    """Table.name is auto-set from the function name."""
    dag = DAG("test-dag")

    @dag.asset
    def fancy_data() -> Table:
        return Table(schema={"x": "float"})

    assert dag.get_node("fancy_data").asset.name == "fancy_data"


def test_asset_must_return_asset_subclass():
    """@dag.asset raises TypeError if fn doesn't return an Asset."""
    dag = DAG("test-dag")

    with pytest.raises(TypeError, match="must return an Asset subclass"):

        @dag.asset
        def bad() -> str:
            return "not an asset"  # type: ignore


def test_asset_cannot_depend_on_job():
    """Assets cannot depend on jobs."""
    dag = DAG("test-dag")

    @dag.asset
    def data() -> Table:
        return Table()

    @dag.job()
    def some_job(data):
        pass

    with pytest.raises(TypeError, match="Assets can only depend on other assets"):

        @dag.asset
        def bad_asset(some_job) -> Table:
            return Table()


def test_job_depends_on_asset():
    """Jobs can depend on assets."""
    dag = DAG("test-dag")

    @dag.asset
    def source() -> Table:
        return Table()

    @dag.job()
    def process(source):
        pass

    job_node = dag.get_node("process")
    assert isinstance(job_node, JobNode)
    assert len(job_node.depends_on) == 1
    assert isinstance(job_node.depends_on[0], AssetNode)


def test_job_cannot_depend_on_job():
    """Jobs cannot depend on other jobs."""
    dag = DAG("test-dag")

    @dag.asset
    def data() -> Table:
        return Table()

    @dag.job()
    def step1(data):
        pass

    with pytest.raises(TypeError, match="Jobs can only depend on assets"):

        @dag.job()
        def step2(step1):
            pass


def test_job_multiple_asset_dependencies():
    """Job with multiple asset params gets multiple dependencies."""
    dag = DAG("test-dag")

    @dag.asset
    def data_a() -> Table:
        return Table()

    @dag.asset
    def data_b() -> Table:
        return Table()

    @dag.job()
    def merge(data_a, data_b):
        pass

    dep_names = [d.id for d in dag.get_node("merge").depends_on]
    assert dep_names == ["data_a", "data_b"]


def test_job_unknown_dependency_raises():
    """Job referencing unknown param name raises ValueError."""
    dag = DAG("test-dag")

    with pytest.raises(ValueError, match="nonexistent"):

        @dag.job()
        def broken(nonexistent):
            pass
