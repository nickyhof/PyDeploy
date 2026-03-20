"""PyDeploy — a lightweight DAG framework with CDK-based plan & deploy.

Usage::

    from pydeploy import DAG, Table

    dag = DAG("my_pipeline")

    @dag.asset
    def raw_data() -> Table:
        return Table(schema={"id": "int"}, path="./data.csv")

    @dag.job()
    def transform(raw_data):
        print(raw_data.schema)

    # Plan phase
    manifest = dag.synth()

    # Deploy phase
    dag.deploy()
"""

from pydeploy.dag import DAG
from pydeploy.deploy import load_assets_from_state
from pydeploy.nodes import Asset, Table, AssetNode, JobNode, Token

__all__ = ["DAG", "Asset", "Table", "AssetNode", "JobNode", "Token", "load_assets_from_state"]
