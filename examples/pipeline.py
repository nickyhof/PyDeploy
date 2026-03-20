"""Example pipeline demonstrating PyDeploy's DAG framework.

Run:
    pydeploy plan examples/pipeline.py
    pydeploy deploy examples/pipeline.py --params examples/params.json
"""

from pydeploy import DAG, Table

dag = DAG("example_pipeline")

# ── Assets ─────────────────────────────────────────────────────────
# team and resource_id are Tokens — resolved at deploy time.


@dag.asset
def raw_users() -> Table:
    """Raw user data source."""
    return Table(schema={"id": "int", "name": "str", "email": "str"})


@dag.asset
def raw_orders() -> Table:
    """Raw order data source."""
    return Table(schema={"order_id": "int", "user_id": "int", "total": "float"})


@dag.asset
def user_orders(raw_users, raw_orders) -> Table:
    """Joined table derived from raw users and orders."""
    return Table(schema={"user_name": "str", "order_total": "float"})


# ── Jobs ───────────────────────────────────────────────────────────


@dag.job()
def ingest_users(raw_users):
    """Ingest user data."""
    print(f"    Team:    {raw_users.team}")
    print(f"    Resource: {raw_users.resource_id}")
    raw_users.ingest([
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ])


@dag.job()
def build_report(user_orders):
    """Build a report from the joined table."""
    print(f"    Team:  {user_orders.team}")
    print(f"    Schema: {user_orders.schema}")
    return "report_complete"
