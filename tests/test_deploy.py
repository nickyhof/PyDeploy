"""Tests for the deploy (provisioning) phase."""

import os
import tempfile

import pytest

from pydeploy import DAG, Table, Token
from pydeploy.deploy import load_assets_from_state


def _tmp_state():
    """Return a temp file path for test state."""
    return os.path.join(tempfile.mkdtemp(), "output", "state.json")


def _default_params(*names: str) -> dict[str, dict[str, str]]:
    """Build a deploy params dict assigning team to every table."""
    return {name: {"team": "test-team"} for name in names}


# ── Token tests ─────────────────────────────────────────────────────


def test_token_unresolved():
    """Unresolved Token stringifies to its ref."""
    t = Token("${my_data.resource_id}")
    assert str(t) == "${my_data.resource_id}"
    assert not t.is_resolved


def test_token_resolved():
    """Resolved Token returns its value."""
    t = Token("${my_data.resource_id}")
    t.resolve("abc-123")
    assert t.is_resolved
    assert t.value == "abc-123"
    assert str(t) == "abc-123"


def test_token_value_before_resolve_raises():
    """Accessing .value before resolve raises RuntimeError."""
    t = Token("${x.resource_id}")
    with pytest.raises(RuntimeError, match="not been resolved"):
        t.value


# ── Team token tests ───────────────────────────────────────────────


def test_team_token_ref():
    """@dag.asset sets team Token ref."""
    dag = DAG("team-test")

    @dag.asset
    def data() -> Table:
        return Table(schema={"id": "int"})

    ds = dag.get_node("data").asset
    assert isinstance(ds.team, Token)
    assert ds.team.ref == "${data.team}"
    assert not ds.team.is_resolved


def test_deploy_resolves_team():
    """Deploy resolves team from params config."""
    dag = DAG("team-resolve")

    @dag.asset
    def data() -> Table:
        return Table(schema={"id": "int"})

    ctx = dag.deploy(
        state_file=_tmp_state(),
        params={"data": {"team": "analytics"}},
    )
    ds = ctx.assets["data"]
    assert ds.team.is_resolved
    assert ds.team.value == "analytics"


def test_deploy_missing_team_raises():
    """Deploy raises ValueError if team not provided."""
    dag = DAG("missing-team")

    @dag.asset
    def data() -> Table:
        return Table(schema={"id": "int"})

    with pytest.raises(ValueError, match="Missing 'team'"):
        dag.deploy(state_file=_tmp_state(), params={})


# ── Resource ID tests ───────────────────────────────────────────────


def test_deploy_resolves_resource_id():
    """Deploy resolves resource_id Token on the asset."""
    dag = DAG("resolve-test")

    @dag.asset
    def data() -> Table:
        return Table(schema={"x": "float"})

    ctx = dag.deploy(
        state_file=_tmp_state(),
        params=_default_params("data"),
    )
    ds = ctx.assets["data"]
    assert ds.resource_id.is_resolved
    assert len(ds.resource_id.value) > 0


def test_deploy_reuses_resource_id():
    """Re-deploying resolves to the same resource_id from state."""
    state_path = _tmp_state()

    dag1 = DAG("reuse-test")

    @dag1.asset
    def data() -> Table:
        return Table(schema={"id": "int"})

    ctx1 = dag1.deploy(state_file=state_path, params=_default_params("data"))
    first_id = ctx1.assets["data"].resource_id.value

    dag2 = DAG("reuse-test")

    @dag2.asset
    def data() -> Table:  # noqa: F811
        return Table(schema={"id": "int"})

    ctx2 = dag2.deploy(state_file=state_path, params=_default_params("data"))
    assert ctx2.assets["data"].resource_id.value == first_id


# ── State hydration tests ───────────────────────────────────────────


def test_load_assets_from_state():
    """load_assets_from_state returns Tables with all tokens resolved."""
    state_path = _tmp_state()
    dag = DAG("hydrate-test")

    @dag.asset
    def source() -> Table:
        return Table(schema={"x": "float"})

    ctx = dag.deploy(
        state_file=state_path,
        params={"source": {"team": "ml-team"}},
    )
    original_id = ctx.assets["source"].resource_id.value

    assets = load_assets_from_state(state_path)
    ds = assets["source"]
    assert ds.resource_id.is_resolved
    assert ds.resource_id.value == original_id
    assert ds.team.is_resolved
    assert ds.team.value == "ml-team"


def test_table_ingest_before_deploy_raises():
    """ingest() raises RuntimeError if resource_id is not resolved."""
    ds = Table(name="test", schema={"id": "int"})
    ds.resource_id = Token("${test.resource_id}")

    with pytest.raises(RuntimeError, match="has no resource_id"):
        ds.ingest([{"id": 1}])


# ── Manifest tests ──────────────────────────────────────────────────


def test_manifest_includes_tokens():
    """Manifest serializes token refs for team and resource_id."""
    dag = DAG("manifest-test")

    @dag.asset
    def users() -> Table:
        return Table(schema={"id": "int"})

    @dag.job()
    def load(users):
        pass

    manifest = dag.synth()
    ds = {n["name"]: n for n in manifest["nodes"]}["users"]

    assert ds["inputs"]["team"] == "${users.team}"
    assert ds["outputs"]["resource_id"] == "${users.resource_id}"
