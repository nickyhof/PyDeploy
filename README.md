# PyDeploy

A lightweight DAG framework for defining, planning, and deploying data pipelines in Python.

## Concepts

| Concept | Description |
|---|---|
| **DAG** | A directed acyclic graph of assets and jobs |
| **Asset** | A data resource (e.g., `Table`) with schema, team, and resource ID |
| **Job** | A function that operates on assets — runs in an isolated environment |
| **Token** | A placeholder value resolved at deploy time (inputs and outputs) |

### Token Types

- **Inputs** — user-provided at deploy time (e.g., `team`). Declared on the asset, resolved from a params config file.
- **Outputs** — system-generated during provisioning (e.g., `resource_id`). Persisted in the state file across deploys.

## Quickstart

### 1. Define a pipeline

```python
# examples/pipeline.py
from pydeploy import DAG, Table

dag = DAG("example_pipeline")

@dag.asset
def raw_users() -> Table:
    return Table(schema={"id": "int", "name": "str"})

@dag.job()
def ingest(raw_users):
    print(f"Team: {raw_users.team}")
    raw_users.ingest([{"id": 1, "name": "Alice"}])
```

### 2. Create a params config

```json
// examples/params.json
{
  "raw_users": { "team": "data-team" }
}
```

### 3. Plan (synthesize)

```bash
./scripts/synth.sh
# or: uv run pydeploy plan examples/pipeline.py
```

Produces `output/example_pipeline.manifest.json` with the execution plan:

```
📋 Plan for DAG 'example_pipeline':
   Nodes (2):
   • [table] raw_users
   • [job] ingest → depends on: ['raw_users']
   Execution order: raw_users → ingest
```

### 4. Deploy

```bash
./scripts/deploy.sh
# or: uv run pydeploy deploy examples/pipeline.py --params examples/params.json
```

Provisions assets and generates job scripts:

```
🚀 Deploying...
  ✓ Table 'raw_users': created resource 9affb1b8-...
  ✓ Job 'ingest': script → output/jobs/ingest.py
```

### 5. Run jobs

```bash
uv run python output/jobs/ingest.py
```

Job scripts hydrate asset dependencies from the state file — simulating container execution.

## Architecture

```
Pipeline Definition (.py)
        │
        ▼
   ┌─────────┐
   │  Synth   │  → manifest.json (tokens as ${refs})
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │ Deploy   │  → state.json   (resolved tokens)
   └────┬────┘    → jobs/*.py   (runnable scripts)
        │
        ▼
   ┌─────────┐
   │  Jobs    │  ← hydrate from state.json
   └─────────┘
```

## Project Structure

```
src/pydeploy/
├── nodes.py    # Token, Asset, Table, AssetNode, JobNode
├── dag.py      # DAG class with @asset and @job decorators
├── synth.py    # Synthesis: DAG → manifest (topological sort)
├── deploy.py   # Deploy: provision assets, generate job scripts
└── cli.py      # CLI: pydeploy plan / deploy

examples/
├── pipeline.py # Example pipeline definition
└── params.json # Deploy-time parameters

scripts/
├── synth.sh    # Shortcut for plan
└── deploy.sh   # Shortcut for deploy
```

## Rules

- Assets can depend on other assets
- Jobs can depend on assets
- Jobs **cannot** depend on other jobs
- Jobs are not executed during deploy — they run separately

## Development

```bash
uv sync
uv run pytest tests/ -v
```
