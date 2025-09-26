# py-orchestrate

[![PyPI version](https://badge.fury.io/py/py-orchestrate.svg)](https://badge.fury.io/py/py-orchestrate)
[![Python](https://img.shields.io/pypi/pyversions/py-orchestrate.svg)](https://pypi.org/project/py-orchestrate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python workflow orchestrator with embedded SQLite database, similar to Durable Functions but with persistent state management.

## Features

- **Workflow Orchestration**: Define workflows that orchestrate multiple activities
- **Persistent State**: All workflow and activity state is persisted in SQLite database
- **Fault Tolerance**: Workflows can be resumed after application restart
- **Activity Tracking**: Track current activity execution and progress
- **Background Execution**: Workflows run asynchronously in background threads
- **Simple API**: Easy-to-use decorators for defining workflows and activities

## Installation

### From PyPI (Recommended)

```bash
# Install using pip
pip install py-orchestrate

# Or using uv
uv add py-orchestrate
```

### From Source

```bash
# Clone the repository
git clone https://github.com/your-username/py-orchestrate.git
cd py-orchestrate

# Install using uv
uv pip install -e .

# Or using pip
pip install -e .
```

## Quick Start

### 1. Define Activities

Activities are individual units of work that can be orchestrated by workflows:

```python
from py_orchestrate import activity

@activity("fetch_data")
def fetch_data(source: str) -> dict:
    # Your activity logic here
    return {"data": f"data_from_{source}", "count": 100}

@activity("process_data")  
def process_data(data: dict) -> dict:
    processed_count = data.get("count", 0) * 2
    return {"processed_data": data["data"] + "_processed", "processed_count": processed_count}
```

### 2. Define Workflows

Workflows orchestrate multiple activities:

```python
from py_orchestrate import workflow

@workflow("data_processing_workflow")
def data_processing_workflow(source: str, destination: str) -> dict:
    # Step 1: Fetch data
    raw_data = fetch_data(source)
    
    # Step 2: Process data  
    processed_data = process_data(raw_data)
    
    # Step 3: Return result
    return {
        "workflow_completed": True,
        "total_processed": processed_data["processed_count"]
    }
```

### 3. Run the Orchestrator

```python
from py_orchestrate import Orchestrator

# Create and start orchestrator
orchestrator = Orchestrator()
orchestrator.start()

try:
    # Invoke a workflow
    workflow_id = orchestrator.invoke_workflow(
        "data_processing_workflow",
        source="database", 
        destination="warehouse"
    )
    
    # Monitor progress
    status = orchestrator.get_workflow_status(workflow_id)
    print(f"Status: {status['status']}")
    print(f"Current activity: {status['current_activity']}")
    
finally:
    orchestrator.stop()
```

## Core Concepts

### Workflows
- State orchestrators that coordinate multiple activities
- Can have status: "processing", "done", or "failed"  
- Run asynchronously in the background
- State is persisted and can resume after application restart

### Activities
- Individual units of work executed by workflows
- Must use basic types (dict, list, str, int, etc.) for parameters and return values
- Cannot pass objects between activities
- Execution is tracked and persisted

### Orchestrator Engine
- Manages workflow and activity execution
- Persists state in SQLite database
- Provides APIs to invoke workflows and query status
- Handles fault tolerance and recovery

## API Reference

### Decorators

#### `@workflow(name=None)`
Marks a function as a workflow.

**Parameters:**
- `name` (str, optional): Name for the workflow. Defaults to function name.

#### `@activity(name=None)`  
Marks a function as an activity.

**Parameters:**
- `name` (str, optional): Name for the activity. Defaults to function name.

### Orchestrator Class

#### `Orchestrator(db_path="py_orchestrate.db", max_workers=5)`
Creates a new orchestrator instance.

**Parameters:**
- `db_path` (str): Path to SQLite database file
- `max_workers` (int): Maximum number of concurrent workflow threads

#### Methods

##### `start()`
Starts the orchestrator engine.

##### `stop()`
Stops the orchestrator engine and waits for workflows to complete.

##### `invoke_workflow(name: str, **kwargs) -> str`
Invokes a workflow by name with input parameters.

**Returns:** Workflow ID for tracking execution

##### `get_workflow_status(workflow_id: str) -> dict`
Gets the current status of a workflow.

**Returns:** Dictionary with workflow status information:
```python
{
    "id": "workflow-id",
    "name": "workflow-name", 
    "status": "processing|done|failed",
    "current_activity": "activity-name or None",
    "error_message": "error message or None",
    "output": "workflow output or None",
    "created_at": "ISO timestamp",
    "updated_at": "ISO timestamp" 
}
```

##### `list_workflows(name: str = None) -> List[dict]`
Lists workflows, optionally filtered by name.

## Database Schema

The library creates two tables in SQLite:

- `workflows`: Stores workflow instances and their state
- `activity_executions`: Stores individual activity executions

## Examples

See `main.py` for a complete working example.

## Requirements

- Python 3.12+
- SQLite (included with Python)

## Development

### Setting up for Development

```bash
# Clone the repository
git clone https://github.com/your-username/py-orchestrate.git
cd py-orchestrate

# Install development dependencies
uv sync --dev

# Run the example
uv run python py_orchestrate/example.py

# Run type checking
uv run mypy py_orchestrate --ignore-missing-imports

# Format code
uv run black py_orchestrate

# Build package
uv run python -m build
```

### Release Process

This project uses GitHub Actions for automated building and publishing to PyPI.

#### Setup (One-time)

1. **Create PyPI API tokens:**
   - Go to [PyPI Account Settings](https://pypi.org/manage/account/)
   - Create an API token for this project
   - Go to [Test PyPI Account Settings](https://test.pypi.org/manage/account/)
   - Create an API token for testing

2. **Add GitHub Secrets:**
   - Go to your GitHub repository → Settings → Secrets and variables → Actions
   - Add these secrets:
     - `PYPI_API_TOKEN`: Your PyPI API token
     - `TEST_PYPI_API_TOKEN`: Your Test PyPI API token

#### Publishing a Release

1. **For a pre-release (e.g., `1.0.0-beta1`):**
   ```bash
   git tag 1.0.0-beta1
   git push origin 1.0.0-beta1
   ```
   This will publish to Test PyPI.

2. **For a stable release (e.g., `1.0.0`):**
   ```bash
   git tag 1.0.0
   git push origin 1.0.0
   ```
   This will publish to PyPI.

3. **The GitHub Action will automatically:**
   - Update the version in `pyproject.toml`
   - Build the package
   - Run quality checks
   - Publish to PyPI/Test PyPI
   - Create a GitHub release with artifacts

## License

MIT License
