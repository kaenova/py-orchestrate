# py-orchestrate

[![PyPI version](https://badge.fury.io/py/py-orchestrate.svg)](https://badge.fury.io/py/py-orchestrate)
[![Python](https://img.shields.io/pypi/pyversions/py-orchestrate.svg)](https://pypi.org/project/py-orchestrate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python workflow orchestrator like any durable orchestrator, but run **locally** with **embedded** SQLite database.

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

## Activity Signature Changes & Best Practices

⚠️ **Important**: Changing activity function signatures can cause workflow failures. This section explains the behavior and provides best practices.

### How Recovery Works

py-orchestrate uses **activity-level recovery** with intelligent caching:

1. **Workflow function restarts**: The workflow function code begins executing from the top
2. **Completed activities use cache**: Activities that finished successfully return cached results instantly  
3. **Only incomplete work re-executes**: Activities that were interrupted or failed run again
4. **No completed work is lost**: All successful activity results are preserved in the database

**Example Recovery Flow:**
```python
@workflow("data_processing")
def data_processing(source: str) -> dict:
    data = fetch_data(source)          # ← If completed: returns cached result instantly
    processed = process_data(data)     # ← If interrupted here: re-executes normally  
    result = save_results(processed)   # ← Continues from interruption point
    return result
```

This provides efficient recovery - the workflow "replays" its execution path but skips completed work.

### Signature Change Behavior

#### ✅ **Safe Changes (No Issues)**

```python
# ✅ Adding optional parameters (backward compatible)
@activity("process_data")
def process_data(data: str, mode: str = "default") -> dict:
    return {"processed": data, "mode": mode}

# ✅ Using different activity names (versioning)
@activity("process_data_v2")  
def process_data_v2(data: str, enhanced: bool = True) -> dict:
    return {"processed": data, "enhanced": enhanced}
```

#### ❌ **Unsafe Changes (Will Cause Errors)**

```python
# ❌ Removing parameters
@activity("process_data")
def process_data(data: str) -> dict:  # Removed 'mode' parameter
    return {"processed": data}

# ❌ Changing parameter types
@activity("process_data") 
def process_data(data: dict) -> dict:  # Changed str -> dict
    return {"processed": data}

# ❌ Adding required parameters
@activity("process_data")
def process_data(data: str, required_param: str) -> dict:  # No default value
    return {"processed": data, "required": required_param}
```

### Error Examples

When signature mismatches occur, you'll see clear error messages:

```python
# Error: "too many positional arguments"
# Caused by: Workflow calls activity(param1, param2) but activity only accepts activity(param1)

# Error: "missing required positional argument"  
# Caused by: Workflow calls activity(param1) but activity requires activity(param1, param2)

# Error: "argument of type 'dict' is not iterable"
# Caused by: Type mismatch between expected and actual parameter types
```

### Best Practices

#### 1. **Version Your Activities**
```python
# Good: Use versioned activity names
@activity("fetch_data_v1")
def fetch_data_v1(source: str) -> dict:
    return {"data": f"v1_{source}"}

@activity("fetch_data_v2") 
def fetch_data_v2(source: str, timeout: int = 30) -> dict:
    return {"data": f"v2_{source}", "timeout": timeout}

# Update workflows to use new versions
@workflow("data_processing_v2")
def data_processing_v2(source: str) -> dict:
    data = fetch_data_v2(source, timeout=60)  # Use new version
    return {"result": data}
```

#### 2. **Only Add Optional Parameters**
```python
# Good: Backward compatible changes
@activity("process_item")
def process_item(item: dict, 
                validation: bool = True,    # Added optional
                timeout: int = 30) -> dict: # Added optional
    result = {"processed": item}
    if validation:
        result["validated"] = True
    return result
```

#### 3. **Use Defensive Programming**
```python
# Good: Handle different signatures gracefully
@activity("flexible_processor")
def flexible_processor(*args, **kwargs) -> dict:
    # Handle both old and new calling patterns
    if len(args) == 1:
        # Old signature: flexible_processor(data)
        data = args[0]
        options = {}
    elif len(args) == 2:
        # New signature: flexible_processor(data, options)
        data, options = args
    else:
        # Keyword arguments
        data = kwargs.get('data')
        options = kwargs.get('options', {})
    
    return {"processed": data, "options": options}
```

#### 4. **Test Signature Changes**
```python
# Always test workflows after activity changes
def test_signature_compatibility():
    orchestrator = Orchestrator("test.db")
    orchestrator.start()
    
    try:
        # Test new signature
        workflow_id = orchestrator.invoke_workflow("my_workflow", input="test")
        status = orchestrator.get_workflow_status(workflow_id)
        
        if status['status'] == 'failed':
            print(f"Signature error: {status['error_message']}")
            
    finally:
        orchestrator.stop()
```

### Activity Implementation Changes During Recovery

**Important**: py-orchestrate uses **activity-level caching** during recovery, which has important implications for activity changes:

#### ✅ **Safe Scenarios (No Issues)**

1. **Activity logic changes after completion**:
   ```python
   # Original implementation (already completed and cached)
   @activity("process_data")
   def process_data(data: str) -> dict:
       return {"result": f"old_logic_{data}"}
   
   # New implementation (won't affect cached result)
   @activity("process_data")  
   def process_data(data: str) -> dict:
       return {"result": f"new_logic_{data}"}
   ```
   **Result**: Workflow uses cached result from original implementation

2. **Missing activities after completion**:
   ```python
   # If activity was deleted but already completed
   # Result: Workflow uses cached result, doesn't try to re-execute
   ```

#### ⚠️ **Risky Scenarios (Will Cause Failures)**

1. **Missing activities for incomplete work**:
   ```python
   # If workflow was interrupted during this activity
   # and then activity is deleted/renamed
   # Result: "Activity 'process_data' not found" error
   ```

2. **Signature changes for incomplete work**:
   ```python
   # If workflow was interrupted during this activity
   # and then signature changes incompatibly  
   # Result: Signature mismatch errors (as documented above)
   ```

#### 🔧 **Best Practices for Activity Changes**

1. **Wait for workflows to complete** before making breaking changes
2. **Use activity versioning** for major changes:
   ```python
   @activity("process_data_v1")  # Keep old version
   @activity("process_data_v2")  # Add new version
   ```
3. **Check for running workflows** before deployments:
   ```python
   # Check for processing workflows
   processing_workflows = orchestrator.list_workflows()
   active = [wf for wf in processing_workflows if wf['status'] == 'processing']
   if active:
       print(f"Warning: {len(active)} workflows still processing")
   ```

### Recovery from Signature Errors

If you encounter signature mismatch errors:

1. **Check the error message** - it tells you exactly what's wrong
2. **Fix the activity signature** - make it backward compatible
3. **Or create a new versioned activity** - safer approach
4. **Test the fix** - ensure existing workflows can complete

### Migration Strategy

When you need to change activity signatures:

```python
# Step 1: Create new versioned activity
@activity("process_data_v2")
def process_data_v2(data: str, new_param: str = "default") -> dict:
    return {"processed": data, "new_param": new_param}

# Step 2: Keep old activity for compatibility (optional)
@activity("process_data_v1") 
def process_data_v1(data: str) -> dict:
    # Delegate to new version with defaults
    return process_data_v2(data, "legacy_default")

# Step 3: Update new workflows to use v2
@workflow("new_workflow")
def new_workflow(input: str) -> dict:
    result = process_data_v2(input, "enhanced")  # Use new version
    return {"result": result}

# Step 4: Migrate existing workflows gradually
```

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

## Troubleshooting

### Common Issues

#### Workflow Stuck in "processing" Status
```python
# Check current activity and error message
status = orchestrator.get_workflow_status(workflow_id)
print(f"Status: {status['status']}")
print(f"Current Activity: {status.get('current_activity')}")
print(f"Error: {status.get('error_message')}")
```

**Possible causes:**
- Activity is taking longer than expected
- Activity crashed without proper error handling
- Database connection issues

#### "too many positional arguments" Error
**Cause:** Activity signature was reduced (fewer parameters)
**Solution:** Add back removed parameters or create new versioned activity

#### "missing required positional argument" Error  
**Cause:** Activity signature was expanded (more required parameters)
**Solution:** Make new parameters optional with default values

#### Workflows Not Resuming After Restart
**Possible causes:**
- Activity functions not registered (missing imports)
- Activity names changed
- Database file moved or deleted

**Solution:**
```python
# Ensure all activities are imported and registered
from my_activities import fetch_data, process_data  # Import activities
from py_orchestrate import Orchestrator

orchestrator = Orchestrator()  
orchestrator.start()  # Recovery happens automatically
```

#### Database Corruption
**Prevention:**
- Don't modify the SQLite database manually
- Use proper shutdown: `orchestrator.stop()`
- Backup database files in production

**Recovery:**
```python
# Check database integrity
import sqlite3
conn = sqlite3.connect("my_workflows.db")
conn.execute("PRAGMA integrity_check;")
```

### Debugging Tips

#### Enable Detailed Logging
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Now you'll see detailed orchestrator logs
orchestrator = Orchestrator()
```

#### Inspect Database State
```python
# Check workflow status directly
workflows = orchestrator.list_workflows("my_workflow_name")
for wf in workflows:
    print(f"ID: {wf['id']}, Status: {wf['status']}")
```

#### Test Activities in Isolation
```python
# Test activity functions directly before using in workflows
@activity("test_activity")
def test_activity(param: str) -> dict:
    return {"result": param}

# Test directly
result = test_activity("test_input")
print(result)  # Should work before putting in workflow
```

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

## License

MIT License
