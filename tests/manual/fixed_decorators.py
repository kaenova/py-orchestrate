#!/usr/bin/env python3
"""
Test all decorator usage patterns after fix.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "py_orchestrate"))

from py_orchestrate import workflow, activity, Orchestrator
from py_orchestrate.decorators import get_registry

print("Testing FIXED decorator usage patterns...")

# Clear registry
registry = get_registry()
registry.workflows.clear()
registry.activities.clear()

print(
    f"Initial registry: workflows={list(registry.workflows.keys())}, activities={list(registry.activities.keys())}"
)

# Test all patterns
print("\n" + "=" * 60)
print("1. Testing @workflow (without parentheses)")


@workflow
def test_workflow_no_parens(data: str) -> dict:
    result = test_activity_no_parens(data)
    return {"workflow_result": result}


print(f"✅ Defined successfully")
print(f"Registry: {list(registry.workflows.keys())}")

print("\n" + "=" * 60)
print("2. Testing @workflow() (with empty parentheses)")


@workflow()
def test_workflow_empty_parens(data: str) -> dict:
    result = test_activity_empty_parens(data)
    return {"workflow_result": result}


print(f"✅ Defined successfully")
print(f"Registry: {list(registry.workflows.keys())}")

print("\n" + "=" * 60)
print("3. Testing @workflow('name') (with custom name)")


@workflow("custom_workflow_name")
def test_workflow_custom_name(data: str) -> dict:
    result = test_activity_custom_name(data)
    return {"workflow_result": result}


print(f"✅ Defined successfully")
print(f"Registry: {list(registry.workflows.keys())}")

print("\n" + "=" * 60)
print("4. Testing @activity (without parentheses)")


@activity
def test_activity_no_parens(data: str) -> dict:
    return {"activity_result": f"processed_{data}"}


print(f"✅ Defined successfully")
print(f"Activities: {list(registry.activities.keys())}")

print("\n" + "=" * 60)
print("5. Testing @activity() (with empty parentheses)")


@activity()
def test_activity_empty_parens(data: str) -> dict:
    return {"activity_result": f"processed_{data}"}


print(f"✅ Defined successfully")
print(f"Activities: {list(registry.activities.keys())}")

print("\n" + "=" * 60)
print("6. Testing @activity('name') (with custom name)")


@activity("custom_activity_name")
def test_activity_custom_name(data: str) -> dict:
    return {"activity_result": f"processed_{data}"}


print(f"✅ Defined successfully")
print(f"Activities: {list(registry.activities.keys())}")

print("\n" + "=" * 60)
print("7. Testing workflow execution")

try:
    orchestrator = Orchestrator("test_fixed_decorators.db")
    orchestrator.start()

    # Test each workflow pattern
    test_cases = [
        ("test_workflow_no_parens", "no_parens"),
        ("test_workflow_empty_parens", "empty_parens"),
        ("custom_workflow_name", "custom_name"),
    ]

    for workflow_name, test_data in test_cases:
        print(f"\n  Testing workflow: {workflow_name}")
        try:
            workflow_id = orchestrator.invoke_workflow(workflow_name, data=test_data)
            print(f"    Started: {workflow_id}")

            import time

            time.sleep(1)

            status = orchestrator.get_workflow_status(workflow_id)
            if status and status["status"] == "done":
                print(f"    ✅ SUCCESS: {status['output']}")
            else:
                print(f"    ❌ FAILED: {status}")

        except Exception as e:
            print(f"    ❌ ERROR: {e}")

    orchestrator.stop()

except Exception as e:
    print(f"❌ Orchestrator error: {e}")

# Cleanup
try:
    os.remove("test_fixed_decorators.db")
except:
    pass

print("\n" + "=" * 60)
print("FINAL REGISTRY STATE:")
print(f"Workflows: {list(registry.workflows.keys())}")
print(f"Activities: {list(registry.activities.keys())}")
print("=" * 60)
print("✅ All decorator patterns should now work correctly!")
