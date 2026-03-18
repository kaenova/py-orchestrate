#!/usr/bin/env python3
"""
Test decorator usage without parentheses to identify the issue.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "py_orchestrate"))

from py_orchestrate import workflow, activity, Orchestrator

print("Testing decorator usage patterns...")

# Test 1: This should work (with parentheses)
print("\n1. Testing @workflow() - with empty parentheses")
try:

    @workflow()
    def test_workflow_1(data: str) -> dict:
        return {"result": f"processed_{data}"}

    print("✅ @workflow() - SUCCESS")
except Exception as e:
    print(f"❌ @workflow() - FAILED: {e}")

# Test 2: This should work (with named parameter)
print("\n2. Testing @workflow('name') - with named parameter")
try:

    @workflow("named_workflow")
    def test_workflow_2(data: str) -> dict:
        return {"result": f"processed_{data}"}

    print("✅ @workflow('name') - SUCCESS")
except Exception as e:
    print(f"❌ @workflow('name') - FAILED: {e}")

# Test 3: This will likely FAIL (without parentheses)
print("\n3. Testing @workflow - without parentheses")
try:

    @workflow  # This is the problematic usage
    def test_workflow_3(data: str) -> dict:
        return {"result": f"processed_{data}"}

    print("✅ @workflow - SUCCESS")
except Exception as e:
    print(f"❌ @workflow - FAILED: {e}")

# Test 4: Same issue with activity
print("\n4. Testing @activity - without parentheses")
try:

    @activity  # This will also fail
    def test_activity_1(data: str) -> dict:
        return {"processed": data}

    print("✅ @activity - SUCCESS")
except Exception as e:
    print(f"❌ @activity - FAILED: {e}")

# Test 5: Try to use the problematic decorators
print("\n5. Testing if the problematic decorators can be used")
try:
    orchestrator = Orchestrator("test_decorator_issue.db")
    orchestrator.start()

    # This will likely fail because test_workflow_3 isn't properly registered
    workflow_id = orchestrator.invoke_workflow("test_workflow_3", data="test")
    print(f"Started workflow: {workflow_id}")

    import time

    time.sleep(1)

    status = orchestrator.get_workflow_status(workflow_id)
    print(f"Workflow status: {status}")

    orchestrator.stop()

except Exception as e:
    print(f"❌ Workflow execution FAILED: {e}")
    try:
        orchestrator.stop()
    except:
        pass

# Cleanup
try:
    os.remove("test_decorator_issue.db")
except:
    pass

print("\n" + "=" * 60)
print("DIAGNOSIS: The issue is that @workflow without parentheses")
print("passes the function as the 'name' parameter instead of None.")
print("This breaks the decorator logic.")
print("=" * 60)
