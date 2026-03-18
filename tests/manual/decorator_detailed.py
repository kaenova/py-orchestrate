#!/usr/bin/env python3
"""
Detailed investigation of decorator behavior without parentheses.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "py_orchestrate"))

from py_orchestrate.decorators import get_registry

print("Investigating decorator registration behavior...")

# Clear registry first
registry = get_registry()
registry.workflows.clear()
registry.activities.clear()

print(f"Initial registry state:")
print(f"  Workflows: {list(registry.workflows.keys())}")
print(f"  Activities: {list(registry.activities.keys())}")

# Test different decorator patterns
from py_orchestrate import workflow, activity

print("\n" + "=" * 50)
print("Testing @workflow() - with parentheses")


@workflow()
def test_with_parens(data: str) -> dict:
    return {"result": data}


print(f"After @workflow():")
print(f"  Workflows: {list(registry.workflows.keys())}")
print(f"  Function name: {test_with_parens.__name__}")

print("\n" + "=" * 50)
print("Testing @workflow - WITHOUT parentheses")


@workflow
def test_without_parens(data: str) -> dict:
    return {"result": data}


print(f"After @workflow:")
print(f"  Workflows: {list(registry.workflows.keys())}")
print(f"  Function name: {test_without_parens.__name__}")
print(f"  Function type: {type(test_without_parens)}")

# Let's check what happened to the function
print(f"\nInvestigating the function returned by @workflow:")
print(f"  Is callable: {callable(test_without_parens)}")
print(f"  Has __name__: {hasattr(test_without_parens, '__name__')}")

if hasattr(test_without_parens, "__name__"):
    print(f"  __name__: {test_without_parens.__name__}")

# Try to call it
print(f"\nTrying to call the function:")
try:
    result = test_without_parens("test_data")
    print(f"  Call result: {result}")
except Exception as e:
    print(f"  Call failed: {e}")

print("\n" + "=" * 50)
print("ANALYSIS:")
print("The issue is that when using @workflow without parentheses,")
print("Python passes the function directly to workflow(), but workflow()")
print("expects to return a decorator function, not decorate directly.")

print(f"\nRegistry final state:")
print(f"  Workflows: {list(registry.workflows.keys())}")
print(f"  Activities: {list(registry.activities.keys())}")
