#!/usr/bin/env python3
"""
Test activity changes during workflow interruption and recovery.

This tests what happens when:
1. Activity logic changes (same signature, different implementation)
2. Activity is renamed/missing during recovery
3. Activity is deleted entirely

These are critical edge cases for production workflows.
"""

import os
import sys
import time
import tempfile
from py_orchestrate import Orchestrator, workflow, activity

# Global flag to simulate different activity implementations
ACTIVITY_VERSION = "v1"


def get_test_db():
    """Get a temporary database file for testing."""
    return tempfile.mktemp(suffix=".db")


# Version 1 of activities
@activity("fetch_data_v1")
def fetch_data_v1(source: str) -> dict:
    print(f"FETCH V1: Fetching from {source}")
    time.sleep(1)  # Simulate work
    return {"data": f"v1_data_from_{source}", "version": "v1"}


@activity("process_data_v1")
def process_data_v1(data: dict) -> dict:
    print(f"PROCESS V1: Processing {data}")
    time.sleep(1)  # Simulate work
    return {"processed": f"v1_processed_{data['data']}", "version": "v1"}


# Version 2 of activities (changed logic, same signature)
@activity("fetch_data_v1")  # Same name, different implementation
def fetch_data_v1_new(source: str) -> dict:
    print(f"FETCH V2: NEW LOGIC - Fetching from {source}")
    time.sleep(1)  # Simulate work
    return {"data": f"v2_NEW_data_from_{source}", "version": "v2"}


@activity("process_data_v1")  # Same name, different implementation
def process_data_v1_new(data: dict) -> dict:
    print(f"PROCESS V2: NEW LOGIC - Processing {data}")
    time.sleep(1)  # Simulate work
    return {"processed": f"v2_NEW_processed_{data['data']}", "version": "v2"}


# Renamed activity (for testing missing activity scenario)
@activity("process_data_renamed")
def process_data_renamed(data: dict) -> dict:
    print(f"PROCESS RENAMED: Processing {data}")
    return {"processed": f"renamed_processed_{data['data']}", "version": "renamed"}


@workflow("test_activity_changes")
def test_activity_changes(source: str) -> dict:
    print(f"WORKFLOW: Starting with source={source}")

    # Step 1: Fetch data (will complete before interruption)
    data = fetch_data_v1(source)
    print(f"WORKFLOW: Got data={data}")

    # Step 2: Process data (will be interrupted here)
    processed = process_data_v1(data)
    print(f"WORKFLOW: Got processed={processed}")

    # Step 3: Final result
    return {"final_result": processed, "workflow_completed": True}


def test_logic_change():
    """Test what happens when activity logic changes but signature stays same."""
    print("\n" + "=" * 60)
    print("TEST 1: Activity Logic Change (Same Signature)")
    print("=" * 60)

    db_path = get_test_db()
    print(f"Using database: {db_path}")

    try:
        # Phase 1: Start workflow with original activity logic
        print("\n--- Phase 1: Start workflow with V1 logic ---")
        orchestrator = Orchestrator(db_path)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "test_activity_changes", source="database"
        )
        print(f"Started workflow: {workflow_id}")

        # Let fetch_data complete, then stop before process_data
        # First, wait a short time to let fetch_data start
        time.sleep(0.5)

        # Check status - workflow should be processing
        status = orchestrator.get_workflow_status(workflow_id)
        print(f"Status after start: {status}")

        # Let fetch_data complete (1s + buffer)
        time.sleep(1.2)

        # Stop abruptly to simulate crash during process_data
        status = orchestrator.get_workflow_status(workflow_id)
        print(f"Status before stop: {status}")

        orchestrator.stop()
        print("Orchestrator stopped (simulating crash)")

        # Phase 2: Restart with NEW activity logic (same signatures)
        print("\n--- Phase 2: Restart with V2 logic (same names) ---")

        # Clear the registry and register new implementations
        from py_orchestrate.decorators import get_registry

        registry = get_registry()
        registry.activities.clear()

        # Re-register activities with NEW logic but SAME names
        # Just call the decorators to register them (they execute on import)
        # The dummy calls register the functions in the activity registry

        # Restart orchestrator
        orchestrator = Orchestrator(db_path)
        orchestrator.start()

        # Check if workflow resumes
        time.sleep(3)  # Let it complete

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"Final status: {final_status}")

        # Analyze results
        if final_status["status"] == "done":
            output = final_status["output"]
            print(f"Workflow output: {output}")

            # Check if it used cached V1 result or new V2 logic
            if "v1" in str(output):
                print("✅ USED CACHED RESULT from V1 (original logic)")
            elif "v2" in str(output):
                print("⚠️  RE-EXECUTED with V2 (new logic)")
            else:
                print("❓ UNCLEAR which logic was used")
        else:
            print(f"❌ Workflow failed: {final_status.get('error_message')}")

        orchestrator.stop()

    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.remove(db_path)


def test_missing_activity():
    """Test what happens when an activity is missing during recovery."""
    print("\n" + "=" * 60)
    print("TEST 2: Missing Activity During Recovery")
    print("=" * 60)

    db_path = get_test_db()
    print(f"Using database: {db_path}")

    try:
        # Phase 1: Start workflow with activities present
        print("\n--- Phase 1: Start workflow with all activities ---")

        # Clear and register original activities
        from py_orchestrate.decorators import get_registry

        registry = get_registry()
        registry.activities.clear()
        fetch_data_v1("dummy")  # Register
        process_data_v1({"dummy": "data"})  # Register

        orchestrator = Orchestrator(db_path)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "test_activity_changes", source="database"
        )
        print(f"Started workflow: {workflow_id}")

        # Let fetch_data complete, then stop
        time.sleep(2.5)

        status = orchestrator.get_workflow_status(workflow_id)
        print(f"Status before stop: {status}")

        orchestrator.stop()
        print("Orchestrator stopped")

        # Phase 2: Restart with missing activity
        print("\n--- Phase 2: Restart with missing process_data_v1 activity ---")

        # Clear registry and only register fetch_data (process_data is "missing")
        registry.activities.clear()
        fetch_data_v1("dummy")  # Only register this one
        # process_data_v1 is now "missing"

        orchestrator = Orchestrator(db_path)
        orchestrator.start()

        # Let it try to recover
        time.sleep(3)

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"Final status: {final_status}")

        if final_status["status"] == "failed":
            print(f"❌ Expected failure: {final_status.get('error_message')}")
        else:
            print(f"⚠️  Unexpected result: {final_status}")

        orchestrator.stop()

    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.remove(db_path)


def test_renamed_activity():
    """Test what happens when an activity is renamed during recovery."""
    print("\n" + "=" * 60)
    print("TEST 3: Renamed Activity During Recovery")
    print("=" * 60)

    db_path = get_test_db()
    print(f"Using database: {db_path}")

    try:
        # Phase 1: Start workflow with original activity names
        print("\n--- Phase 1: Start with original activity names ---")

        from py_orchestrate.decorators import get_registry

        registry = get_registry()
        registry.activities.clear()
        fetch_data_v1("dummy")
        process_data_v1({"dummy": "data"})

        orchestrator = Orchestrator(db_path)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "test_activity_changes", source="database"
        )
        print(f"Started workflow: {workflow_id}")

        # Let fetch_data complete, then stop
        time.sleep(2.5)

        orchestrator.stop()
        print("Orchestrator stopped")

        # Phase 2: Restart with renamed activity
        print("\n--- Phase 2: Restart with renamed process_data activity ---")

        registry.activities.clear()
        fetch_data_v1("dummy")  # Keep same name
        process_data_renamed({"dummy": "data"})  # Different name!

        orchestrator = Orchestrator(db_path)
        orchestrator.start()

        time.sleep(3)

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"Final status: {final_status}")

        if final_status["status"] == "failed":
            print(
                f"❌ Expected failure due to renamed activity: {final_status.get('error_message')}"
            )
        else:
            print(f"⚠️  Unexpected success: {final_status}")

        orchestrator.stop()

    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 test_activity_changes.py <test>")
        print("Tests:")
        print("  logic     - Test activity logic change")
        print("  missing   - Test missing activity")
        print("  renamed   - Test renamed activity")
        print("  all       - Run all tests")
        return

    test_type = sys.argv[1].lower()

    if test_type == "logic" or test_type == "all":
        test_logic_change()

    if test_type == "missing" or test_type == "all":
        test_missing_activity()

    if test_type == "renamed" or test_type == "all":
        test_renamed_activity()

    print("\n" + "=" * 60)
    print("TESTING COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
