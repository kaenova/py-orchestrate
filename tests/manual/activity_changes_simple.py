#!/usr/bin/env python3
"""
Simple test for activity implementation changes during workflow recovery.
"""

import os
import sys
import time
import tempfile
import threading
from py_orchestrate import Orchestrator, workflow, activity

# Test database
TEST_DB = "test_activity_changes.db"

# Global flag to control which version of activities to use
ACTIVITY_VERSION = "v1"


@activity("fetch_data")
def fetch_data_flexible(source: str) -> dict:
    """Activity that changes behavior based on global flag."""
    print(f"FETCH {ACTIVITY_VERSION}: Fetching from {source}")
    time.sleep(1)  # Simulate work

    if ACTIVITY_VERSION == "v1":
        return {"data": f"v1_data_from_{source}", "version": "v1"}
    else:
        return {"data": f"v2_NEW_data_from_{source}", "version": "v2"}


@activity("process_data")
def process_data_flexible(data: dict) -> dict:
    """Activity that changes behavior based on global flag."""
    print(f"PROCESS {ACTIVITY_VERSION}: Processing {data}")
    time.sleep(2)  # Longer to ensure we can interrupt here

    if ACTIVITY_VERSION == "v1":
        return {"processed": f"v1_processed_{data['data']}", "version": "v1"}
    else:
        return {"processed": f"v2_NEW_processed_{data['data']}", "version": "v2"}


@workflow("test_implementation_change")
def test_implementation_change(source: str) -> dict:
    print(f"WORKFLOW: Starting with source={source}")

    # Step 1: Fetch data (should complete)
    data = fetch_data_flexible(source)
    print(f"WORKFLOW: Got data={data}")

    # Step 2: Process data (interrupt here)
    processed = process_data_flexible(data)
    print(f"WORKFLOW: Got processed={processed}")

    return {"final_result": processed, "workflow_completed": True}


def test_activity_implementation_change():
    """Test changing activity implementation during recovery."""
    print("=" * 60)
    print("Testing Activity Implementation Change During Recovery")
    print("=" * 60)

    # Clean up any existing database
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    global ACTIVITY_VERSION

    try:
        # Phase 1: Start workflow with V1 implementation
        print("\n--- Phase 1: Start workflow with V1 implementation ---")
        ACTIVITY_VERSION = "v1"

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "test_implementation_change", source="database"
        )
        print(f"Started workflow: {workflow_id}")

        # Let fetch_data complete (1s), then interrupt during process_data (2s)
        time.sleep(1.5)  # fetch_data done, process_data starting

        status = orchestrator.get_workflow_status(workflow_id)
        print(f"Status during execution: {status}")

        # Stop orchestrator to simulate crash
        orchestrator.stop()
        print("Orchestrator stopped (simulating crash/restart)")

        # Phase 2: Restart with V2 implementation
        print("\n--- Phase 2: Restart with V2 implementation ---")
        ACTIVITY_VERSION = "v2"  # Change implementation

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        print("Orchestrator restarted, waiting for recovery...")
        time.sleep(4)  # Let recovery complete

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"Final status: {final_status}")

        # Analyze the results
        if final_status["status"] == "done":
            output = final_status["output"]
            print(f"\nWorkflow output: {output}")

            final_result = output.get("final_result", {})
            fetch_version = None
            process_version = None

            # Check which versions were used
            if "v1_data_from" in str(final_result):
                fetch_version = "v1 (cached)"
            elif "v2_NEW_data_from" in str(final_result):
                fetch_version = "v2 (re-executed)"

            if "v1_processed" in str(final_result):
                process_version = "v1 (cached)"
            elif "v2_NEW_processed" in str(final_result):
                process_version = "v2 (re-executed)"

            print(f"\nAnalysis:")
            print(f"  fetch_data used: {fetch_version}")
            print(f"  process_data used: {process_version}")

            if fetch_version == "v1 (cached)" and process_version == "v2 (re-executed)":
                print(
                    "✅ CORRECT: Used cached result for completed activity, new logic for incomplete activity"
                )
            elif (
                fetch_version == "v2 (re-executed)"
                and process_version == "v2 (re-executed)"
            ):
                print("⚠️  UNEXPECTED: Re-executed both activities with new logic")
            else:
                print(f"❓ UNCLEAR: Unexpected combination of versions")

        else:
            print(f"❌ Workflow failed: {final_status.get('error_message')}")

        orchestrator.stop()

    finally:
        # Cleanup
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)


def test_missing_activity():
    """Test what happens when activity is completely missing during recovery."""
    print("\n" + "=" * 60)
    print("Testing Missing Activity During Recovery")
    print("=" * 60)

    # Clean up
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    try:
        # Phase 1: Start workflow normally
        print("\n--- Phase 1: Start workflow normally ---")
        global ACTIVITY_VERSION
        ACTIVITY_VERSION = "v1"

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "test_implementation_change", source="database"
        )
        print(f"Started workflow: {workflow_id}")

        # Let fetch_data complete, interrupt during process_data
        time.sleep(1.5)

        orchestrator.stop()
        print("Orchestrator stopped")

        # Phase 2: Restart but "remove" the process_data activity
        print("\n--- Phase 2: Restart with missing process_data activity ---")

        # Clear activity registry and only register fetch_data
        from py_orchestrate.decorators import get_registry

        registry = get_registry()

        # Save original activities
        original_activities = registry.activities.copy()

        # Clear and only register fetch_data
        registry.activities.clear()
        fetch_data_flexible("dummy")  # Re-register just this one

        print(f"Available activities: {list(registry.activities.keys())}")

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        time.sleep(3)  # Let it try to recover

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"Final status: {final_status}")

        if final_status["status"] == "failed":
            print(f"✅ Expected failure: {final_status.get('error_message')}")
        elif final_status["status"] == "processing":
            print("⚠️  Workflow stuck in processing (expected)")
        else:
            print(f"❓ Unexpected status: {final_status}")

        orchestrator.stop()

        # Restore registry
        registry.activities.update(original_activities)

    finally:
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 test_activity_changes_simple.py <test>")
        print("Tests:")
        print("  implementation - Test activity implementation change")
        print("  missing       - Test missing activity")
        print("  all          - Run all tests")
        return

    test_type = sys.argv[1].lower()

    if test_type == "implementation" or test_type == "all":
        test_activity_implementation_change()

    if test_type == "missing" or test_type == "all":
        test_missing_activity()

    print("\nTesting complete!")


if __name__ == "__main__":
    main()
