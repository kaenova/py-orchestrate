#!/usr/bin/env python3
"""
Test activity changes with forced interruption during execution.
"""

import os
import sys
import time
import signal
import tempfile
import threading
from py_orchestrate import Orchestrator, workflow, activity

TEST_DB = "test_forced_interruption.db"


@activity("step1")
def step1(input_data: str) -> dict:
    print(f"STEP1: Processing {input_data}")
    time.sleep(1)
    return {"step1_result": f"step1_completed_{input_data}", "version": "original"}


@activity("step2")
def step2(data: dict) -> dict:
    print(f"STEP2: Processing {data}")
    time.sleep(5)  # Long running to ensure we can interrupt
    return {
        "step2_result": f"step2_completed_{data['step1_result']}",
        "version": "original",
    }


@activity("step2")  # Same name, different implementation
def step2_new(data: dict) -> dict:
    print(f"STEP2 NEW: Processing with new logic {data}")
    time.sleep(2)
    return {
        "step2_result": f"step2_NEW_completed_{data['step1_result']}",
        "version": "new",
    }


@workflow("test_forced_interruption")
def test_forced_interruption(input_data: str) -> dict:
    print(f"WORKFLOW: Starting with {input_data}")

    # Step 1: Quick step (will complete)
    result1 = step1(input_data)
    print(f"WORKFLOW: Step1 done: {result1}")

    # Step 2: Long step (will be interrupted)
    result2 = step2(result1)
    print(f"WORKFLOW: Step2 done: {result2}")

    return {"final": result2, "completed": True}


def test_with_interruption():
    """Test with manual interruption during step2."""
    print("=" * 60)
    print("Testing Activity Change with Forced Interruption")
    print("=" * 60)

    # Clean up
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    try:
        print("\n--- Phase 1: Start workflow and interrupt during step2 ---")

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "test_forced_interruption", input_data="test_input"
        )
        print(f"Started workflow: {workflow_id}")

        # Let step1 complete (1s) and step2 start (5s total)
        time.sleep(2)  # step1 done, step2 running

        status = orchestrator.get_workflow_status(workflow_id)
        print(f"Status during step2: {status}")

        # Force stop during step2
        print("Forcing orchestrator stop during step2...")
        orchestrator.stop()

        print("\n--- Phase 2: Restart with same activity (should resume) ---")

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        print("Waiting for recovery to complete...")
        time.sleep(6)  # Let step2 complete

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"Final status: {final_status}")

        if final_status["status"] == "done":
            output = final_status["output"]
            print(f"Output: {output}")

            if "step1_completed" in str(output) and "step2_completed" in str(output):
                print("✅ SUCCESS: Workflow resumed and completed")

                if "version" in str(output):
                    version_info = output.get("final", {}).get("version", "unknown")
                    print(f"   Used version: {version_info}")
            else:
                print("❌ UNEXPECTED: Workflow completed but with unexpected results")
        else:
            print(f"❌ FAILED: {final_status.get('error_message')}")

        orchestrator.stop()

    finally:
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)


def test_activity_missing():
    """Test missing activity during recovery."""
    print("\n" + "=" * 60)
    print("Testing Missing Activity During Recovery")
    print("=" * 60)

    # Clean up
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    try:
        print("\n--- Phase 1: Start workflow normally ---")

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "test_forced_interruption", input_data="test_input"
        )
        print(f"Started workflow: {workflow_id}")

        # Interrupt during step2
        time.sleep(2)
        orchestrator.stop()
        print("Orchestrator stopped during step2")

        print("\n--- Phase 2: Restart but remove step2 activity ---")

        # Clear step2 from registry (simulate deleted activity)
        from py_orchestrate.decorators import get_registry

        registry = get_registry()

        print(f"Activities before removal: {list(registry.activities.keys())}")

        # Remove step2 activity
        if "step2" in registry.activities:
            del registry.activities["step2"]
            print("Removed step2 activity from registry")

        print(f"Activities after removal: {list(registry.activities.keys())}")

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        print("Waiting for recovery attempt...")
        time.sleep(4)

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"Final status: {final_status}")

        if final_status["status"] == "failed":
            print(f"✅ Expected failure: {final_status.get('error_message')}")
        elif final_status["status"] == "processing":
            print("⚠️  Workflow stuck in processing (activity not found)")
        else:
            print(f"❓ Unexpected result: {final_status}")

        orchestrator.stop()

    finally:
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 test_forced_interruption.py <test>")
        print("Tests:")
        print("  interrupt - Test interruption and recovery")
        print("  missing   - Test missing activity")
        print("  all       - Run all tests")
        return

    test_type = sys.argv[1].lower()

    if test_type == "interrupt" or test_type == "all":
        test_with_interruption()

    if test_type == "missing" or test_type == "all":
        test_activity_missing()

    print("\nAll tests completed!")


if __name__ == "__main__":
    main()
