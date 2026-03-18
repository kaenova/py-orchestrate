#!/usr/bin/env python3
"""
Interactive test for activity changes during manual interruption.
This test will tell you exactly when to interrupt it.
"""

import os
import sys
import time
import tempfile
from py_orchestrate import Orchestrator, workflow, activity

TEST_DB = "test_manual_interruption.db"


@activity("step1")
def step1(input_data: str) -> dict:
    print(f"🔄 STEP1: Processing {input_data}")
    time.sleep(2)  # 2 seconds
    print(f"✅ STEP1: Completed processing {input_data}")
    return {"step1_result": f"step1_completed_{input_data}", "version": "original"}


@activity("step2")
def step2(data: dict) -> dict:
    print(f"🔄 STEP2: Starting long processing of {data}")
    print(f"⏰ STEP2: This will take 10 seconds...")

    for i in range(10):
        time.sleep(1)
        print(f"   STEP2: Progress {i + 1}/10 seconds...")

    print(f"✅ STEP2: Completed processing")
    return {
        "step2_result": f"step2_completed_{data['step1_result']}",
        "version": "original",
    }


# Alternative implementation with different logic
@activity("step2_new")
def step2_new(data: dict) -> dict:
    print(f"🔄 STEP2_NEW: Starting NEW LOGIC processing of {data}")
    time.sleep(3)
    print(f"✅ STEP2_NEW: Completed with NEW LOGIC")
    return {
        "step2_result": f"step2_NEW_completed_{data['step1_result']}",
        "version": "new",
    }


@workflow("manual_interruption_test")
def manual_interruption_test(input_data: str) -> dict:
    print(f"🚀 WORKFLOW: Starting with {input_data}")

    # Step 1: Quick step (will complete)
    result1 = step1(input_data)
    print(f"📋 WORKFLOW: Step1 completed: {result1}")

    # Step 2: Long step (interrupt during this)
    result2 = step2(result1)
    print(f"📋 WORKFLOW: Step2 completed: {result2}")

    return {"final": result2, "completed": True}


def test_manual_interruption():
    """Test with clear instructions for manual interruption."""
    print("=" * 80)
    print("🧪 MANUAL INTERRUPTION TEST")
    print("=" * 80)

    # Clean up
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    try:
        print("\n📍 Phase 1: Starting workflow - YOU WILL INTERRUPT DURING STEP2")
        print("🔍 Watch for the message: '🛑 INTERRUPT NOW!' and press Ctrl+C")
        print("\n⏳ Starting workflow in 3 seconds...")

        for i in range(3, 0, -1):
            print(f"   Starting in {i}...")
            time.sleep(1)

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "manual_interruption_test", input_data="test_data"
        )
        print(f"🆔 Started workflow: {workflow_id}")

        # Wait for step1 to complete (2 seconds)
        time.sleep(3)

        # Check status
        status = orchestrator.get_workflow_status(workflow_id)
        print(f"\n📊 Current status: {status}")

        # Give clear interruption instruction
        print("\n" + "=" * 80)
        print("🛑 INTERRUPT NOW! Press Ctrl+C to simulate crash during STEP2!")
        print(
            "🛑 You should see STEP2 progress messages - interrupt anytime during those"
        )
        print("=" * 80)

        # Wait for the long step2 (or interruption)
        time.sleep(15)  # step2 takes 10 seconds

        # If we get here, workflow completed without interruption
        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"\n🏁 Workflow completed without interruption: {final_status}")
        orchestrator.stop()

    except KeyboardInterrupt:
        print(f"\n\n💥 INTERRUPTED! Simulating crash...")
        try:
            orchestrator.stop()
        except:
            pass

        print(f"\n📍 Phase 2: Restarting orchestrator to test recovery...")
        print(f"🔄 The workflow should resume and complete step2...")

        time.sleep(2)

        # Restart orchestrator
        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        print(f"⏳ Waiting for recovery to complete...")
        time.sleep(12)  # Give enough time for step2 to complete

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"\n📊 Final status after recovery: {final_status}")

        if final_status["status"] == "done":
            output = final_status["output"]
            print(f"✅ SUCCESS: Workflow recovered and completed!")
            print(f"📋 Final output: {output}")

            # Check which activities were used
            final_result = output.get("final", {})
            if "step1_completed" in str(final_result):
                print(f"   ✅ Step1: Used cached result (as expected)")
            if "step2_completed" in str(final_result):
                print(f"   ✅ Step2: Completed after recovery")
        else:
            print(f"❌ FAILURE: {final_status.get('error_message')}")

        orchestrator.stop()

    finally:
        # Cleanup
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)


def test_activity_change_during_recovery():
    """Test what happens when activity implementation changes during recovery."""
    print("\n" + "=" * 80)
    print("🧪 ACTIVITY CHANGE DURING RECOVERY TEST")
    print("=" * 80)

    # Clean up
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    try:
        print("\n📍 Phase 1: Starting workflow with original activity")

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        workflow_id = orchestrator.invoke_workflow(
            "manual_interruption_test", input_data="test_data"
        )
        print(f"🆔 Started workflow: {workflow_id}")

        # Wait for step1, then interrupt during step2
        time.sleep(3)

        print("\n" + "=" * 60)
        print("🛑 INTERRUPT NOW! Press Ctrl+C during STEP2!")
        print("=" * 60)

        time.sleep(15)

        # Workflow completed without interruption
        print("🏁 Workflow completed without interruption")
        orchestrator.stop()

    except KeyboardInterrupt:
        print(f"\n\n💥 INTERRUPTED during STEP2!")
        try:
            orchestrator.stop()
        except:
            pass

        print(f"\n📍 Phase 2: Changing activity implementation and restarting...")

        # Simulate changing the activity by clearing registry and re-registering
        from py_orchestrate.decorators import get_registry

        registry = get_registry()

        print(f"🔧 Original activities: {list(registry.activities.keys())}")

        # Clear step2 and register the new version
        if "step2" in registry.activities:
            del registry.activities["step2"]
            print(f"🗑️  Removed original step2")

        # Register new implementation
        step2_new({"dummy": "data"})  # This registers step2_new

        # But we need step2, not step2_new for the workflow
        # Let's manually register step2_new as step2
        registry.activities["step2"] = registry.activities["step2_new"]

        print(f"🔧 New activities: {list(registry.activities.keys())}")
        print(f"🔄 Restarting orchestrator with changed activity...")

        orchestrator = Orchestrator(TEST_DB)
        orchestrator.start()

        print(f"⏳ Waiting for recovery with new activity implementation...")
        time.sleep(5)

        final_status = orchestrator.get_workflow_status(workflow_id)
        print(f"\n📊 Final status: {final_status}")

        if final_status["status"] == "done":
            output = final_status["output"]
            print(f"✅ SUCCESS: Workflow recovered with new activity!")
            print(f"📋 Output: {output}")

            # Check which version was used
            if "version" in str(output):
                version = output.get("final", {}).get("version", "unknown")
                if version == "new":
                    print(f"   🆕 Used NEW activity implementation")
                elif version == "original":
                    print(f"   🔄 Used cached result from original implementation")
        else:
            print(f"❌ FAILURE: {final_status.get('error_message')}")

        orchestrator.stop()

    finally:
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 test_manual_interruption.py <test>")
        print("Tests:")
        print("  basic  - Basic interruption and recovery test")
        print("  change - Activity change during recovery test")
        return

    test_type = sys.argv[1].lower()

    if test_type == "basic":
        test_manual_interruption()
    elif test_type == "change":
        test_activity_change_during_recovery()
    else:
        print(f"Unknown test: {test_type}")


if __name__ == "__main__":
    main()
