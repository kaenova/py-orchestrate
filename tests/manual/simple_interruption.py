#!/usr/bin/env python3
"""
Simple manual interruption test - interrupt when told, then run recovery separately.
"""

import os
import sys
import time
from py_orchestrate import Orchestrator, workflow, activity

TEST_DB = "test_simple_interruption.db"


@activity("quick_step")
def quick_step(input_data: str) -> dict:
    print(f"🔄 QUICK_STEP: Processing {input_data}")
    time.sleep(1)
    print(f"✅ QUICK_STEP: Completed")
    return {"quick_result": f"quick_done_{input_data}", "version": "original"}


@activity("slow_step")
def slow_step(data: dict) -> dict:
    print(f"🔄 SLOW_STEP: Starting long processing...")
    print(f"⏰ This will take 8 seconds - INTERRUPT ANYTIME during progress!")

    for i in range(8):
        time.sleep(1)
        print(
            f"   🕐 SLOW_STEP Progress: {i + 1}/8 seconds... (INTERRUPT NOW with Ctrl+C)"
        )

    print(f"✅ SLOW_STEP: Completed")
    return {"slow_result": f"slow_done_{data['quick_result']}", "version": "original"}


@workflow("simple_test")
def simple_test(input_data: str) -> dict:
    print(f"🚀 STARTING WORKFLOW with {input_data}")

    # Quick step (will complete before you can interrupt)
    result1 = quick_step(input_data)
    print(f"📋 Quick step done: {result1}")

    # Slow step (interrupt during this)
    result2 = slow_step(result1)
    print(f"📋 Slow step done: {result2}")

    return {"final": result2, "completed": True}


def start_workflow():
    """Start the workflow - you'll interrupt this."""
    print("=" * 60)
    print("🚀 STARTING WORKFLOW FOR INTERRUPTION")
    print("=" * 60)

    # Clean up
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    orchestrator = Orchestrator(TEST_DB)
    orchestrator.start()

    workflow_id = orchestrator.invoke_workflow("simple_test", input_data="test")
    print(f"🆔 Workflow ID: {workflow_id}")

    # Wait for quick_step to complete
    time.sleep(2)

    print(f"\n📊 Status after quick step:")
    status = orchestrator.get_workflow_status(workflow_id)
    print(f"   Status: {status['status']}")
    print(f"   Current activity: {status.get('current_activity')}")

    print(f"\n🛑 INTERRUPT COMING UP - GET READY TO PRESS Ctrl+C!")
    print(f"🛑 You'll see progress messages - interrupt during those!")

    # This will run the slow step - user should interrupt here
    time.sleep(15)  # Longer than the 8 seconds needed

    # If we get here, no interruption happened
    final_status = orchestrator.get_workflow_status(workflow_id)
    print(f"\n🏁 No interruption - workflow completed: {final_status['status']}")
    orchestrator.stop()


def check_recovery():
    """Check what happened after interruption and recover."""
    print("=" * 60)
    print("🔧 CHECKING RECOVERY AFTER INTERRUPTION")
    print("=" * 60)

    if not os.path.exists(TEST_DB):
        print("❌ No database found - run 'start' first and interrupt it")
        return

    # First check database directly
    print("🔍 Checking database contents directly:")
    import sqlite3

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.execute("SELECT id, name, status, current_activity FROM workflows")
    db_workflows = cursor.fetchall()
    conn.close()

    print(f"📋 Found {len(db_workflows)} workflows in database:")
    for wf in db_workflows:
        workflow_id, name, status, current_activity = wf
        print(
            f"   🆔 {workflow_id[:8]}... - {name} - {status} - current: {current_activity}"
        )

    if not db_workflows:
        print("❌ No workflows found in database")
        return

    # Now start orchestrator for recovery
    print(f"\n🔄 Starting orchestrator for recovery...")
    orchestrator = Orchestrator(TEST_DB)
    orchestrator.start()

    # Get the workflow ID from database
    workflow_id = db_workflows[0][0]

    print(f"⏳ Waiting for automatic recovery of workflow {workflow_id[:8]}...")

    # Check status immediately
    initial_status = orchestrator.get_workflow_status(workflow_id)
    print(
        f"📊 Initial status: {initial_status['status']} - current activity: {initial_status.get('current_activity')}"
    )

    # Wait for recovery
    time.sleep(10)

    # Check final status
    final_status = orchestrator.get_workflow_status(workflow_id)
    print(f"\n📊 RECOVERY RESULT:")
    print(f"   Status: {final_status['status']}")
    print(f"   Current activity: {final_status.get('current_activity')}")
    print(f"   Error: {final_status.get('error_message')}")

    if final_status["status"] == "done":
        print(f"   ✅ SUCCESS: Workflow recovered and completed!")
        output = final_status["output"]
        print(f"   📋 Final output: {output}")

        # Analyze what happened
        if "quick_done" in str(output) and "slow_done" in str(output):
            print(f"   🎯 ANALYSIS: Both steps completed")
            print(
                f"      - Quick step: Used cached result (completed before interruption)"
            )
            print(f"      - Slow step: Re-executed after recovery")

    elif final_status["status"] == "failed":
        print(f"   ❌ FAILED: {final_status.get('error_message')}")
    else:
        print(f"   ⏳ Still processing: {final_status['status']}")

    # Also list workflows through orchestrator API
    workflows = orchestrator.list_workflows()
    print(f"\n📋 Orchestrator API found {len(workflows)} workflows:")

    for wf in workflows:
        print(f"   🆔 {wf['id'][:8]}... - {wf['name']} - {wf['status']}")

        if wf["status"] == "processing":
            print(
                f"      🔄 This workflow is still processing - recovery should happen automatically"
            )
            print(f"      ⏳ Waiting for recovery to complete...")

            # Wait for recovery
            time.sleep(10)

            # Check final status
            final_status = orchestrator.get_workflow_status(wf["id"])
            print(f"\n📊 RECOVERY RESULT:")
            print(f"   Status: {final_status['status']}")
            print(f"   Current activity: {final_status.get('current_activity')}")
            print(f"   Error: {final_status.get('error_message')}")

            if final_status["status"] == "done":
                print(f"   ✅ SUCCESS: Workflow recovered and completed!")
                output = final_status["output"]
                print(f"   📋 Final output: {output}")

                # Analyze what happened
                if "quick_done" in str(output) and "slow_done" in str(output):
                    print(f"   🎯 ANALYSIS: Both steps completed")
                    print(
                        f"      - Quick step: Used cached result (completed before interruption)"
                    )
                    print(f"      - Slow step: Re-executed after recovery")

            elif final_status["status"] == "failed":
                print(f"   ❌ FAILED: {final_status.get('error_message')}")
            else:
                print(f"   ⏳ Still processing: {final_status['status']}")

        elif wf["status"] == "done":
            print(f"      ✅ This workflow already completed")
        elif wf["status"] == "failed":
            print(f"      ❌ This workflow failed: {wf.get('error_message')}")

    orchestrator.stop()


def clean_up():
    """Clean up test database."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
        print(f"🧹 Cleaned up {TEST_DB}")
    else:
        print(f"🧹 No database to clean up")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 test_simple_interruption.py <command>")
        print("Commands:")
        print("  start    - Start workflow (you interrupt with Ctrl+C)")
        print("  recover  - Check recovery after interruption")
        print("  clean    - Clean up test database")
        print("")
        print("Typical usage:")
        print("  1. python3 test_simple_interruption.py start")
        print("     (interrupt with Ctrl+C when you see progress messages)")
        print("  2. python3 test_simple_interruption.py recover")
        print("     (check what happened and see recovery)")
        print("  3. python3 test_simple_interruption.py clean")
        return

    command = sys.argv[1].lower()

    if command == "start":
        start_workflow()
    elif command == "recover":
        check_recovery()
    elif command == "clean":
        clean_up()
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
