"""
Test script focused on signature change issues during recovery.
Creates long-running workflows that are easy to interrupt.
"""

import time
import sys
import os
from py_orchestrate import workflow, activity, Orchestrator


# Long running activities for easier interruption
@activity("long_step1")
def long_step1(data: str) -> dict:
    print(f"[STEP1] Starting long step1 with: {data}")
    for i in range(5):
        print(f"[STEP1] Progress {i + 1}/5...")
        time.sleep(2)
    result = {"step1_result": data.upper(), "step": 1}
    print(f"[STEP1] Completed: {result}")
    return result


@activity("long_step2")
def long_step2(data: dict) -> dict:
    print(f"[STEP2] Starting long step2 with: {data}")
    for i in range(4):
        print(f"[STEP2] Progress {i + 1}/4...")
        time.sleep(3)
    result = {"step2_result": data["step1_result"] + "_step2", "step": 2}
    print(f"[STEP2] Completed: {result}")
    return result


@workflow("signature_recovery_test")
def signature_recovery_test(input_data: str) -> dict:
    print(f"[WORKFLOW] Starting signature recovery test with: {input_data}")

    # Step 1: Long step (10 seconds)
    step1_result = long_step1(input_data)
    print("[WORKFLOW] Step 1 completed, moving to step 2...")

    # Step 2: Long step (12 seconds) - good place to interrupt
    step2_result = long_step2(step1_result)
    print("[WORKFLOW] Step 2 completed!")

    return {"final_result": step2_result, "workflow_completed": True}


def start_interruptible():
    """Start a workflow that can be interrupted."""
    print("\n" + "=" * 70)
    print("SIGNATURE TEST: Starting Long Workflow")
    print("=" * 70)
    print("This workflow takes ~22 seconds total (10s + 12s)")
    print("🎯 INTERRUPT after Step 1 completes (around 10 seconds)")

    orchestrator = Orchestrator("test_signature_recovery.db")
    orchestrator.start()

    try:
        workflow_id = orchestrator.invoke_workflow(
            "signature_recovery_test", input_data="test_signature_change"
        )

        print(f"Started workflow: {workflow_id}")
        print("=" * 70)

        start_time = time.time()
        while True:
            status = orchestrator.get_workflow_status(workflow_id)
            elapsed = int(time.time() - start_time)
            print(
                f"[{elapsed:02d}s] Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
            )

            if status["status"] in ["done", "failed"]:
                if status["status"] == "done":
                    print(f"✅ Completed: {status['output']}")
                else:
                    print(f"❌ Failed: {status['error_message']}")
                break

            time.sleep(2)

    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("🔄 WORKFLOW INTERRUPTED!")
        print("Now run: python3 test_signature_recovery.py recovery")
        print("=" * 70)

    finally:
        orchestrator.stop()


def test_recovery_with_signature_changes():
    """Test recovery when activity signatures have changed."""
    print("\n" + "=" * 70)
    print("SIGNATURE RECOVERY TEST")
    print("=" * 70)
    print("Testing recovery with CHANGED activity signatures...")

    # IMPORTANT: Redefine activities with DIFFERENT signatures
    @activity("long_step2")  # Same name but DIFFERENT signature!
    def long_step2_modified(
        data: dict, mode: str = "recovery", extra_param: int = 42
    ) -> dict:
        print(f"[STEP2-MODIFIED] Processing with NEW SIGNATURE: {data}")
        print(f"[STEP2-MODIFIED] Mode: {mode}, Extra: {extra_param}")

        for i in range(3):
            print(f"[STEP2-MODIFIED] New processing {i + 1}/3...")
            time.sleep(2)

        result = {
            "step2_result": data["step1_result"] + "_MODIFIED",
            "step": 2,
            "modified": True,
            "mode": mode,
            "extra_param": extra_param,
        }
        print(f"[STEP2-MODIFIED] Completed with new signature: {result}")
        return result

    orchestrator = Orchestrator("test_signature_recovery.db")
    orchestrator.start()

    try:
        # Check for interrupted workflows
        workflows = orchestrator.list_workflows("signature_recovery_test")
        print(f"Found {len(workflows)} signature recovery test workflows")

        interrupted = [wf for wf in workflows if wf["status"] == "processing"]
        print(f"Found {len(interrupted)} interrupted workflows")

        if not interrupted:
            print("❌ No interrupted workflows found!")
            print("Run: python3 test_signature_recovery.py start")
            print("Then interrupt it and run this recovery test.")
            return

        for wf in interrupted:
            print(f"📋 Workflow {wf['id'][:8]}: {wf['status']}")
            print(f"   Current activity: {wf.get('current_activity', 'None')}")
            print(f"   Created: {wf['created_at']}")

        print(f"\n🔄 Waiting for recovery system with CHANGED signatures...")
        print("⚠️  The long_step2 activity now has different parameters!")

        # Wait for recovery
        time.sleep(8)

        # Monitor recovery
        for wf in interrupted:
            workflow_id = wf["id"]
            print(f"\n📊 Monitoring recovery of {workflow_id[:8]}...")

            for check in range(20):  # Check for up to 40 seconds
                status = orchestrator.get_workflow_status(workflow_id)
                print(
                    f"  [{check * 2:02d}s] Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
                )

                if status["status"] in ["done", "failed"]:
                    if status["status"] == "done":
                        print(f"✅ RECOVERY SUCCESS with signature change!")
                        print(f"   Final result: {status['output']}")
                    else:
                        print(f"❌ RECOVERY FAILED due to signature change!")
                        print(f"   Error: {status['error_message']}")

                        # This is the interesting part - what's the error?
                        print("\n🔍 DETAILED ERROR ANALYSIS:")
                        print(f"   Error type: {type(status['error_message'])}")
                        print(f"   Error message: {status['error_message']}")
                    break

                time.sleep(2)
            else:
                print(f"⏰ Recovery monitoring timed out for {workflow_id[:8]}")

    finally:
        orchestrator.stop()


def show_database_state():
    """Show the current state of workflows in the database."""
    print("\n" + "=" * 70)
    print("DATABASE STATE")
    print("=" * 70)

    orchestrator = Orchestrator("test_signature_recovery.db")
    orchestrator.start()

    try:
        workflows = orchestrator.list_workflows("signature_recovery_test")
        print(f"Total workflows: {len(workflows)}")

        for i, wf in enumerate(workflows, 1):
            print(f"\n{i}. Workflow {wf['id'][:8]}...")
            print(f"   Status: {wf['status']}")
            print(f"   Current Activity: {wf.get('current_activity', 'None')}")
            print(f"   Created: {wf['created_at']}")
            if wf.get("error_message"):
                print(f"   Error: {wf['error_message']}")

    finally:
        orchestrator.stop()


def cleanup():
    """Clean up test database."""
    if os.path.exists("test_signature_recovery.db"):
        os.remove("test_signature_recovery.db")
        print("Cleaned up test_signature_recovery.db")


def main():
    if len(sys.argv) < 2:
        print("Signature Recovery Test Commands:")
        print(
            "  python3 test_signature_recovery.py start     - Start interruptible workflow"
        )
        print(
            "  python3 test_signature_recovery.py recovery  - Test recovery with changed signatures"
        )
        print("  python3 test_signature_recovery.py status    - Show database state")
        print("  python3 test_signature_recovery.py cleanup   - Remove test database")
        print("")
        print("Test Process:")
        print("1. Run 'start' and interrupt after step1 (10s)")
        print("2. Run 'recovery' to test signature change handling")
        return

    command = sys.argv[1]

    if command == "start":
        start_interruptible()
    elif command == "recovery":
        test_recovery_with_signature_changes()
    elif command == "status":
        show_database_state()
    elif command == "cleanup":
        cleanup()
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
