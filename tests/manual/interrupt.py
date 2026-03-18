"""
Test script specifically for testing workflow interruption and recovery.
Uses longer delays to allow for manual interruption.
"""

import time
import sys
import os
from py_orchestrate import workflow, activity, Orchestrator


@activity("long_step1")
def long_step1(source: str) -> dict:
    """Step 1: Long running fetch operation."""
    print(f"[STEP 1] Starting long fetch from {source}...")
    for i in range(5):
        print(f"[STEP 1] Progress: {i + 1}/5...")
        time.sleep(2)  # 10 seconds total
    result = {"data": f"fetched_from_{source}", "timestamp": time.time()}
    print(f"[STEP 1] COMPLETED: {result}")
    return result


@activity("long_step2")
def long_step2(data: dict) -> dict:
    """Step 2: Long running processing."""
    print(f"[STEP 2] Starting long processing of: {data}")
    for i in range(4):
        print(f"[STEP 2] Processing phase {i + 1}/4...")
        time.sleep(3)  # 12 seconds total
    result = {
        "processed_data": data["data"] + "_processed",
        "processing_time": 12,
        "original_timestamp": data["timestamp"],
    }
    print(f"[STEP 2] COMPLETED: {result}")
    return result


@activity("long_step3")
def long_step3(processed_data: dict) -> dict:
    """Step 3: Long validation."""
    print(f"[STEP 3] Starting validation of: {processed_data}")
    for i in range(3):
        print(f"[STEP 3] Validation check {i + 1}/3...")
        time.sleep(2)  # 6 seconds total
    result = {
        "validation_result": "PASSED",
        "validated_data": processed_data,
        "validation_timestamp": time.time(),
    }
    print(f"[STEP 3] COMPLETED: {result}")
    return result


@workflow("long_workflow")
def long_workflow(source: str, destination: str) -> dict:
    """A long-running workflow designed to be interrupted."""
    print(f"[WORKFLOW] Starting long workflow")
    print(f"[WORKFLOW] This will take about 28 seconds total (10+12+6)")
    print(f"[WORKFLOW] Source: {source}, Destination: {destination}")

    # Step 1: 10 seconds
    step1_result = long_step1(source)
    print(f"[WORKFLOW] Step 1 completed, moving to step 2...")

    # Step 2: 12 seconds (good place to interrupt)
    step2_result = long_step2(step1_result)
    print(f"[WORKFLOW] Step 2 completed, moving to step 3...")

    # Step 3: 6 seconds
    step3_result = long_step3(step2_result)

    final_result = {
        "workflow_completed": True,
        "total_steps": 3,
        "final_result": step3_result,
    }

    print(f"[WORKFLOW] ALL STEPS COMPLETED: {final_result}")
    return final_result


def start_long_workflow():
    """Start a workflow that's easy to interrupt."""
    print("\n" + "=" * 60)
    print("INTERRUPT TEST: Starting Long Workflow")
    print("=" * 60)
    print("This workflow will take about 28 seconds.")
    print("RECOMMENDED: Interrupt with Ctrl+C after Step 1 completes (10 seconds)")
    print("Then run: python3 test_interrupt.py resume")

    orchestrator = Orchestrator("test_interrupt.db")
    orchestrator.start()

    try:
        workflow_id = orchestrator.invoke_workflow(
            "long_workflow", source="large_database", destination="data_warehouse"
        )

        print(f"\nStarted workflow: {workflow_id}")
        print("=" * 60)

        # Monitor workflow with status updates
        start_time = time.time()
        while True:
            status = orchestrator.get_workflow_status(workflow_id)
            if not status:
                print("Workflow not found!")
                break

            elapsed = int(time.time() - start_time)
            print(
                f"[{elapsed:02d}s] Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
            )

            if status["status"] in ["done", "failed"]:
                if status["status"] == "done":
                    print(f"[COMPLETED] Workflow finished: {status['output']}")
                else:
                    print(f"[FAILED] Workflow error: {status['error_message']}")
                break

            time.sleep(2)  # Check every 2 seconds

    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("🔄 WORKFLOW INTERRUPTED!")
        print("Database state has been preserved.")
        print("Run 'python3 test_interrupt.py resume' to test recovery")
        print("=" * 60)

    finally:
        orchestrator.stop()


def test_resume():
    """Test resuming interrupted workflows."""
    print("\n" + "=" * 60)
    print("RECOVERY TEST: Resuming Interrupted Workflows")
    print("=" * 60)

    orchestrator = Orchestrator("test_interrupt.db")
    orchestrator.start()

    try:
        # Check for processing workflows
        workflows = orchestrator.list_workflows("long_workflow")
        print(f"Found {len(workflows)} long_workflow instances:")

        processing_workflows = []
        for wf in workflows:
            print(f"  {wf['id'][:8]}...: {wf['status']} (created: {wf['created_at']})")
            if wf["current_activity"]:
                print(f"    Current activity: {wf['current_activity']}")
            if wf["status"] == "processing":
                processing_workflows.append(wf)

        if not processing_workflows:
            print("\n❌ No interrupted workflows found!")
            print("Run 'python3 test_interrupt.py start' first, then interrupt it.")
            return

        print(f"\n🔄 Found {len(processing_workflows)} interrupted workflow(s)")
        print("Waiting for recovery system to resume them...")

        # Wait for recovery to kick in
        time.sleep(8)  # Give recovery loop time to find and start resuming

        # Monitor recovery progress
        for wf in processing_workflows:
            workflow_id = wf["id"]
            print(f"\n📊 Monitoring recovery of workflow {workflow_id[:8]}...")

            start_time = time.time()
            while time.time() - start_time < 60:  # 60 second timeout
                status = orchestrator.get_workflow_status(workflow_id)
                if not status:
                    print("❌ Workflow not found!")
                    break

                elapsed = int(time.time() - start_time)
                print(
                    f"[{elapsed:02d}s] Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
                )

                if status["status"] in ["done", "failed"]:
                    if status["status"] == "done":
                        print(
                            f"✅ RECOVERY SUCCESS! Workflow completed: {status['output']['workflow_completed']}"
                        )
                    else:
                        print(f"❌ RECOVERY FAILED: {status['error_message']}")
                    break

                time.sleep(3)  # Check every 3 seconds
            else:
                print("⏰ Recovery monitoring timed out")

    finally:
        orchestrator.stop()


def cleanup():
    """Clean up test database."""
    if os.path.exists("test_interrupt.db"):
        os.remove("test_interrupt.db")
        print("Cleaned up test_interrupt.db")
    else:
        print("No test database to clean up")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print(
            "  python3 test_interrupt.py start   - Start long workflow (interrupt with Ctrl+C)"
        )
        print(
            "  python3 test_interrupt.py resume  - Test recovery of interrupted workflow"
        )
        print("  python3 test_interrupt.py cleanup - Remove test database")
        return

    command = sys.argv[1]

    if command == "start":
        start_long_workflow()
    elif command == "resume":
        test_resume()
    elif command == "cleanup":
        cleanup()
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
