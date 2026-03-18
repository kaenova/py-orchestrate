"""
Comprehensive test script for py-orchestrate recovery and persistence.
Tests sudden exit and workflow continuation scenarios.
"""

import time
import sys
import os
import signal
from py_orchestrate import workflow, activity, Orchestrator


@activity("step1_fetch")
def step1_fetch(source: str) -> dict:
    """First step: fetch data."""
    print(f"[STEP 1] Fetching data from {source}...")
    time.sleep(2)  # Simulate work
    result = {"data": f"fetched_from_{source}", "timestamp": time.time()}
    print(f"[STEP 1] Completed: {result}")
    return result


@activity("step2_process")
def step2_process(data: dict) -> dict:
    """Second step: process data."""
    print(f"[STEP 2] Processing data: {data}")
    time.sleep(3)  # Simulate longer work
    result = {
        "processed_data": data["data"] + "_processed",
        "processing_time": 3,
        "original_timestamp": data["timestamp"],
    }
    print(f"[STEP 2] Completed: {result}")
    return result


@activity("step3_validate")
def step3_validate(processed_data: dict) -> dict:
    """Third step: validate processed data."""
    print(f"[STEP 3] Validating: {processed_data}")
    time.sleep(2)  # Simulate validation
    result = {
        "validation_result": "PASSED",
        "validated_data": processed_data,
        "validation_timestamp": time.time(),
    }
    print(f"[STEP 3] Completed: {result}")
    return result


@activity("step4_save")
def step4_save(validated_data: dict, destination: str) -> dict:
    """Fourth step: save final data."""
    print(f"[STEP 4] Saving to {destination}: {validated_data}")
    time.sleep(2)  # Simulate save
    result = {
        "saved": True,
        "destination": destination,
        "save_timestamp": time.time(),
        "final_data": validated_data,
    }
    print(f"[STEP 4] Completed: {result}")
    return result


@workflow("recovery_test_workflow")
def recovery_test_workflow(
    source: str, destination: str, workflow_id: str = "test"
) -> dict:
    """A multi-step workflow that can be interrupted and resumed."""
    print(f"[WORKFLOW] Starting recovery test workflow (ID: {workflow_id})")
    print(f"[WORKFLOW] Source: {source}, Destination: {destination}")

    # Step 1: Fetch data (2 seconds)
    step1_result = step1_fetch(source)

    # Step 2: Process data (3 seconds) - this is where we might interrupt
    step2_result = step2_process(step1_result)

    # Step 3: Validate (2 seconds)
    step3_result = step3_validate(step2_result)

    # Step 4: Save (2 seconds)
    step4_result = step4_save(step3_result, destination)

    final_result = {
        "workflow_completed": True,
        "workflow_id": workflow_id,
        "total_steps": 4,
        "final_result": step4_result,
    }

    print(f"[WORKFLOW] Completed successfully: {final_result}")
    return final_result


def monitor_workflow(
    orchestrator: Orchestrator, workflow_id: str, timeout: int = 30
) -> dict:
    """Monitor a workflow until completion or timeout."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        status = orchestrator.get_workflow_status(workflow_id)
        if not status:
            print(f"Workflow {workflow_id} not found!")
            return None

        print(
            f"[MONITOR] Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
        )

        if status["status"] in ["done", "failed"]:
            if status["status"] == "done":
                print(f"[MONITOR] Workflow completed! Output: {status['output']}")
            else:
                print(f"[MONITOR] Workflow failed: {status['error_message']}")
            return status

        time.sleep(1)

    print(f"[MONITOR] Timeout after {timeout} seconds")
    return orchestrator.get_workflow_status(workflow_id)


def test_normal_execution():
    """Test normal workflow execution without interruption."""
    print("\n" + "=" * 60)
    print("TEST 1: Normal Workflow Execution")
    print("=" * 60)

    orchestrator = Orchestrator("test_normal.db")
    orchestrator.start()

    try:
        workflow_id = orchestrator.invoke_workflow(
            "recovery_test_workflow",
            source="database",
            destination="warehouse",
            workflow_id="normal_test",
        )

        print(f"Started workflow: {workflow_id}")
        final_status = monitor_workflow(orchestrator, workflow_id, timeout=15)

        if final_status and final_status["status"] == "done":
            print("✅ Normal execution test PASSED")
        else:
            print("❌ Normal execution test FAILED")

    finally:
        orchestrator.stop()


def start_interruptible_workflow():
    """Start a workflow that can be interrupted."""
    print("\n" + "=" * 60)
    print("TEST 2: Starting Interruptible Workflow")
    print("=" * 60)
    print("This workflow will run for about 9 seconds total.")
    print(
        "You can interrupt it with Ctrl+C after step 1 completes (around 2-3 seconds)"
    )
    print("Then run 'python3 test_recovery.py resume' to test recovery")

    orchestrator = Orchestrator("test_recovery.db")
    orchestrator.start()

    try:
        workflow_id = orchestrator.invoke_workflow(
            "recovery_test_workflow",
            source="api_server",
            destination="data_lake",
            workflow_id="interrupt_test",
        )

        print(f"Started interruptible workflow: {workflow_id}")
        print("Monitoring workflow... Press Ctrl+C to interrupt after step 1 completes")

        # Monitor with longer timeout
        final_status = monitor_workflow(orchestrator, workflow_id, timeout=30)

        if final_status and final_status["status"] == "done":
            print("✅ Workflow completed without interruption")
        else:
            print("⚠️  Workflow was interrupted or failed")

    except KeyboardInterrupt:
        print("\n🔄 Workflow interrupted! Database state preserved.")
        print("Run 'python3 test_recovery.py resume' to test recovery")

    finally:
        orchestrator.stop()


def test_recovery():
    """Test recovery of interrupted workflows."""
    print("\n" + "=" * 60)
    print("TEST 3: Recovery from Interruption")
    print("=" * 60)

    orchestrator = Orchestrator("test_recovery.db")
    orchestrator.start()

    try:
        # List all workflows to see what's available
        workflows = orchestrator.list_workflows("recovery_test_workflow")
        print(f"Found {len(workflows)} recovery test workflows:")

        for wf in workflows:
            print(f"  {wf['id']}: {wf['status']} (created: {wf['created_at']})")
            if wf["current_activity"]:
                print(f"    Current activity: {wf['current_activity']}")

        # Wait for recovery to kick in
        print("\nWaiting for recovery process to resume interrupted workflows...")
        time.sleep(10)  # Give recovery loop time to find and resume workflows

        # Check status again
        print("\nChecking workflow status after recovery:")
        for wf in workflows:
            if wf["status"] == "processing":
                updated_status = orchestrator.get_workflow_status(wf["id"])
                print(f"Workflow {wf['id']}: {updated_status['status']}")
                if updated_status["status"] == "processing":
                    print(
                        f"  Current activity: {updated_status.get('current_activity', 'None')}"
                    )

                # Monitor this workflow
                print(f"\nMonitoring recovered workflow {wf['id']}...")
                final_status = monitor_workflow(orchestrator, wf["id"], timeout=20)

                if final_status and final_status["status"] == "done":
                    print("✅ Recovery test PASSED - workflow resumed and completed")
                else:
                    print("❌ Recovery test FAILED")

    finally:
        orchestrator.stop()


def cleanup_test_data():
    """Clean up test database files."""
    print("\n" + "=" * 60)
    print("CLEANUP: Removing test databases")
    print("=" * 60)

    test_files = ["test_normal.db", "test_recovery.db"]

    for file in test_files:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed {file}")
        else:
            print(f"{file} not found")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 test_recovery.py normal    - Test normal execution")
        print("  python3 test_recovery.py interrupt - Start interruptible workflow")
        print("  python3 test_recovery.py resume    - Test recovery from interruption")
        print("  python3 test_recovery.py cleanup   - Clean up test files")
        print("  python3 test_recovery.py all       - Run all tests")
        return

    command = sys.argv[1]

    if command == "normal":
        test_normal_execution()
    elif command == "interrupt":
        start_interruptible_workflow()
    elif command == "resume":
        test_recovery()
    elif command == "cleanup":
        cleanup_test_data()
    elif command == "all":
        cleanup_test_data()
        test_normal_execution()
        print("\n" + "=" * 60)
        print("Now run: python3 test_recovery.py interrupt")
        print("Then interrupt it and run: python3 test_recovery.py resume")
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
