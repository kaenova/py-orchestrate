"""
Test script to explore what happens when activity signatures change
between workflow runs and recoveries.
"""

import time
import sys
import os
from py_orchestrate import workflow, activity, Orchestrator


# Original activity signature
@activity("process_data_v1")
def process_data_v1(data: str) -> dict:
    """Version 1: Simple string input."""
    print(f"[V1] Processing data: {data}")
    time.sleep(2)
    result = {"processed": data.upper(), "version": "v1"}
    print(f"[V1] Result: {result}")
    return result


# Modified activity signature - added parameter
@activity("process_data_v2")
def process_data_v2(data: str, mode: str = "default") -> dict:
    """Version 2: Added mode parameter."""
    print(f"[V2] Processing data: {data} with mode: {mode}")
    time.sleep(2)
    result = {"processed": data.upper(), "mode": mode, "version": "v2"}
    print(f"[V2] Result: {result}")
    return result


# Modified activity signature - changed parameter type
@activity("process_data_v3")
def process_data_v3(data: dict) -> dict:
    """Version 3: Changed data from string to dict."""
    print(f"[V3] Processing data dict: {data}")
    time.sleep(2)
    result = {"processed": str(data).upper(), "version": "v3"}
    print(f"[V3] Result: {result}")
    return result


@workflow("signature_test_workflow_v1")
def signature_test_workflow_v1(input_data: str) -> dict:
    """Original workflow using v1 activity."""
    print(f"[WORKFLOW V1] Starting with: {input_data}")

    # Use original activity
    result = process_data_v1(input_data)

    return {"workflow_result": result, "workflow_version": "v1"}


@workflow("signature_test_workflow_v2")
def signature_test_workflow_v2(input_data: str) -> dict:
    """Modified workflow using v2 activity with new parameter."""
    print(f"[WORKFLOW V2] Starting with: {input_data}")

    # Use modified activity with new parameter
    result = process_data_v2(input_data, mode="enhanced")

    return {"workflow_result": result, "workflow_version": "v2"}


@workflow("signature_test_workflow_v3")
def signature_test_workflow_v3(input_data: str) -> dict:
    """Workflow that changes how it calls activities."""
    print(f"[WORKFLOW V3] Starting with: {input_data}")

    # Convert string to dict and use v3 activity
    data_dict = {"original": input_data, "timestamp": time.time()}
    result = process_data_v3(data_dict)

    return {"workflow_result": result, "workflow_version": "v3"}


def test_scenario_1():
    """Test 1: Adding parameters to activities."""
    print("\n" + "=" * 70)
    print("TEST 1: Adding Parameters to Activities")
    print("=" * 70)
    print("This tests what happens when an activity gains new parameters")

    orchestrator = Orchestrator("test_signature_changes.db")
    orchestrator.start()

    try:
        # Start with v1 workflow
        print("\n🚀 Running V1 workflow...")
        workflow_id = orchestrator.invoke_workflow(
            "signature_test_workflow_v1", input_data="hello_world"
        )

        # Monitor until completion
        while True:
            status = orchestrator.get_workflow_status(workflow_id)
            print(
                f"Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
            )

            if status["status"] in ["done", "failed"]:
                if status["status"] == "done":
                    print(f"✅ V1 completed: {status['output']}")
                else:
                    print(f"❌ V1 failed: {status['error_message']}")
                break
            time.sleep(0.5)

    finally:
        orchestrator.stop()


def test_scenario_2():
    """Test 2: What happens during recovery with signature changes."""
    print("\n" + "=" * 70)
    print("TEST 2: Recovery with Signature Changes")
    print("=" * 70)
    print("This tests recovery when activity signatures have changed")

    orchestrator = Orchestrator("test_signature_changes.db")
    orchestrator.start()

    try:
        # Check existing workflows
        workflows = orchestrator.list_workflows("signature_test_workflow_v1")
        print(f"Found {len(workflows)} existing v1 workflows")

        # Now try to run v2 workflow which uses different activity signature
        print("\n🔄 Now running V2 workflow (with changed activity signature)...")
        workflow_id = orchestrator.invoke_workflow(
            "signature_test_workflow_v2", input_data="hello_world_v2"
        )

        # Monitor until completion
        while True:
            status = orchestrator.get_workflow_status(workflow_id)
            print(
                f"Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
            )

            if status["status"] in ["done", "failed"]:
                if status["status"] == "done":
                    print(f"✅ V2 completed: {status['output']}")
                else:
                    print(f"❌ V2 failed: {status['error_message']}")
                    print("Error details:")
                    print(status["error_message"])
                break
            time.sleep(0.5)

    finally:
        orchestrator.stop()


def test_scenario_3():
    """Test 3: Create an interruptible workflow and change signatures during recovery."""
    print("\n" + "=" * 70)
    print("TEST 3: Interrupt Workflow and Change Signatures")
    print("=" * 70)
    print("Start workflow, interrupt it, then test recovery with changed signatures")

    # First, create a multi-step workflow that we can interrupt
    @activity("step1_fetch")
    def step1_fetch(source: str) -> dict:
        print(f"[STEP1] Fetching from {source}...")
        time.sleep(3)
        return {"data": f"fetched_from_{source}", "step": 1}

    @activity("step2_process")
    def step2_process(data: dict) -> dict:
        print(f"[STEP2] Processing {data}...")
        time.sleep(3)
        return {"processed": data["data"] + "_processed", "step": 2}

    @workflow("interruptible_workflow")
    def interruptible_workflow(source: str) -> dict:
        print(f"[WORKFLOW] Starting interruptible workflow with {source}")

        step1_result = step1_fetch(source)
        step2_result = step2_process(step1_result)

        return {"final": step2_result}

    orchestrator = Orchestrator("test_signature_interrupt.db")
    orchestrator.start()

    try:
        print("🚀 Starting interruptible workflow...")
        workflow_id = orchestrator.invoke_workflow(
            "interruptible_workflow", source="test_database"
        )

        print(f"Started workflow {workflow_id}")
        print("💡 MANUAL STEP: Interrupt this with Ctrl+C after step1 completes")

        # Monitor
        start_time = time.time()
        while time.time() - start_time < 10:  # 10 second timeout
            status = orchestrator.get_workflow_status(workflow_id)
            print(
                f"Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
            )

            if status["status"] in ["done", "failed"]:
                if status["status"] == "done":
                    print(f"✅ Completed before interrupt: {status['output']}")
                else:
                    print(f"❌ Failed: {status['error_message']}")
                break
            time.sleep(1)
        else:
            print("⏰ Timeout - workflow may still be running")

    except KeyboardInterrupt:
        print("\n🔄 Workflow interrupted!")

    finally:
        orchestrator.stop()


def test_recovery_with_changed_signatures():
    """Test recovery when signatures have changed."""
    print("\n" + "=" * 70)
    print("TEST 4: Recovery with Changed Activity Signatures")
    print("=" * 70)

    # Redefine activities with different signatures
    @activity("step2_process")  # Same name, different signature
    def step2_process_new(data: dict, enhancement: str = "default") -> dict:
        print(f"[STEP2-NEW] Processing {data} with enhancement {enhancement}...")
        time.sleep(2)
        return {
            "processed": data["data"] + f"_processed_{enhancement}",
            "step": 2,
            "enhanced": True,
        }

    orchestrator = Orchestrator("test_signature_interrupt.db")
    orchestrator.start()

    try:
        # Check for interrupted workflows
        workflows = orchestrator.list_workflows("interruptible_workflow")
        print(f"Found {len(workflows)} interruptible workflows")

        processing_workflows = [wf for wf in workflows if wf["status"] == "processing"]
        print(f"Found {len(processing_workflows)} interrupted workflows")

        if processing_workflows:
            print("🔄 Letting recovery system attempt to resume...")
            time.sleep(8)  # Let recovery kick in

            # Check results
            for wf in processing_workflows:
                status = orchestrator.get_workflow_status(wf["id"])
                print(f"Workflow {wf['id'][:8]}: {status['status']}")
                if status["status"] == "failed":
                    print(f"❌ Recovery failed: {status['error_message']}")
                elif status["status"] == "done":
                    print(f"✅ Recovery succeeded: {status['output']}")
                else:
                    print(
                        f"🔄 Still processing: {status.get('current_activity', 'None')}"
                    )
        else:
            print("No interrupted workflows to test recovery with")

    finally:
        orchestrator.stop()


def cleanup():
    """Clean up test databases."""
    test_files = ["test_signature_changes.db", "test_signature_interrupt.db"]
    for file in test_files:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed {file}")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 test_signatures.py scenario1    - Test adding parameters")
        print("  python3 test_signatures.py scenario2    - Test signature changes")
        print(
            "  python3 test_signatures.py interrupt    - Create interruptible workflow"
        )
        print(
            "  python3 test_signatures.py recovery     - Test recovery with changed signatures"
        )
        print("  python3 test_signatures.py cleanup      - Clean up test files")
        print("  python3 test_signatures.py all          - Run all signature tests")
        return

    command = sys.argv[1]

    if command == "scenario1":
        test_scenario_1()
    elif command == "scenario2":
        test_scenario_2()
    elif command == "interrupt":
        test_scenario_3()
    elif command == "recovery":
        test_recovery_with_changed_signatures()
    elif command == "cleanup":
        cleanup()
    elif command == "all":
        cleanup()
        print("Running all signature change tests...")
        test_scenario_1()
        test_scenario_2()
        print("\n💡 To test recovery scenarios:")
        print("1. Run: python3 test_signatures.py interrupt")
        print("2. Interrupt it with Ctrl+C after step1")
        print("3. Run: python3 test_signatures.py recovery")
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
