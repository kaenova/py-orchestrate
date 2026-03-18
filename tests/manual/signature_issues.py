"""
Test what happens with workflow signature changes and true activity signature mismatches.
"""

import time
import sys
import os
from py_orchestrate import workflow, activity, Orchestrator


@activity("stable_activity")
def stable_activity(input_data: str) -> dict:
    """An activity that doesn't change."""
    print(f"[STABLE] Processing: {input_data}")
    time.sleep(2)
    return {"stable_result": input_data.upper()}


# Test 1: Workflow signature changes
@workflow("workflow_signature_test_v1")
def workflow_signature_test_v1(input_data: str) -> dict:
    """Original workflow signature."""
    print(f"[WORKFLOW V1] Processing: {input_data}")
    result = stable_activity(input_data)
    return {"version": "v1", "result": result}


@workflow("workflow_signature_test_v2")
def workflow_signature_test_v2(input_data: str, extra_param: str = "default") -> dict:
    """Modified workflow signature with extra parameter."""
    print(f"[WORKFLOW V2] Processing: {input_data} with extra: {extra_param}")
    result = stable_activity(input_data)
    return {"version": "v2", "result": result, "extra": extra_param}


# Test 2: Force activity signature mismatch by changing how we call activities
@activity("flexible_activity")
def flexible_activity(*args, **kwargs) -> dict:
    """Activity that can handle different signatures."""
    print(f"[FLEXIBLE] Args: {args}, Kwargs: {kwargs}")
    time.sleep(2)

    # Handle different calling patterns
    if len(args) == 1 and isinstance(args[0], str):
        return {"flexible_result": f"string_{args[0]}"}
    elif len(args) == 1 and isinstance(args[0], dict):
        return {"flexible_result": f"dict_{args[0]}"}
    elif len(args) == 2:
        return {"flexible_result": f"two_args_{args[0]}_{args[1]}"}
    else:
        return {"flexible_result": f"unknown_{args}_{kwargs}"}


@workflow("calling_pattern_test_v1")
def calling_pattern_test_v1(input_data: str) -> dict:
    """Workflow that calls activity one way."""
    print(f"[CALLING V1] Calling with string: {input_data}")
    # Call with single string argument
    result = flexible_activity(input_data)
    return {"version": "v1", "result": result}


@workflow("calling_pattern_test_v2")
def calling_pattern_test_v2(input_data: str) -> dict:
    """Workflow that calls the same activity differently."""
    print(f"[CALLING V2] Calling with dict: {input_data}")
    # Call with dict argument instead
    data_dict = {"original": input_data, "version": "v2"}
    result = flexible_activity(data_dict)
    return {"version": "v2", "result": result}


# Test 3: True signature mismatch - remove parameters
@activity("changing_activity_v1")
def changing_activity_v1(param1: str, param2: str, param3: int = 42) -> dict:
    """Original activity with 3 parameters."""
    print(f"[CHANGING V1] param1={param1}, param2={param2}, param3={param3}")
    time.sleep(2)
    return {"v1_result": f"{param1}_{param2}_{param3}"}


@activity("changing_activity_v2")  # Same activity name!
def changing_activity_v2(param1: str) -> dict:  # Fewer parameters!
    """Modified activity with only 1 parameter - this should cause issues!"""
    print(f"[CHANGING V2] Only param1={param1}")
    time.sleep(2)
    return {"v2_result": f"simplified_{param1}"}


@workflow("true_signature_mismatch_test")
def true_signature_mismatch_test(input_data: str) -> dict:
    """Workflow that will have true signature mismatch issues."""
    print(f"[TRUE MISMATCH] Processing: {input_data}")

    # This will be stored in the database with 3 parameters
    # But on recovery, the activity might only accept 1 parameter
    result = changing_activity_v1(input_data, "second_param", 99)

    return {"mismatch_test": result}


def test_workflow_signature_changes():
    """Test what happens when workflow signatures change."""
    print("\n" + "=" * 70)
    print("TEST 1: Workflow Signature Changes")
    print("=" * 70)

    orchestrator = Orchestrator("test_workflow_signatures.db")
    orchestrator.start()

    try:
        # Run v1 workflow
        print("🚀 Running V1 workflow...")
        wf1_id = orchestrator.invoke_workflow(
            "workflow_signature_test_v1", input_data="test1"
        )

        # Wait for completion
        while True:
            status = orchestrator.get_workflow_status(wf1_id)
            if status["status"] in ["done", "failed"]:
                print(
                    f"V1 result: {status['status']} - {status.get('output', status.get('error_message'))}"
                )
                break
            time.sleep(0.5)

        # Now try v2 workflow with different signature
        print("\n🔄 Running V2 workflow with different signature...")
        wf2_id = orchestrator.invoke_workflow(
            "workflow_signature_test_v2", input_data="test2", extra_param="added_param"
        )

        # Wait for completion
        while True:
            status = orchestrator.get_workflow_status(wf2_id)
            if status["status"] in ["done", "failed"]:
                print(
                    f"V2 result: {status['status']} - {status.get('output', status.get('error_message'))}"
                )
                break
            time.sleep(0.5)

    finally:
        orchestrator.stop()


def test_calling_pattern_changes():
    """Test different ways of calling the same activity."""
    print("\n" + "=" * 70)
    print("TEST 2: Activity Calling Pattern Changes")
    print("=" * 70)

    orchestrator = Orchestrator("test_calling_patterns.db")
    orchestrator.start()

    try:
        # Test different calling patterns
        print("🚀 Testing different calling patterns...")

        wf1_id = orchestrator.invoke_workflow(
            "calling_pattern_test_v1", input_data="string_call"
        )
        wf2_id = orchestrator.invoke_workflow(
            "calling_pattern_test_v2", input_data="dict_call"
        )

        # Wait for both
        for wf_id, name in [(wf1_id, "V1 (string)"), (wf2_id, "V2 (dict)")]:
            while True:
                status = orchestrator.get_workflow_status(wf_id)
                if status["status"] in ["done", "failed"]:
                    print(
                        f"{name} result: {status['status']} - {status.get('output', status.get('error_message'))}"
                    )
                    break
                time.sleep(0.5)

    finally:
        orchestrator.stop()


def create_workflow_with_signature_mismatch():
    """Create a workflow that will have signature mismatch on recovery."""
    print("\n" + "=" * 70)
    print("TEST 3: Creating Workflow with Future Signature Mismatch")
    print("=" * 70)

    orchestrator = Orchestrator("test_true_mismatch.db")
    orchestrator.start()

    try:
        print("🚀 Starting workflow that will have signature issues on recovery...")
        workflow_id = orchestrator.invoke_workflow(
            "true_signature_mismatch_test", input_data="mismatch_test"
        )

        print(f"Started workflow: {workflow_id}")
        print("💡 This workflow calls changing_activity_v1 with 3 parameters")
        print(
            "   After this completes, we'll test recovery with changing_activity_v2 (1 parameter)"
        )

        # Wait for completion
        while True:
            status = orchestrator.get_workflow_status(workflow_id)
            print(
                f"Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
            )
            if status["status"] in ["done", "failed"]:
                if status["status"] == "done":
                    print(f"✅ Completed: {status['output']}")
                else:
                    print(f"❌ Failed: {status['error_message']}")
                break
            time.sleep(1)

    finally:
        orchestrator.stop()


def test_recovery_with_signature_mismatch():
    """Test recovery when activity signature truly changed."""
    print("\n" + "=" * 70)
    print("TEST 4: Recovery with TRUE Signature Mismatch")
    print("=" * 70)
    print("⚠️  WARNING: Redefining changing_activity to have fewer parameters!")

    # Here's the key: we redefine the activity with a different signature
    # The database contains a call with 3 parameters, but now the activity only accepts 1

    from py_orchestrate.decorators import get_registry

    registry = get_registry()

    # Remove the old activity definition
    if "changing_activity_v1" in registry.activities:
        del registry.activities["changing_activity_v1"]

    # Register the new one with fewer parameters
    @activity("changing_activity_v1")  # Same name, different signature!
    def changing_activity_v1_reduced(param1: str) -> dict:  # Only 1 parameter now!
        """Redefined activity with FEWER parameters - this should cause issues!"""
        print(f"[CHANGING V1 REDUCED] Only param1={param1}")
        print("⚠️  This activity now only accepts 1 parameter but was called with 3!")
        time.sleep(2)
        return {"v1_reduced_result": f"reduced_{param1}"}

    orchestrator = Orchestrator("test_true_mismatch.db")
    orchestrator.start()

    try:
        # Check existing workflows
        workflows = orchestrator.list_workflows("true_signature_mismatch_test")
        print(f"Found {len(workflows)} workflows to test recovery on")

        # Force a "recovery" by creating the same workflow again
        # This should trigger the signature mismatch
        print("\n🔄 Attempting to run workflow with changed activity signature...")
        workflow_id = orchestrator.invoke_workflow(
            "true_signature_mismatch_test", input_data="signature_test"
        )

        # Monitor for errors
        while True:
            status = orchestrator.get_workflow_status(workflow_id)
            print(
                f"Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
            )
            if status["status"] in ["done", "failed"]:
                if status["status"] == "done":
                    print(f"✅ Unexpected success: {status['output']}")
                    print("   (The activity somehow handled the signature mismatch)")
                else:
                    print(f"❌ Expected failure: {status['error_message']}")
                    print("   This error shows what happens with signature mismatches!")
                break
            time.sleep(1)

    finally:
        orchestrator.stop()


def cleanup():
    """Clean up test databases."""
    test_files = [
        "test_workflow_signatures.db",
        "test_calling_patterns.db",
        "test_true_mismatch.db",
    ]
    for file in test_files:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed {file}")


def main():
    if len(sys.argv) < 2:
        print("Signature Testing Commands:")
        print(
            "  python3 test_signature_issues.py workflows   - Test workflow signature changes"
        )
        print(
            "  python3 test_signature_issues.py patterns    - Test activity calling patterns"
        )
        print(
            "  python3 test_signature_issues.py create      - Create workflow for mismatch test"
        )
        print(
            "  python3 test_signature_issues.py mismatch    - Test true signature mismatch"
        )
        print("  python3 test_signature_issues.py all         - Run all tests")
        print("  python3 test_signature_issues.py cleanup     - Remove test databases")
        return

    command = sys.argv[1]

    if command == "workflows":
        test_workflow_signature_changes()
    elif command == "patterns":
        test_calling_pattern_changes()
    elif command == "create":
        create_workflow_with_signature_mismatch()
    elif command == "mismatch":
        test_recovery_with_signature_mismatch()
    elif command == "all":
        cleanup()
        test_workflow_signature_changes()
        test_calling_pattern_changes()
        create_workflow_with_signature_mismatch()
        print("\n" + "=" * 70)
        print("🎯 SIGNATURE ANALYSIS COMPLETE")
        print("=" * 70)
        print("Key findings:")
        print("1. Workflow signature changes work fine (different workflows)")
        print("2. Activity calling pattern changes work (flexible activities)")
        print("3. For true signature mismatches, run:")
        print("   python3 test_signature_issues.py mismatch")
    elif command == "cleanup":
        cleanup()
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
