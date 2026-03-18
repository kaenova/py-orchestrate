"""
Direct test of signature mismatch during recovery.
This simulates what happens when activity signatures change between runs.
"""

import time
import sys
import os
import sqlite3
import json
from datetime import datetime
from py_orchestrate import workflow, activity, Orchestrator


def create_interrupted_workflow_manually():
    """Manually create an interrupted workflow in the database to test recovery."""
    print("🔧 Creating interrupted workflow manually in database...")

    db_path = "test_signature_mismatch.db"

    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)

    # Create database and tables
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE workflows (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                input_data TEXT NOT NULL,
                output_data TEXT,
                current_activity TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE activity_executions (
                id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                activity_name TEXT NOT NULL,
                input_data TEXT NOT NULL,
                output_data TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                FOREIGN KEY (workflow_id) REFERENCES workflows (id)
            )
        """)

        # Insert interrupted workflow
        workflow_id = "test-signature-mismatch-workflow"
        now = datetime.now().isoformat()

        conn.execute(
            """
            INSERT INTO workflows 
            (id, name, status, input_data, output_data, current_activity, error_message, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                workflow_id,
                "signature_mismatch_test",
                "processing",
                json.dumps({"input": "test_data"}),
                None,
                "problematic_activity",
                None,
                now,
                now,
            ),
        )

        # Insert completed activity
        conn.execute(
            """
            INSERT INTO activity_executions
            (id, workflow_id, activity_name, input_data, output_data, status, error_message, created_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                "completed-activity-1",
                workflow_id,
                "step_one",
                json.dumps({"input": "test_data"}),
                json.dumps({"result": "step_one_completed"}),
                "completed",
                None,
                now,
                now,
            ),
        )

        # Insert running activity (simulates interruption during this activity)
        conn.execute(
            """
            INSERT INTO activity_executions
            (id, workflow_id, activity_name, input_data, output_data, status, error_message, created_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                "interrupted-activity-2",
                workflow_id,
                "problematic_activity",
                json.dumps({"step_one_result": {"result": "step_one_completed"}}),
                None,
                "running",
                None,
                now,
                None,
            ),
        )

        conn.commit()

    print(f"✅ Created interrupted workflow: {workflow_id}")
    print("   - step_one: completed")
    print("   - problematic_activity: interrupted (running)")
    return workflow_id


# Original activity that was running when interrupted
@activity("problematic_activity")
def problematic_activity_original(step_one_result: dict) -> dict:
    """Original version that expects dict parameter."""
    print(f"[ORIGINAL] Processing with dict: {step_one_result}")
    time.sleep(2)
    return {"original_result": step_one_result["result"] + "_original"}


# Define activities for the workflow
@activity("step_one")
def step_one(input: str) -> dict:
    """First step that completes successfully."""
    print(f"[STEP1] Processing: {input}")
    time.sleep(1)
    return {"result": "step_one_completed"}


@workflow("signature_mismatch_test")
def signature_mismatch_test(input: str) -> dict:
    """Test workflow."""
    print(f"[WORKFLOW] Starting with: {input}")

    # Step 1 (already completed)
    step1_result = step_one(input)

    # Step 2 (this will have signature issues)
    step2_result = problematic_activity_original(step1_result)

    return {"final": step2_result}


def test_recovery_with_original_signature():
    """Test recovery with original signature - should work."""
    print("\n" + "=" * 70)
    print("TEST 1: Recovery with ORIGINAL signature")
    print("=" * 70)

    orchestrator = Orchestrator("test_signature_mismatch.db")
    orchestrator.start()

    try:
        print("🔄 Waiting for recovery with original signature...")
        time.sleep(8)

        # Check workflow status
        workflows = orchestrator.list_workflows("signature_mismatch_test")
        for wf in workflows:
            status = orchestrator.get_workflow_status(wf["id"])
            print(f"Workflow {wf['id']}: {status['status']}")
            if status["status"] == "failed":
                print(f"❌ Error: {status['error_message']}")
            elif status["status"] == "done":
                print(f"✅ Success: {status['output']}")
            else:
                print(f"🔄 Still processing: {status.get('current_activity')}")

    finally:
        orchestrator.stop()


def test_recovery_with_changed_signature():
    """Test recovery with CHANGED signature - should fail."""
    print("\n" + "=" * 70)
    print("TEST 2: Recovery with CHANGED signature")
    print("=" * 70)

    # REDEFINE the activity with DIFFERENT signature
    @activity("problematic_activity")
    def problematic_activity_new(
        step_one_result: dict, new_param: str = "default"
    ) -> dict:
        """NEW version with additional parameter."""
        print(
            f"[NEW] Processing with dict: {step_one_result} and new_param: {new_param}"
        )
        time.sleep(2)
        return {
            "new_result": step_one_result["result"] + "_new",
            "new_param": new_param,
        }

    orchestrator = Orchestrator("test_signature_mismatch.db")
    orchestrator.start()

    try:
        print("🔄 Waiting for recovery with CHANGED signature...")
        print("   (This should cause issues because the signature changed)")
        time.sleep(8)

        # Check workflow status
        workflows = orchestrator.list_workflows("signature_mismatch_test")
        for wf in workflows:
            status = orchestrator.get_workflow_status(wf["id"])
            print(f"Workflow {wf['id']}: {status['status']}")
            if status["status"] == "failed":
                print(f"❌ Error (expected): {status['error_message']}")
                print("\n🔍 ANALYSIS: This error occurred because:")
                print("   1. The activity was interrupted while running")
                print("   2. On recovery, the activity signature changed")
                print("   3. The orchestrator tried to call the new signature")
                print("   4. But the stored parameters don't match the new signature")
            elif status["status"] == "done":
                print(f"✅ Unexpected success: {status['output']}")
            else:
                print(f"🔄 Still processing: {status.get('current_activity')}")

    finally:
        orchestrator.stop()


def test_signature_type_mismatch():
    """Test what happens when parameter types change."""
    print("\n" + "=" * 70)
    print("TEST 3: Recovery with TYPE MISMATCH")
    print("=" * 70)

    # Create fresh interrupted workflow
    create_interrupted_workflow_manually()

    # REDEFINE activity with DIFFERENT PARAMETER TYPE
    @activity("problematic_activity")
    def problematic_activity_type_changed(
        step_one_result: str,
    ) -> dict:  # Changed dict -> str
        """Version that expects STRING instead of DICT."""
        print(f"[TYPE_CHANGED] Processing string: {step_one_result}")
        time.sleep(2)
        return {"type_changed_result": step_one_result + "_type_changed"}

    orchestrator = Orchestrator("test_signature_mismatch.db")
    orchestrator.start()

    try:
        print("🔄 Waiting for recovery with TYPE MISMATCH...")
        print("   (Activity now expects string but stored data is dict)")
        time.sleep(8)

        # Check workflow status
        workflows = orchestrator.list_workflows("signature_mismatch_test")
        for wf in workflows:
            status = orchestrator.get_workflow_status(wf["id"])
            print(f"Workflow {wf['id']}: {status['status']}")
            if status["status"] == "failed":
                print(f"❌ Type mismatch error: {status['error_message']}")
                print("\n🔍 ANALYSIS: This shows what happens when:")
                print("   1. An activity expects a dict parameter")
                print("   2. The signature changes to expect a string")
                print(
                    "   3. Recovery tries to pass the stored dict to the new string parameter"
                )
            elif status["status"] == "done":
                print(f"✅ Unexpected success: {status['output']}")

    finally:
        orchestrator.stop()


def cleanup():
    """Clean up test database."""
    if os.path.exists("test_signature_mismatch.db"):
        os.remove("test_signature_mismatch.db")
        print("Cleaned up test_signature_mismatch.db")


def main():
    if len(sys.argv) < 2:
        print("Signature Mismatch Test Commands:")
        print(
            "  python3 test_signature_mismatch.py setup      - Create interrupted workflow"
        )
        print(
            "  python3 test_signature_mismatch.py original   - Test with original signature"
        )
        print(
            "  python3 test_signature_mismatch.py changed    - Test with changed signature"
        )
        print(
            "  python3 test_signature_mismatch.py types      - Test with type mismatch"
        )
        print("  python3 test_signature_mismatch.py all        - Run all tests")
        print("  python3 test_signature_mismatch.py cleanup    - Remove test database")
        return

    command = sys.argv[1]

    if command == "setup":
        create_interrupted_workflow_manually()
    elif command == "original":
        test_recovery_with_original_signature()
    elif command == "changed":
        test_recovery_with_changed_signature()
    elif command == "types":
        test_signature_type_mismatch()
    elif command == "all":
        print("🧪 Running complete signature mismatch test suite...")
        cleanup()

        # Test 1: Original signature (should work)
        create_interrupted_workflow_manually()
        test_recovery_with_original_signature()

        # Test 2: Changed signature (may cause issues)
        create_interrupted_workflow_manually()
        test_recovery_with_changed_signature()

        # Test 3: Type mismatch (should definitely cause issues)
        test_signature_type_mismatch()

        print("\n" + "=" * 70)
        print("🎯 SIGNATURE CHANGE TEST RESULTS SUMMARY")
        print("=" * 70)
        print("These tests reveal how py-orchestrate handles signature changes:")
        print("1. If signatures don't change - recovery works perfectly")
        print("2. If signatures change - depends on the specific change")
        print("3. Type mismatches are the most problematic")
        print("\n💡 Best practices:")
        print("- Keep activity signatures stable in production")
        print("- Use optional parameters for extensions")
        print("- Version your activities if you must change signatures")

    elif command == "cleanup":
        cleanup()
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
