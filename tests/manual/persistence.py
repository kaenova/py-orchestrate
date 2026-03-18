"""
Test script to demonstrate workflow persistence across application restarts.
"""

import time
import sys
from py_orchestrate import workflow, activity, Orchestrator


@activity("long_running_task")
def long_running_task(duration: int) -> dict:
    """Simulate a long-running task."""
    print(f"Starting long task for {duration} seconds...")
    time.sleep(duration)
    print("Long task completed!")
    return {"completed": True, "duration": duration}


@activity("final_task")
def final_task(result: dict) -> dict:
    """Final task in the workflow."""
    print(f"Final task with result: {result}")
    return {"final_result": True, "previous": result}


@workflow("persistent_workflow")
def persistent_workflow(task_duration: int) -> dict:
    """A workflow that can be interrupted and resumed."""
    print("Starting persistent workflow...")

    # This task might be interrupted
    long_result = long_running_task(task_duration)

    # This task should complete after restart
    final_result = final_task(long_result)

    return {"workflow_done": True, "result": final_result}


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "start":
        # Start a long-running workflow
        print("=== Starting Long-Running Workflow ===")
        orchestrator = Orchestrator("test_persistence.db")
        orchestrator.start()

        try:
            workflow_id = orchestrator.invoke_workflow(
                "persistent_workflow",
                task_duration=10,  # 10 second task
            )
            print(f"Started workflow {workflow_id}")
            print(
                "Workflow is running... you can interrupt this and restart to see persistence"
            )

            # Monitor for a bit
            for i in range(15):
                status = orchestrator.get_workflow_status(workflow_id)
                print(
                    f"Status: {status['status']}, Activity: {status.get('current_activity', 'None')}"
                )
                if status["status"] in ["done", "failed"]:
                    break
                time.sleep(1)

        finally:
            orchestrator.stop()

    else:
        # Check for existing workflows and resume
        print("=== Checking for Existing Workflows ===")
        orchestrator = Orchestrator("test_persistence.db")
        orchestrator.start()

        try:
            # List all persistent workflows
            workflows = orchestrator.list_workflows("persistent_workflow")

            if not workflows:
                print("No existing workflows found.")
                print("Run: python3 test_persistence.py start")
            else:
                print(f"Found {len(workflows)} workflows:")
                for wf in workflows:
                    print(f"  {wf['id']}: {wf['status']} (created: {wf['created_at']})")
                    if wf["status"] == "processing":
                        print(
                            f"    Current activity: {wf.get('current_activity', 'None')}"
                        )

        finally:
            orchestrator.stop()


if __name__ == "__main__":
    main()
