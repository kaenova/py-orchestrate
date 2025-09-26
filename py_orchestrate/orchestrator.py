"""
Main orchestrator engine for py-orchestrate.
"""

import uuid
import threading
import time
import traceback
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, Future
import inspect

from .models import (
    DatabaseManager, WorkflowInstance, ActivityExecution, 
    WorkflowStatus
)
from .decorators import get_registry


class ActivityContext:
    """Context for activity execution within a workflow."""
    
    def __init__(self, orchestrator: 'Orchestrator', workflow_id: str):
        self.orchestrator = orchestrator
        self.workflow_id = workflow_id
        self.completed_activities = []  # For recovery
        self.activity_results = {}  # For recovery


class Orchestrator:
    """Main orchestrator engine for managing workflows and activities."""
    
    def __init__(self, db_path: str = "py_orchestrate.db", max_workers: int = 5):
        self.db = DatabaseManager(db_path)
        self.registry = get_registry()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running_workflows: Dict[str, Future] = {}
        self._running = False
        self._recovery_thread = None
    
    def start(self) -> None:
        """Start the orchestrator engine."""
        if self._running:
            return
        
        self._running = True
        
        # Start recovery thread to resume interrupted workflows
        self._recovery_thread = threading.Thread(target=self._recovery_loop, daemon=True)
        self._recovery_thread.start()
        
        print("Orchestrator started")
    
    def stop(self) -> None:
        """Stop the orchestrator engine."""
        self._running = False
        
        # Wait for running workflows to complete
        running_futures = list(self.running_workflows.values())
        for future in running_futures:
            try:
                future.result(timeout=5)  # Give 5 seconds for graceful shutdown
            except Exception:
                pass
        
        self.executor.shutdown(wait=True)
        print("Orchestrator stopped")
    
    def invoke_workflow(self, name: str, **kwargs) -> str:
        """
        Invoke a workflow by name with input parameters.
        
        Args:
            name: Name of the workflow to invoke
            **kwargs: Input parameters for the workflow
        
        Returns:
            Workflow ID for tracking the execution
        """
        if name not in self.registry.workflows:
            raise ValueError(f"Workflow '{name}' not found")
        
        workflow_id = str(uuid.uuid4())
        now = datetime.now()
        
        # Create workflow instance
        workflow_instance = WorkflowInstance(
            id=workflow_id,
            name=name,
            status=WorkflowStatus.PROCESSING,
            input_data=kwargs,
            output_data=None,
            current_activity=None,
            error_message=None,
            created_at=now,
            updated_at=now
        )
        
        # Save to database
        self.db.save_workflow(workflow_instance)
        
        # Submit workflow for execution
        future = self.executor.submit(self._execute_workflow, workflow_id)
        self.running_workflows[workflow_id] = future
        
        return workflow_id
    
    def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a workflow.
        
        Args:
            workflow_id: ID of the workflow to query
        
        Returns:
            Dictionary containing workflow status information
        """
        workflow = self.db.get_workflow(workflow_id)
        if not workflow:
            return None
        
        return {
            "id": workflow.id,
            "name": workflow.name,
            "status": workflow.status.value,
            "current_activity": workflow.current_activity,
            "error_message": workflow.error_message,
            "output": workflow.output_data,
            "created_at": workflow.created_at.isoformat(),
            "updated_at": workflow.updated_at.isoformat()
        }
    
    def list_workflows(self, name: str = None) -> List[Dict[str, Any]]:
        """
        List workflows, optionally filtered by name.
        
        Args:
            name: Optional workflow name to filter by
        
        Returns:
            List of workflow status dictionaries
        """
        if name:
            workflows = self.db.get_workflows_by_name(name)
        else:
            # For now, we'll implement a simple query for all workflows
            # In a real implementation, you might want pagination
            workflows = []
        
        return [
            {
                "id": w.id,
                "name": w.name,
                "status": w.status.value,
                "current_activity": w.current_activity,
                "error_message": w.error_message,
                "output": w.output_data,
                "created_at": w.created_at.isoformat(),
                "updated_at": w.updated_at.isoformat()
            }
            for w in workflows
        ]
    
    def _execute_workflow(self, workflow_id: str) -> None:
        """Execute a workflow in a separate thread."""
        try:
            workflow_instance = self.db.get_workflow(workflow_id)
            if not workflow_instance:
                return
            
            workflow_func = self.registry.get_workflow(workflow_instance.name)
            
            # Create activity context
            context = ActivityContext(self, workflow_id)
            
            # Execute workflow with instrumented activities
            result = self._run_workflow_with_instrumentation(
                workflow_func, workflow_instance.input_data, context
            )
            
            # Update workflow as completed
            workflow_instance.status = WorkflowStatus.DONE
            workflow_instance.output_data = result
            workflow_instance.current_activity = None
            workflow_instance.updated_at = datetime.now()
            
            self.db.save_workflow(workflow_instance)
            
        except Exception as e:
            # Handle workflow failure
            workflow_instance = self.db.get_workflow(workflow_id)
            if workflow_instance:
                workflow_instance.status = WorkflowStatus.FAILED
                workflow_instance.error_message = str(e)
                workflow_instance.updated_at = datetime.now()
                self.db.save_workflow(workflow_instance)
        
        finally:
            # Remove from running workflows
            if workflow_id in self.running_workflows:
                del self.running_workflows[workflow_id]
    
    def _run_workflow_with_instrumentation(
        self, workflow_func, input_data: Dict[str, Any], context: ActivityContext
    ) -> Any:
        """Run workflow with activity instrumentation."""
        
        # Get original activities and replace with instrumented versions
        original_activities = {}
        for activity_name, activity_func in self.registry.activities.items():
            original_activities[activity_name] = activity_func
            
            # Create instrumented activity
            instrumented = self._create_instrumented_activity(
                activity_name, activity_func, context
            )
            
            # Replace in globals if the activity is used in the workflow
            workflow_globals = workflow_func.__globals__
            if activity_name in workflow_globals:
                workflow_globals[activity_name] = instrumented
        
        try:
            # Execute the workflow
            result = workflow_func(**input_data)
            return result
        except Exception as e:
            # Log the error and re-raise
            print(f"Error in workflow execution: {e}")
            raise
        finally:
            # Restore original activities
            workflow_globals = workflow_func.__globals__
            for activity_name, original_func in original_activities.items():
                if activity_name in workflow_globals:
                    workflow_globals[activity_name] = original_func
    
    def _run_workflow_with_recovery(
        self, workflow_func, input_data: Dict[str, Any], context: ActivityContext
    ) -> Any:
        """Run workflow with recovery-aware activity instrumentation."""
        
        # Get original activities and replace with recovery-aware instrumented versions
        original_activities = {}
        for activity_name, activity_func in self.registry.activities.items():
            original_activities[activity_name] = activity_func
            
            # Create recovery-aware instrumented activity
            instrumented = self._create_recovery_aware_activity(
                activity_name, activity_func, context
            )
            
            # Replace in globals if the activity is used in the workflow
            workflow_globals = workflow_func.__globals__
            if activity_name in workflow_globals:
                workflow_globals[activity_name] = instrumented
        
        try:
            # Execute the workflow
            result = workflow_func(**input_data)
            return result
        except Exception as e:
            # Log the error and re-raise
            print(f"Error in recovery workflow execution: {e}")
            raise
        finally:
            # Restore original activities
            workflow_globals = workflow_func.__globals__
            for activity_name, original_func in original_activities.items():
                if activity_name in workflow_globals:
                    workflow_globals[activity_name] = original_func
    
    def _create_instrumented_activity(
        self, activity_name: str, activity_func, context: ActivityContext
    ):
        """Create an instrumented version of an activity function."""
        
        def instrumented_activity(*args, **kwargs):
            # Update workflow current activity
            workflow_instance = self.db.get_workflow(context.workflow_id)
            if workflow_instance:
                workflow_instance.current_activity = activity_name
                workflow_instance.updated_at = datetime.now()
                self.db.save_workflow(workflow_instance)
            
            # Create activity execution record
            execution_id = str(uuid.uuid4())
            now = datetime.now()
            
            # Convert args to dict for storage
            sig = inspect.signature(activity_func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            input_data = dict(bound_args.arguments)
            
            execution = ActivityExecution(
                id=execution_id,
                workflow_id=context.workflow_id,
                activity_name=activity_name,
                input_data=input_data,
                output_data=None,
                status="running",
                error_message=None,
                created_at=now,
                completed_at=None
            )
            
            self.db.save_activity_execution(execution)
            
            try:
                # Execute the activity
                result = activity_func(*args, **kwargs)
                
                # Update execution as completed
                execution.output_data = result
                execution.status = "completed"
                execution.completed_at = datetime.now()
                self.db.save_activity_execution(execution)
                
                return result
                
            except Exception as e:
                # Update execution as failed
                execution.status = "failed"
                execution.error_message = str(e)
                execution.completed_at = datetime.now()
                self.db.save_activity_execution(execution)
                raise
        
        return instrumented_activity
    
    def _create_recovery_aware_activity(
        self, activity_name: str, activity_func, context: ActivityContext
    ):
        """Create a recovery-aware instrumented version of an activity function."""
        
        def recovery_aware_activity(*args, **kwargs):
            # Check if this activity was already completed
            if activity_name in context.completed_activities:
                print(f"Activity {activity_name} already completed, returning cached result")
                return context.activity_results.get(activity_name)
            
            # Update workflow current activity
            workflow_instance = self.db.get_workflow(context.workflow_id)
            if workflow_instance:
                workflow_instance.current_activity = activity_name
                workflow_instance.updated_at = datetime.now()
                self.db.save_workflow(workflow_instance)
            
            # Create activity execution record
            execution_id = str(uuid.uuid4())
            now = datetime.now()
            
            # Convert args to dict for storage
            sig = inspect.signature(activity_func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            input_data = dict(bound_args.arguments)
            
            execution = ActivityExecution(
                id=execution_id,
                workflow_id=context.workflow_id,
                activity_name=activity_name,
                input_data=input_data,
                output_data=None,
                status="running",
                error_message=None,
                created_at=now,
                completed_at=None
            )
            
            self.db.save_activity_execution(execution)
            
            try:
                # Execute the activity
                result = activity_func(*args, **kwargs)
                
                # Update execution as completed
                execution.output_data = result
                execution.status = "completed"
                execution.completed_at = datetime.now()
                self.db.save_activity_execution(execution)
                
                # Store result for potential recovery
                context.activity_results[activity_name] = result
                context.completed_activities.append(activity_name)
                
                return result
                
            except Exception as e:
                # Update execution as failed
                execution.status = "failed"
                execution.error_message = str(e)
                execution.completed_at = datetime.now()
                self.db.save_activity_execution(execution)
                raise
        
        return recovery_aware_activity
    
    def _recovery_loop(self) -> None:
        """Recovery loop to resume interrupted workflows."""
        while self._running:
            try:
                self._recover_interrupted_workflows()
            except Exception as e:
                print(f"Error in recovery loop: {e}")
            
            time.sleep(5)  # Check every 5 seconds
    
    def _recover_interrupted_workflows(self) -> None:
        """Find and resume interrupted workflows."""
        # Get all processing workflows from database
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.execute("""
                SELECT id, name FROM workflows 
                WHERE status = 'processing'
            """)
            
            processing_workflows = cursor.fetchall()
        
        for workflow_id, workflow_name in processing_workflows:
            # Check if this workflow is already running
            if workflow_id in self.running_workflows:
                continue
            
            # Check if workflow function still exists
            if workflow_name not in self.registry.workflows:
                print(f"Warning: Workflow '{workflow_name}' not found in registry, marking as failed")
                self._mark_workflow_failed(workflow_id, f"Workflow '{workflow_name}' not registered")
                continue
            
            print(f"Recovering interrupted workflow: {workflow_id} ({workflow_name})")
            
            # Resume the workflow
            future = self.executor.submit(self._resume_workflow, workflow_id)
            self.running_workflows[workflow_id] = future
    
    def _resume_workflow(self, workflow_id: str) -> None:
        """Resume a workflow from where it left off."""
        try:
            workflow_instance = self.db.get_workflow(workflow_id)
            if not workflow_instance:
                return
            
            workflow_func = self.registry.get_workflow(workflow_instance.name)
            
            # Get completed activities for this workflow
            completed_activities = self.db.get_activity_executions(workflow_id)
            completed_activity_names = [
                act.activity_name for act in completed_activities 
                if act.status == "completed"
            ]
            
            print(f"Resuming workflow {workflow_id}, completed activities: {completed_activity_names}")
            
            # Create activity context with recovery info
            context = ActivityContext(self, workflow_id)
            context.completed_activities = completed_activity_names
            context.activity_results = {
                act.activity_name: act.output_data 
                for act in completed_activities 
                if act.status == "completed" and act.output_data
            }
            
            # Execute workflow with recovery-aware instrumentation
            result = self._run_workflow_with_recovery(
                workflow_func, workflow_instance.input_data, context
            )
            
            # Update workflow as completed
            workflow_instance.status = WorkflowStatus.DONE
            workflow_instance.output_data = result
            workflow_instance.current_activity = None
            workflow_instance.updated_at = datetime.now()
            
            self.db.save_workflow(workflow_instance)
            print(f"Workflow {workflow_id} resumed and completed successfully")
            
        except Exception as e:
            # Handle workflow failure
            print(f"Error resuming workflow {workflow_id}: {e}")
            self._mark_workflow_failed(workflow_id, str(e))
        
        finally:
            # Remove from running workflows
            if workflow_id in self.running_workflows:
                del self.running_workflows[workflow_id]
    
    def _mark_workflow_failed(self, workflow_id: str, error_message: str) -> None:
        """Mark a workflow as failed."""
        workflow_instance = self.db.get_workflow(workflow_id)
        if workflow_instance:
            workflow_instance.status = WorkflowStatus.FAILED
            workflow_instance.error_message = error_message
            workflow_instance.current_activity = None
            workflow_instance.updated_at = datetime.now()
            self.db.save_workflow(workflow_instance)