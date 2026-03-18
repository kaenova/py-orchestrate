"""Database manager interfaces and shared helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from ..models import ActivityExecution, WorkflowInstance, WorkflowStatus


class BaseDatabaseManager(ABC):
    """Abstract database manager interface for workflow persistence."""

    @abstractmethod
    def save_workflow(self, workflow: WorkflowInstance) -> None:
        """Save or update a workflow instance."""

    @abstractmethod
    def get_workflow(self, workflow_id: str) -> Optional[WorkflowInstance]:
        """Get a workflow instance by ID."""

    @abstractmethod
    def get_workflows_by_name(self, name: str) -> list[WorkflowInstance]:
        """Get all workflow instances for a workflow name."""

    @abstractmethod
    def list_workflows(self) -> list[WorkflowInstance]:
        """List all workflow instances."""

    @abstractmethod
    def get_processing_workflows(self) -> list[WorkflowInstance]:
        """List all workflow instances that are still processing."""

    @abstractmethod
    def save_activity_execution(self, execution: ActivityExecution) -> None:
        """Save or update an activity execution."""

    @abstractmethod
    def get_activity_executions(self, workflow_id: str) -> list[ActivityExecution]:
        """Get all activity executions for a workflow."""


def workflow_to_document(workflow: WorkflowInstance) -> Dict[str, Any]:
    """Convert a workflow instance to a serializable document."""
    return {
        "id": workflow.id,
        "name": workflow.name,
        "status": workflow.status.value,
        "input_data": workflow.input_data,
        "output_data": workflow.output_data,
        "current_activity": workflow.current_activity,
        "error_message": workflow.error_message,
        "created_at": workflow.created_at.isoformat(),
        "updated_at": workflow.updated_at.isoformat(),
    }


def workflow_from_document(document: Dict[str, Any]) -> WorkflowInstance:
    """Convert a document to a workflow instance."""
    return WorkflowInstance(
        id=document["id"],
        name=document["name"],
        status=WorkflowStatus(document["status"]),
        input_data=document["input_data"],
        output_data=document.get("output_data"),
        current_activity=document.get("current_activity"),
        error_message=document.get("error_message"),
        created_at=datetime.fromisoformat(document["created_at"]),
        updated_at=datetime.fromisoformat(document["updated_at"]),
    )


def activity_to_document(execution: ActivityExecution) -> Dict[str, Any]:
    """Convert an activity execution to a serializable document."""
    return {
        "id": execution.id,
        "workflow_id": execution.workflow_id,
        "activity_name": execution.activity_name,
        "input_data": execution.input_data,
        "output_data": execution.output_data,
        "status": execution.status,
        "error_message": execution.error_message,
        "created_at": execution.created_at.isoformat(),
        "completed_at": (
            execution.completed_at.isoformat() if execution.completed_at else None
        ),
    }


def activity_from_document(document: Dict[str, Any]) -> ActivityExecution:
    """Convert a document to an activity execution."""
    return ActivityExecution(
        id=document["id"],
        workflow_id=document["workflow_id"],
        activity_name=document["activity_name"],
        input_data=document["input_data"],
        output_data=document.get("output_data"),
        status=document["status"],
        error_message=document.get("error_message"),
        created_at=datetime.fromisoformat(document["created_at"]),
        completed_at=(
            datetime.fromisoformat(document["completed_at"])
            if document.get("completed_at")
            else None
        ),
    )


def workflows_from_documents(
    documents: Iterable[Dict[str, Any]],
) -> list[WorkflowInstance]:
    """Convert an iterable of workflow documents."""
    return [workflow_from_document(document) for document in documents]


def activities_from_documents(
    documents: Iterable[Dict[str, Any]],
) -> list[ActivityExecution]:
    """Convert an iterable of activity execution documents."""
    return [activity_from_document(document) for document in documents]
