"""Database models for py-orchestrate."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, List
from dataclasses import dataclass


class WorkflowStatus(Enum):
    """Workflow status enumeration."""

    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


@dataclass
class WorkflowInstance:
    """Represents a workflow instance."""

    id: str
    name: str
    status: WorkflowStatus
    input_data: Dict[str, Any]
    output_data: Optional[Dict[str, Any]]
    current_activity: Optional[str]
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime


@dataclass
class ActivityExecution:
    """Represents an activity execution."""

    id: str
    workflow_id: str
    activity_name: str
    input_data: Dict[str, Any]
    output_data: Optional[Dict[str, Any]]
    status: str
    error_message: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]


def __getattr__(name: str):
    """Provide a lazy compatibility alias for the SQLite database manager."""
    if name == "DatabaseManager":
        from .db_manager.sqlite import DatabaseManager

        return DatabaseManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
