"""
Database models for py-orchestrate.
"""

import sqlite3
import json
import uuid
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


class DatabaseManager:
    """Manages the SQLite database for workflow persistence."""

    def __init__(self, db_path: str = "py_orchestrate.db"):
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workflows (
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
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS activity_executions (
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
            """
            )

            conn.commit()

    def save_workflow(self, workflow: WorkflowInstance) -> None:
        """Save or update a workflow instance."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO workflows 
                (id, name, status, input_data, output_data, current_activity, 
                 error_message, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    workflow.id,
                    workflow.name,
                    workflow.status.value,
                    json.dumps(workflow.input_data),
                    json.dumps(workflow.output_data) if workflow.output_data else None,
                    workflow.current_activity,
                    workflow.error_message,
                    workflow.created_at.isoformat(),
                    workflow.updated_at.isoformat(),
                ),
            )
            conn.commit()

    def get_workflow(self, workflow_id: str) -> Optional[WorkflowInstance]:
        """Get a workflow instance by ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT id, name, status, input_data, output_data, current_activity,
                       error_message, created_at, updated_at
                FROM workflows WHERE id = ?
            """,
                (workflow_id,),
            )

            row = cursor.fetchone()
            if not row:
                return None

            return WorkflowInstance(
                id=row[0],
                name=row[1],
                status=WorkflowStatus(row[2]),
                input_data=json.loads(row[3]),
                output_data=json.loads(row[4]) if row[4] else None,
                current_activity=row[5],
                error_message=row[6],
                created_at=datetime.fromisoformat(row[7]),
                updated_at=datetime.fromisoformat(row[8]),
            )

    def get_workflows_by_name(self, name: str) -> List[WorkflowInstance]:
        """Get all workflow instances by name."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT id, name, status, input_data, output_data, current_activity,
                       error_message, created_at, updated_at
                FROM workflows WHERE name = ?
                ORDER BY created_at DESC
            """,
                (name,),
            )

            workflows = []
            for row in cursor.fetchall():
                workflows.append(
                    WorkflowInstance(
                        id=row[0],
                        name=row[1],
                        status=WorkflowStatus(row[2]),
                        input_data=json.loads(row[3]),
                        output_data=json.loads(row[4]) if row[4] else None,
                        current_activity=row[5],
                        error_message=row[6],
                        created_at=datetime.fromisoformat(row[7]),
                        updated_at=datetime.fromisoformat(row[8]),
                    )
                )

            return workflows

    def save_activity_execution(self, execution: ActivityExecution) -> None:
        """Save an activity execution."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO activity_executions
                (id, workflow_id, activity_name, input_data, output_data,
                 status, error_message, created_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    execution.id,
                    execution.workflow_id,
                    execution.activity_name,
                    json.dumps(execution.input_data),
                    (
                        json.dumps(execution.output_data)
                        if execution.output_data
                        else None
                    ),
                    execution.status,
                    execution.error_message,
                    execution.created_at.isoformat(),
                    (
                        execution.completed_at.isoformat()
                        if execution.completed_at
                        else None
                    ),
                ),
            )
            conn.commit()

    def get_activity_executions(self, workflow_id: str) -> List[ActivityExecution]:
        """Get all activity executions for a workflow."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT id, workflow_id, activity_name, input_data, output_data,
                       status, error_message, created_at, completed_at
                FROM activity_executions WHERE workflow_id = ?
                ORDER BY created_at ASC
            """,
                (workflow_id,),
            )

            executions = []
            for row in cursor.fetchall():
                executions.append(
                    ActivityExecution(
                        id=row[0],
                        workflow_id=row[1],
                        activity_name=row[2],
                        input_data=json.loads(row[3]),
                        output_data=json.loads(row[4]) if row[4] else None,
                        status=row[5],
                        error_message=row[6],
                        created_at=datetime.fromisoformat(row[7]),
                        completed_at=datetime.fromisoformat(row[8]) if row[8] else None,
                    )
                )

            return executions
