"""SQLite database manager implementation."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from .base import (
    BaseDatabaseManager,
    activity_from_document,
    activity_to_document,
    workflow_from_document,
    workflow_to_document,
)
from ..models import ActivityExecution, WorkflowInstance


class SQLiteDatabaseManager(BaseDatabaseManager):
    """SQLite-backed workflow persistence."""

    def __init__(self, db_path: str = "py_orchestrate.db"):
        self.db_path = db_path
        self._init_database()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_database(self) -> None:
        with self._connect() as conn:
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
        document = workflow_to_document(workflow)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO workflows
                (id, name, status, input_data, output_data, current_activity,
                 error_message, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    document["id"],
                    document["name"],
                    document["status"],
                    json.dumps(document["input_data"]),
                    (
                        json.dumps(document["output_data"])
                        if document["output_data"] is not None
                        else None
                    ),
                    document["current_activity"],
                    document["error_message"],
                    document["created_at"],
                    document["updated_at"],
                ),
            )
            conn.commit()

    def get_workflow(self, workflow_id: str) -> Optional[WorkflowInstance]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT id, name, status, input_data, output_data, current_activity,
                       error_message, created_at, updated_at
                FROM workflows WHERE id = ?
                """,
                (workflow_id,),
            )
            row = cursor.fetchone()

        return self._workflow_from_row(row)

    def get_workflows_by_name(self, name: str) -> list[WorkflowInstance]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT id, name, status, input_data, output_data, current_activity,
                       error_message, created_at, updated_at
                FROM workflows WHERE name = ?
                ORDER BY created_at DESC
                """,
                (name,),
            )
            rows = cursor.fetchall()

        return self._workflows_from_rows(rows)

    def list_workflows(self) -> list[WorkflowInstance]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT id, name, status, input_data, output_data, current_activity,
                       error_message, created_at, updated_at
                FROM workflows
                ORDER BY created_at DESC
                """
            )
            rows = cursor.fetchall()

        return self._workflows_from_rows(rows)

    def get_processing_workflows(self) -> list[WorkflowInstance]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT id, name, status, input_data, output_data, current_activity,
                       error_message, created_at, updated_at
                FROM workflows
                WHERE status = 'processing'
                ORDER BY created_at ASC
                """
            )
            rows = cursor.fetchall()

        return self._workflows_from_rows(rows)

    def save_activity_execution(self, execution: ActivityExecution) -> None:
        document = activity_to_document(execution)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO activity_executions
                (id, workflow_id, activity_name, input_data, output_data,
                 status, error_message, created_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    document["id"],
                    document["workflow_id"],
                    document["activity_name"],
                    json.dumps(document["input_data"]),
                    (
                        json.dumps(document["output_data"])
                        if document["output_data"] is not None
                        else None
                    ),
                    document["status"],
                    document["error_message"],
                    document["created_at"],
                    document["completed_at"],
                ),
            )
            conn.commit()

    def get_activity_executions(self, workflow_id: str) -> list[ActivityExecution]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT id, workflow_id, activity_name, input_data, output_data,
                       status, error_message, created_at, completed_at
                FROM activity_executions WHERE workflow_id = ?
                ORDER BY created_at ASC
                """,
                (workflow_id,),
            )
            rows = cursor.fetchall()

        return [self._activity_from_row(row) for row in rows]

    def _workflow_from_row(
        self, row: Optional[sqlite3.Row]
    ) -> Optional[WorkflowInstance]:
        if row is None:
            return None

        return workflow_from_document(self._deserialize_row(row))

    def _activity_from_row(self, row: sqlite3.Row) -> ActivityExecution:
        return activity_from_document(self._deserialize_row(row))

    def _workflows_from_rows(self, rows: list[sqlite3.Row]) -> list[WorkflowInstance]:
        workflows: list[WorkflowInstance] = []
        for row in rows:
            workflow = self._workflow_from_row(row)
            if workflow is not None:
                workflows.append(workflow)
        return workflows

    def _deserialize_row(self, row: sqlite3.Row) -> dict[str, Any]:
        document = dict(row)
        document["input_data"] = json.loads(document["input_data"])
        if document.get("output_data") is not None:
            document["output_data"] = json.loads(document["output_data"])
        return document


DatabaseManager = SQLiteDatabaseManager
