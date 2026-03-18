"""Azure Cosmos DB database manager implementation."""

from __future__ import annotations

from importlib import import_module
from typing import Any, Optional

from .base import (
    BaseDatabaseManager,
    activities_from_documents,
    activity_to_document,
    workflow_from_document,
    workflow_to_document,
    workflows_from_documents,
)
from ..models import ActivityExecution, WorkflowInstance


def _load_cosmos_classes() -> tuple[Any, Any]:
    """Load Cosmos SDK classes lazily so the dependency can stay optional."""
    try:
        cosmos_module = import_module("azure.cosmos")
    except ImportError as exc:
        raise ImportError(
            "Cosmos DB support requires the optional dependency 'azure-cosmos'. "
            "Install it with 'pip install py-orchestrate[cosmos]'."
        ) from exc

    return cosmos_module.CosmosClient, cosmos_module.PartitionKey


class CosmosDatabaseManager(BaseDatabaseManager):
    """Azure Cosmos DB-backed workflow persistence."""

    def __init__(
        self,
        database_id: str,
        workflow_container_id: str,
        activity_container_id: str,
        connection_string: Optional[str] = None,
        endpoint: Optional[str] = None,
        credential: Any = None,
    ):
        cosmos_client_cls, partition_key_cls = _load_cosmos_classes()

        if not connection_string and not endpoint:
            raise ValueError("Provide either connection_string or endpoint.")

        if endpoint and credential is None:
            raise ValueError(
                "A credential is required when using endpoint authentication."
            )

        if connection_string:
            self.client = cosmos_client_cls.from_connection_string(connection_string)
        else:
            self.client = cosmos_client_cls(endpoint, credential=credential)

        self.database_id = database_id
        self.workflow_container_id = workflow_container_id
        self.activity_container_id = activity_container_id

        self.database = self.client.create_database_if_not_exists(id=database_id)
        self.workflows_container = self.database.create_container_if_not_exists(
            id=workflow_container_id,
            partition_key=partition_key_cls(path="/id"),
        )
        self.activity_executions_container = (
            self.database.create_container_if_not_exists(
                id=activity_container_id,
                partition_key=partition_key_cls(path="/workflow_id"),
            )
        )

    def save_workflow(self, workflow: WorkflowInstance) -> None:
        self.workflows_container.upsert_item(workflow_to_document(workflow))

    def get_workflow(self, workflow_id: str) -> Optional[WorkflowInstance]:
        try:
            document = self.workflows_container.read_item(
                item=workflow_id,
                partition_key=workflow_id,
            )
        except Exception:
            return None

        return workflow_from_document(document)

    def get_workflows_by_name(self, name: str) -> list[WorkflowInstance]:
        documents = self.workflows_container.query_items(
            query=("SELECT * FROM c WHERE c.name = @name ORDER BY c.created_at DESC"),
            parameters=[{"name": "@name", "value": name}],
            enable_cross_partition_query=True,
        )
        return workflows_from_documents(documents)

    def list_workflows(self) -> list[WorkflowInstance]:
        documents = self.workflows_container.query_items(
            query="SELECT * FROM c ORDER BY c.created_at DESC",
            enable_cross_partition_query=True,
        )
        return workflows_from_documents(documents)

    def get_processing_workflows(self) -> list[WorkflowInstance]:
        documents = self.workflows_container.query_items(
            query=(
                "SELECT * FROM c WHERE c.status = @status ORDER BY c.created_at ASC"
            ),
            parameters=[{"name": "@status", "value": "processing"}],
            enable_cross_partition_query=True,
        )
        return workflows_from_documents(documents)

    def save_activity_execution(self, execution: ActivityExecution) -> None:
        self.activity_executions_container.upsert_item(activity_to_document(execution))

    def get_activity_executions(self, workflow_id: str) -> list[ActivityExecution]:
        documents = self.activity_executions_container.query_items(
            query=(
                "SELECT * FROM c WHERE c.workflow_id = @workflow_id "
                "ORDER BY c.created_at ASC"
            ),
            parameters=[{"name": "@workflow_id", "value": workflow_id}],
            partition_key=workflow_id,
        )
        return activities_from_documents(documents)
