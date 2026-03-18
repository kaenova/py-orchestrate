"""Shared pytest fixtures for the test suite."""

from __future__ import annotations

from pathlib import Path

import pytest

from py_orchestrate.decorators import get_registry


@pytest.fixture(autouse=True)
def clear_workflow_registry():
    """Reset the global workflow registry between tests."""
    registry = get_registry()
    registry.workflows.clear()
    registry.activities.clear()
    yield
    registry.workflows.clear()
    registry.activities.clear()


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """Create a temporary SQLite database path."""
    return str(tmp_path / "py_orchestrate_test.db")
