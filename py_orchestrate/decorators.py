"""
Decorators for workflow and activity functions.
"""

import functools
from typing import Callable, Any, Dict, Optional


class WorkflowRegistry:
    """Registry for workflow and activity functions."""

    def __init__(self) -> None:
        self.workflows: Dict[str, Callable] = {}
        self.activities: Dict[str, Callable] = {}

    def register_workflow(self, name: str, func: Callable[..., Any]) -> None:
        """Register a workflow function."""
        self.workflows[name] = func

    def register_activity(self, name: str, func: Callable[..., Any]) -> None:
        """Register an activity function."""
        self.activities[name] = func

    def get_workflow(self, name: str) -> Callable[..., Any]:
        """Get a workflow function by name."""
        if name not in self.workflows:
            raise ValueError(f"Workflow '{name}' not found")
        return self.workflows[name]

    def get_activity(self, name: str) -> Callable[..., Any]:
        """Get an activity function by name."""
        if name not in self.activities:
            raise ValueError(f"Activity '{name}' not found")
        return self.activities[name]


# Global registry instance
_registry = WorkflowRegistry()


def workflow(name: Optional[str] = None):
    """
    Decorator to mark a function as a workflow.

    Args:
        name: Optional name for the workflow. If not provided, uses function name.

    Example:
        @workflow("my_workflow")
        def process_data(data: dict) -> dict:
            # Orchestrate activities
            result1 = activity_func1(data)
            result2 = activity_func2(result1)
            return result2
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        workflow_name = name or func.__name__
        _registry.register_workflow(workflow_name, func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Add metadata to the function
        setattr(wrapper, "_is_workflow", True)
        setattr(wrapper, "_workflow_name", workflow_name)

        return wrapper

    return decorator


def activity(name: Optional[str] = None):
    """
    Decorator to mark a function as an activity.

    Args:
        name: Optional name for the activity. If not provided, uses function name.

    Example:
        @activity("process_item")
        def process_item(item: dict) -> dict:
            # Process the item
            return {"processed": True, "item": item}
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        activity_name = name or func.__name__
        _registry.register_activity(activity_name, func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Add metadata to the function
        setattr(wrapper, "_is_activity", True)
        setattr(wrapper, "_activity_name", activity_name)

        return wrapper

    return decorator


def get_registry() -> WorkflowRegistry:
    """Get the global workflow registry."""
    return _registry
