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


def workflow(name_or_func=None):
    """
    Decorator to mark a function as a workflow.

    Can be used as:
    - @workflow (without parentheses)
    - @workflow() (with empty parentheses)
    - @workflow("custom_name") (with custom name)

    Args:
        name_or_func: Either a string name for the workflow, or the function being decorated
                     (when used without parentheses), or None (when used with empty parentheses).

    Example:
        @workflow
        def process_data_1(data: dict) -> dict:
            return data

        @workflow()
        def process_data_2(data: dict) -> dict:
            return data

        @workflow("my_workflow")
        def process_data_3(data: dict) -> dict:
            return data
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # If name_or_func is a string, use it as the name
        # If name_or_func is None (empty parentheses), use function name
        # If name_or_func is a function (no parentheses), use function name
        if isinstance(name_or_func, str):
            workflow_name = name_or_func
        else:
            workflow_name = func.__name__

        _registry.register_workflow(workflow_name, func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Add metadata to the function
        setattr(wrapper, "_is_workflow", True)
        setattr(wrapper, "_workflow_name", workflow_name)

        return wrapper

    # If name_or_func is a function (used as @workflow without parentheses),
    # apply the decorator immediately
    if callable(name_or_func):
        return decorator(name_or_func)

    # Otherwise, return the decorator function to be applied later
    return decorator


def activity(name_or_func=None):
    """
    Decorator to mark a function as an activity.

    Can be used as:
    - @activity (without parentheses)
    - @activity() (with empty parentheses)
    - @activity("custom_name") (with custom name)

    Args:
        name_or_func: Either a string name for the activity, or the function being decorated
                     (when used without parentheses), or None (when used with empty parentheses).

    Example:
        @activity
        def process_item_1(item: dict) -> dict:
            return {"processed": True, "item": item}

        @activity()
        def process_item_2(item: dict) -> dict:
            return {"processed": True, "item": item}

        @activity("custom_activity")
        def process_item_3(item: dict) -> dict:
            return {"processed": True, "item": item}
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # If name_or_func is a string, use it as the name
        # If name_or_func is None (empty parentheses), use function name
        # If name_or_func is a function (no parentheses), use function name
        if isinstance(name_or_func, str):
            activity_name = name_or_func
        else:
            activity_name = func.__name__

        _registry.register_activity(activity_name, func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Add metadata to the function
        setattr(wrapper, "_is_activity", True)
        setattr(wrapper, "_activity_name", activity_name)

        return wrapper

    # If name_or_func is a function (used as @activity without parentheses),
    # apply the decorator immediately
    if callable(name_or_func):
        return decorator(name_or_func)

    # Otherwise, return the decorator function to be applied later
    return decorator


def get_registry() -> WorkflowRegistry:
    """Get the global workflow registry."""
    return _registry
