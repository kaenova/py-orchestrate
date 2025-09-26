---
name: Bug report
about: Create a report to help us improve
title: '[BUG] '
labels: bug
assignees: ''
---

## Bug Description
A clear and concise description of what the bug is.

## To Reproduce
Steps to reproduce the behavior:
1. Create workflow with '...'
2. Execute activity '...'
3. See error

## Expected Behavior
A clear and concise description of what you expected to happen.

## Actual Behavior
A clear and concise description of what actually happened.

## Code Example
```python
# Minimal code example that reproduces the issue
from py_orchestrate import workflow, activity, Orchestrator

@activity("example_activity")
def example_activity():
    # Your code here
    pass

@workflow("example_workflow") 
def example_workflow():
    # Your code here
    pass
```

## Environment
- py-orchestrate version: [e.g. 0.1.0]
- Python version: [e.g. 3.12.0]
- Operating System: [e.g. Ubuntu 22.04, macOS 14.0, Windows 11]

## Error Message
```
Paste the full error message and stack trace here
```

## Database State
- [ ] This issue involves workflow persistence/recovery
- [ ] Database file size: [if relevant]
- [ ] Number of workflows in database: [if known]

## Additional Context
Add any other context about the problem here, such as:
- Does this happen consistently or intermittently?
- Did this work in a previous version?
- Any workarounds you've found?