# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-09-26

### Added
- Initial release of py-orchestrate
- Workflow orchestrator with embedded SQLite database
- `@workflow` and `@activity` decorators for defining workflows and activities
- Persistent state management with automatic recovery
- Fault tolerance - workflows resume after application restart
- Background workflow execution with thread pool
- Activity-level recovery and caching
- Comprehensive workflow status tracking
- Support for Python 3.12+

### Features
- **Workflow Management**: Define and execute multi-step workflows
- **Activity Orchestration**: Compose workflows from reusable activities  
- **Persistent State**: All workflow state stored in SQLite database
- **Automatic Recovery**: Resume interrupted workflows from last completed activity
- **Status Monitoring**: Query workflow status, current activity, and results
- **Error Handling**: Graceful failure handling with error messages
- **Background Execution**: Non-blocking workflow execution
- **Simple API**: Easy-to-use decorators and orchestrator interface

### Technical Details
- Built on SQLite for embedded persistence
- Thread-based concurrent execution
- Activity-level atomicity and recovery
- JSON serialization for workflow data
- Comprehensive logging and monitoring