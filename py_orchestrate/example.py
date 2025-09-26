"""
Simple example demonstrating py-orchestrate usage.
"""

from py_orchestrate import workflow, activity, Orchestrator


@activity("fetch_user")
def fetch_user(user_id: int) -> dict:
    """Fetch user data from database."""
    # Simulate database fetch
    return {"id": user_id, "name": f"User{user_id}", "email": f"user{user_id}@example.com"}


@activity("send_email")
def send_email(user: dict, subject: str, body: str) -> dict:
    """Send email to user."""
    # Simulate email sending
    print(f"Sending email to {user['email']}: {subject}")
    return {"sent": True, "recipient": user["email"], "subject": subject}


@workflow("user_notification")
def user_notification_workflow(user_id: int, message: str) -> dict:
    """Workflow to fetch user and send notification."""
    # Step 1: Fetch user data
    user = fetch_user(user_id)
    
    # Step 2: Send notification email
    email_result = send_email(
        user=user,
        subject="Important Notification",
        body=message
    )
    
    return {
        "user_notified": True,
        "user": user,
        "email_result": email_result
    }


def main():
    """Run the example workflow."""
    # Create and start orchestrator
    orchestrator = Orchestrator("example.db")
    orchestrator.start()
    
    try:
        # Execute workflow
        workflow_id = orchestrator.invoke_workflow(
            "user_notification",
            user_id=123,
            message="Your account has been updated successfully!"
        )
        
        print(f"Started workflow: {workflow_id}")
        
        # Monitor until completion
        import time
        while True:
            status = orchestrator.get_workflow_status(workflow_id)
            print(f"Status: {status['status']}")
            
            if status['status'] in ['done', 'failed']:
                if status['status'] == 'done':
                    print(f"Result: {status['output']}")
                else:
                    print(f"Error: {status['error_message']}")
                break
            
            time.sleep(0.5)
    
    finally:
        orchestrator.stop()


if __name__ == "__main__":
    main()