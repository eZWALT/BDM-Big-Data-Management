import pytest
from unittest.mock import patch
import time
from src.utils.task import Task, TaskStatus


# Mock Task subclass for testing
class MockTask(Task):
    def setup(self):
        # Mock setup method (does nothing)
        pass

    def execute(self):
        # Simulate a task that takes some time
        time.sleep(1)
        return "Execution successful"


# Test Task Status Transition
def test_task_status_initialization():
    task = MockTask()
    assert task.status() == TaskStatus.PENDING


# Test Task Execution Flow
@patch("time.sleep", return_value=None)  # Mock sleep to avoid actual delay in test
@patch("loguru.logger.info")
@patch("loguru.logger.success")
@patch("loguru.logger.error")
@patch("loguru.logger.warning")
def test_task_execution(mock_warning, mock_error, mock_success, mock_info, mock_sleep):
    task = MockTask()

    # Start with PENDING status
    assert task.status() == TaskStatus.PENDING

    # Call execute
    task.execute()

    # Check status after execution
    assert task.status() == TaskStatus.COMPLETED

    # Ensure logs are called for task progress
    mock_info.assert_called_once_with("[MOCK TASK] Task starting...")
    mock_success.assert_called_once_with("[MOCK TASK] Task completed after 0.00 seconds")  # Update to 0.00 seconds

    # Check for any errors or warnings during successful execution
    mock_error.assert_not_called()
    mock_warning.assert_not_called()


# Test Task Cancellation (KeyboardInterrupt)
@patch("time.sleep", side_effect=KeyboardInterrupt)
@patch("loguru.logger.info")
@patch("loguru.logger.warning")
def test_task_cancelled(mock_warning, mock_info, mock_sleep):
    task = MockTask()

    # Start with PENDING status
    assert task.status() == TaskStatus.PENDING

    # Test KeyboardInterrupt during execution
    with pytest.raises(KeyboardInterrupt):
        task.execute()

    # Check status after cancellation
    assert task.status() == TaskStatus.CANCELLED

    # Ensure logs are called for task cancellation
    mock_info.assert_called_once_with("[MOCK TASK] Task starting...")
    mock_warning.assert_called_once_with("[MOCK TASK] Task cancelled after 0.00 seconds")


# Test Task Failure (General Exception)
@patch("time.sleep", side_effect=Exception("Something went wrong"))
@patch("loguru.logger.info")
@patch("loguru.logger.error")
def test_task_failure(mock_error, mock_info, mock_sleep):
    task = MockTask()

    # Start with PENDING status
    assert task.status() == TaskStatus.PENDING

    # Test general failure during execution
    with pytest.raises(Exception):
        task.execute()

    # Check status after failure
    assert task.status() == TaskStatus.FAILED

    # Ensure logs are called for task failure
    mock_info.assert_called_once_with("[MOCK TASK] Task starting...")
    mock_error.assert_called_once_with("[MOCK TASK] Task failed after 0.00 seconds: Something went wrong")


# Test Task Status without execution
def test_task_status_without_execution():
    task = MockTask()

    # Check initial status
    assert task.status() == TaskStatus.PENDING
