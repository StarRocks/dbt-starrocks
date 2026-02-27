import pytest
from unittest.mock import patch
from dbt.adapters.sql import SQLAdapter

seed_csv = """
id,value
1,a
2,b
3,c
""".lstrip()

long_running_model = """
{{ config(materialized='table') }}
select *, SLEEP(30) from {{ ref('seed_data') }}
""".lstrip()

class TestTaskCancellation:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_data.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "long_running_model.sql": long_running_model,
        }

    def test_cancel_task_called(self, project):
        """
        Test that _cancel_task is called when cancel_open_connections is invoked.
        """
        adapter = project.adapter
        
        # Simulate a running task
        adapter._running_tasks = {"test_connection": "test_task_id_123"}
        
        # Mock _cancel_task
        with patch.object(adapter, '_cancel_task') as mock_cancel:
            adapter.cancel_open_connections()
            
            mock_cancel.assert_called_once_with("test_task_id_123")

    def test_cancel_task_executes_drop(self, project):
        """
        Test that _cancel_task executes DROP TASK statement.
        """
        adapter = project.adapter
        task_id = "test_drop_task_123"
        
        # Mock the parent execute method to capture SQL
        with patch.object(SQLAdapter, 'execute') as mock_execute:
            adapter._cancel_task(task_id)
            
            # Verify DROP TASK was called
            mock_execute.assert_called_once()
            call_args = mock_execute.call_args
            assert f"DROP TASK `{task_id}`" in call_args[1]['sql']
