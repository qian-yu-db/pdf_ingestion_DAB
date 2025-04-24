# Common pytest fixtures for all test modules
import os
import sys
from unittest.mock import MagicMock

import pytest

# Add src directory to path to allow imports in tests
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def dbutils_mock():
    """Create a mock DBUtils."""
    dbutils = MagicMock()
    notebook = MagicMock()
    fs = MagicMock()
    dbutils.fs = fs

    # Mocking the notebook entry point
    entry_point = MagicMock()
    entry_point.getDbutils.return_value = notebook
    notebook.notebook.return_value = notebook
    notebook.getContext.return_value = notebook
    notebook.apiToken.return_value = notebook
    notebook.get.return_value = "mock-token"

    dbutils.notebook.entry_point = entry_point
    return dbutils
