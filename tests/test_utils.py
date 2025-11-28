"""Tests for py_utils.utils module."""

import pytest
from py_utils.utils import example_function


def test_example_function():
    """Test the example_function returns expected output."""
    result = example_function()
    assert result == "Hello from py-utils!"
    assert isinstance(result, str)
