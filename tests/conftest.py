"""Actual loopback HTTP fixtures; subprocess ownership lives in scripts/local_stack.py."""
import pytest
from scripts.local_stack import AUTH, ROOT, free_port, local_stack, server


@pytest.fixture
def stack_factory(tmp_path):
    def factory(**kwargs):
        return local_stack(artifact_dir=tmp_path, **kwargs)
    return factory


@pytest.fixture
def stack(stack_factory):
    with stack_factory() as value:
        yield value
