import logging
import sys
import pytest
from pathlib import Path

from src.ice_pick.connection import get_credentials


@pytest.fixture
def filepath(path = "sf_conn.config"):
    return path



def test_get_credentials(filepath):
    creds = get_credentials(filepath)

    assert isinstance(creds, dict)



