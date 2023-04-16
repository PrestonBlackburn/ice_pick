
from unittest import mock

import pytest


from snowflake.snowpark import Session
from ice_pick.account_object import (
    Warehouse,
    Role,
    AccountObject
)



def test_warehouse_init():
    Session_mock = mock.create_autospec(Session)

    warehouse = Warehouse(Session_mock, "COMPUTE_WH")
    
    assert warehouse.name == "COMPUTE_WH"
    assert warehouse.object_type == "warehouse"
    assert warehouse.session == Session_mock

