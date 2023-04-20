
from unittest import mock

import pytest


from snowflake.snowpark import Session
from ice_pick.schema_object import (
    SchemaObject
)




def test_table_init():
    Session_mock = mock.create_autospec(Session)

    table_obj = SchemaObject(Session_mock, "TEST", "SCHEMA_1", "CUSTOMER", "TABLE")
    
    assert table_obj.object_name == "CUSTOMER"
    assert table_obj.object_type == "TABLE"
    assert table_obj.database == "TEST"
    assert table_obj.schema == "SCHEMA_1"

