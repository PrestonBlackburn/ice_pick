
from unittest import mock

import pytest


import snowflake.snowpark
from snowflake.snowpark import Session
from snowflake.snowpark._internal.server_connection import ServerConnection

from ice_pick.extension import extend_session
from ice_pick.account_object import (
    Warehouse,
    Role,
    AccountObject
)

from ice_pick.schema_object import (
    SchemaObject
)

from ice_pick.utils import (
    concat_standalone
)


def test_ext_functions():

    # Create a fake connection
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()

    # extend the session class
    Session_ext = extend_session(snowflake.snowpark.session.Session)
    mock_ext_session = Session_ext(mock_connection)

    # Create objects
    wh_ext = mock_ext_session.Warehouse("COMPUTE_WH")
    role_ext = mock_ext_session.Role("ROLE1")

    # account object test
    assert wh_ext.name == "COMPUTE_WH"
    assert role_ext.name == "ROLE1"
    
    # session object test
    assert mock_ext_session.create_schema_object("TEST", "SCHEMA_1", "CUSTOMER", "TABLE") == SchemaObject(mock_ext_session, "TEST", "SCHEMA_1", "CUSTOMER", "TABLE")
