
from unittest import mock

import pytest


from snowflake.snowpark import Session
from ice_pick.account_object import (
    Warehouse
)



def test_init():
    warehouse = Warehouse(Session, "TEST")

    print(warehouse)