#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""
Contains core classes of Ice Pick
"""

__all__ = [
    "AccountObject",
    "Account",
    "Warehouse",
    "Role",
    "User",
    "Database",
    "Schema",
    "Integration",
    "NetworkPolicy",
    "ResourceMonitor",
    "SchemaObject",
    "SchemaObjectFilter",
    "AccountObjectFilter",
    "extend_session",
    "concat_standalone",
    "melt_standalone",
    "Privilege",
    "Grant",

]


from ice_pick.version import VERSION

__version__ = ".".join(str(x) for x in VERSION if x is not None)


from ice_pick.schema_object import SchemaObject
from ice_pick.filters import SchemaObjectFilter, AccountObjectFilter

from ice_pick.account_object import (
    AccountObject,
    Account,
    Warehouse, 
    Role, 
    User,
    Database,
    Schema,
    Integration,
    NetworkPolicy,
    ResourceMonitor,
)

from ice_pick.privileges import Privilege, Grant

from ice_pick.extension import extend_session
from ice_pick.utils import concat_standalone
from ice_pick.utils import melt_standalone
