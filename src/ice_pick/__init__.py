
#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""
Contains core classes of Ice Pick
"""

__all__ = [
    "AccountObject",
    "Warehouse",
    "Role",
    "SchemaObject",
    "SchemaObjectFilter",
    "extend_session",
    "concat_standalone"
]


from ice_pick.version import VERSION

__version__ = ".".join(str(x) for x in VERSION if x is not None)


from ice_pick.schema_object import SchemaObject
from ice_pick.filter import SchemaObjectFilter

from ice_pick.account_object import AccountObject
from ice_pick.account_object import Warehouse
from ice_pick.account_object import Role

from ice_pick.extension import extend_session
from ice_pick.utils import concat_standalone

