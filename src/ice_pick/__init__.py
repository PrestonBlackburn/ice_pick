
#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""
Contains core classes of Ice Pick
"""

__all__ = [
    "SchemaObject",
    "SchemaObjectFilter",
    "extend_session"
]


from ice_pick.version import VERSION

__version__ = ".".join(str(x) for x in VERSION if x is not None)


from ice_pick.schema_object import SchemaObject
from ice_pick.filter import SchemaObjectFilter
from ice_pick.extension import extend_session

