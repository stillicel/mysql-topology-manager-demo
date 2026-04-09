"""
Plugin registry for MySQL cluster inspection checkers.

Each checker registers itself via the @register_checker decorator.
The registry maintains insertion order so checks run in a predictable sequence.
"""

_checker_registry = {}


def register_checker(name, description=""):
    """Decorator to register an inspection checker."""
    def decorator(func):
        _checker_registry[name] = {
            "func": func,
            "description": description,
        }
        return func
    return decorator


def get_all_checkers():
    """Return a copy of the checker registry."""
    return dict(_checker_registry)


# Import all checker modules so they self-register on import.
from mysql_topo.checkers import connection_count  # noqa: E402, F401
from mysql_topo.checkers import topology_scale     # noqa: E402, F401
from mysql_topo.checkers import schema_scale       # noqa: E402, F401
