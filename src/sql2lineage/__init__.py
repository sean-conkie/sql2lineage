"""sql2lineage package."""

from os import PathLike
from pathlib import Path
from typing import Optional, TypeAlias

__app_name__ = "sql2lineage"
__version__ = "0.2.0"

StrPath: TypeAlias = str | PathLike[str]


def walk_dir(path: StrPath, glob: Optional[str] = None):

    pass
