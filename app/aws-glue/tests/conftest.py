"""
Root test configuration — ensures app/ is on sys.path.

This allows tests to import from ``src.dependencies`` (e.g.
``from src.dependencies.config import Config``) regardless
of the working directory when pytest is invoked.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add app/ to sys.path so that "from src.dependencies import ..." resolves.
_app_root = Path(__file__).parent.parent.resolve()
if str(_app_root) not in sys.path:
    sys.path.insert(0, str(_app_root))
