"""Allow ``python -m formualizer`` to use the console entry."""

import sys

from .cli import main

sys.exit(main())
