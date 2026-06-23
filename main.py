"""Convenience launcher.

Equivalent to running the installed `oep-upload` command or `python -m oep_upload`.
Kept so `python main.py` keeps working from a checkout.
"""

import sys

from oep_upload.cli import main

if __name__ == "__main__":
    sys.exit(main())