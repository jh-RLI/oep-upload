"""Enable `python -m oep_upload` to run the CLI."""

import sys

from oep_upload.cli import main

if __name__ == "__main__":
    sys.exit(main())