"""Allow `python -m openbridge <args>` to run the CLI."""
from .cli import main
import sys

if __name__ == "__main__":
    sys.exit(main())
