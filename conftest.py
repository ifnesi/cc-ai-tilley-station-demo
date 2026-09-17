"""Test-time environment.

The apps read every setting from the environment (from the git-ignored .env) and
fail loudly if a value is missing — they never fall back to .env_example. So for
the test run we load the committed .env_example here (no secrets, just the
documented defaults) before any app module is imported, making the suite
self-contained and independent of a developer's real .env.
"""

import pathlib

from dotenv import load_dotenv

# Loaded at conftest import (before test modules import emulator.reference), and
# does not override anything already set in the environment.
load_dotenv(pathlib.Path(__file__).resolve().parent / ".env_example")
