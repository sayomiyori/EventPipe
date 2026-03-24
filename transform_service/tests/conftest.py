import os

import pytest


@pytest.fixture
def integration_env() -> None:
    if os.environ.get("TRANSFORM_INTEGRATION") != "1":
        pytest.skip("Set TRANSFORM_INTEGRATION=1 and start docker compose services")
