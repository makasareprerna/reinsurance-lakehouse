import pytest

from relake.config import load_config


@pytest.fixture(scope="session")
def spark():
    from relake.spark import get_spark

    session = get_spark("relake-tests")
    yield session
    session.stop()


@pytest.fixture()
def cfg(tmp_path):
    """A config whose data paths all live in a fresh temporary folder."""
    return load_config(root=tmp_path, scale=0.3)
