import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest

from src.taxi.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("nyc-taxi-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()