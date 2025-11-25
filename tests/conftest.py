# tests/conftest.py
import time
import logging
from ezl import core as ezlcore

logging.basicConfig(level=logging.INFO)
ezlcore.logger.setLevel(logging.INFO)


def small_sleep():
    # use a small but non-zero sleep used by tests to allow threads to run
    time.sleep(0.02)
