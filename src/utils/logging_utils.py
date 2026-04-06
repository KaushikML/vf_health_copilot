"""Logging helpers with consistent formatting."""

from __future__ import annotations

import logging
from typing import Union


def get_logger(name: str, level: Union[int, str] = logging.INFO) -> logging.Logger:
    """Return a configured logger instance.

    The function avoids adding duplicate handlers when imported repeatedly from
    notebooks, tests, or Streamlit sessions.
    """

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
