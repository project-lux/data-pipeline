"""The one logger every module in the package shares.

Use ``from lux_pipeline.log import logger`` or
``logging.getLogger("lux_pipeline")``; they are the same object.
"""

import logging

logger = logging.getLogger("lux_pipeline")
