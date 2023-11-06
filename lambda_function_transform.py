import os
import logging
logging.basicConfig(level=logging.INFO)

from flows.transformer import flow


def lambda_handler(event, context):
    flow_name = os.path.splitext(os.path.basename(__file__))[0]
    logger = logging.getLogger(flow_name)
    flow(
        logger=logger
    )
