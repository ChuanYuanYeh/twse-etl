import os
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)

from flows.extractor import flow


def lambda_handler(event, context):
    flow_name = os.path.splitext(os.path.basename(__file__))[0]
    logger = logging.getLogger(flow_name)
    date = datetime.now().strftime('%Y%m%d')
    flow(
        endpoint='TWT93U',
        extraction_date=date,
        logger=logger
    )
