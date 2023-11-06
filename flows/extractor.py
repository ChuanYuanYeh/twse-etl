"""
Framework for extracting and uploading raw data
"""
import os
import pandas as pd
from logging import Logger

from config.config import config
from tools.rest_api import RestAPI
from tools.html_parser import HTMLTableParser
from tools.s3_conn import S3Conn


def flow(endpoint: str, extraction_date: str, logger: Logger) -> None:
    """
    The extractor flow will extract data from an HTTP request and save it to
    S3 for further processing.

    Parameters
    ----------
    endpoint: str - API endpoint to request
    extraction_date: str - Date for request parameter and output filename
    logger: Logger - Object for logging messages

    Returns
    ----------
    None
    """
    requester = RestAPI(config['BASE_URL'])
    html_parser = HTMLTableParser()
    s3_conn = S3Conn()

    os.makedirs('/tmp/', exist_ok=True)
    filename = f'{extraction_date}.csv'
    local_fp = f'/tmp/{filename}'
    s3_prefix = f'{config["BUCKET_PREFIX"]}/to_be_processed'

    params = {
        'date': extraction_date,
        'response': 'html'
    }

    logger.info('GETting raw response')
    response = requester.send_get_request(endpoint, params=params)
    
    logger.info('Parsing and saving HTML table')
    df = html_parser.parse_table_from_raw_text(response)
    html_parser.write_table(local_fp, df)
    
    logger.info('Uploading to S3')
    s3_conn.upload_file_by_name(local_fp, filename, s3_prefix)
