"""
Framework for transforming raw data to desired format
"""
import os
from logging import Logger

from config.config import config
from tools.csv_transformer import ShortSalesTranformer
from tools.s3_conn import S3Conn


def flow(logger: Logger):
    s3_conn = S3Conn()
    trf = ShortSalesTranformer()

    os.makedirs('/tmp/output/', exist_ok=True)
    s3_prefix = f'{config["BUCKET_PREFIX"]}/output'

    logger.info('Moving files to /processing/')
    s3_conn.move_s3_file(f'{config["BUCKET_PREFIX"]}/to_be_processed/', f'{config["BUCKET_PREFIX"]}/processing/')
    
    logger.info('Downloading files for transforming')
    fps = s3_conn.download_s3_file(f'{config["BUCKET_PREFIX"]}/processing/', f'/tmp/')
    
    logger.info('Transforming data')
    for raw_fp in fps:
        filename = raw_fp.split('/')[len(raw_fp.split('/'))-1]
        output_fp = f'/tmp/output/{filename}'
        raw_df = trf.get_raw_data(raw_fp)
        transformed_df = trf.transform(raw_df)
        trf.write_data(transformed_df, output_fp)
        
        logger.info('Uploading to S3')
        s3_conn.upload_file_by_name(output_fp, filename, s3_prefix)
    
    logger.info('Archiving raw files')
    s3_conn.move_to_archive(f'{config["BUCKET_PREFIX"]}')
    
