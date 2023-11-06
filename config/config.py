import os

config = {}

config['BUCKET_WORKING'] = os.environ['BUCKET_WORKING']
config['BUCKET_PREFIX'] = os.environ['BUCKET_PREFIX']
config['BASE_URL'] = os.environ['BASE_URL']

config['RETRY_LIMIT'] = os.getenv('RETRY_LIMIT', 3)
