"""
Class for interacting with AWS services
"""
import os
import boto3
from datetime import datetime

from config.config import config

class S3Conn:
    """
    """
    def __init__(self) -> None:
        self.s3 = boto3.resource('s3')

    def upload_file_by_name(self, source_fp: str, s3_fp: str, key_prefix: str) -> None:
        """
        Upload file to S3

        Parameters
        ----------
        source_fp: str - Source Filename with Full Path to be uploaded
        s3_fp: str - S3 Filename
        key_prefix: str - S3 Folder Prefix

        Returns
        ----------
        None
        """
        full_file_name = f'{key_prefix}/{s3_fp}'
        if os.path.isfile(source_fp):
            self.s3.meta.client.upload_file(source_fp, config['BUCKET_WORKING'], full_file_name)

    def move_s3_file(self, from_path: str, to_path: str, fetch_single_file: bool=False) -> None:
        """
        Move S3 files to another path within datawarehouse Bucket

        Parameters
        ----------
        from_path: str - S3 Source Folder Prefix
        to_path: str - S3 Target Folder Prefix
        fetch_single_file: bool - Flag if need to fetch a single file

        Returns
        ----------
        None
        """
        bucket_src = self.s3.Bucket(config['BUCKET_WORKING'])
        bucketListResultSet = bucket_src.objects.filter(Prefix=from_path)
        bucket_dest = self.s3.Bucket(config['BUCKET_WORKING'])
        numFile = 0
        for k in bucketListResultSet:
            if fetch_single_file and numFile > 0:
                break

            if is_s3_file_name(k.key):
                file_name_list = k.key.split('/')
                file_name = file_name_list[len(file_name_list)-1]
                if file_name.replace(" ", ""):
                    copy_source = {
                    'Bucket': config['BUCKET_WORKING'],
                    'Key': k.key
                    }
                    bucket_dest.copy(copy_source,"{0}{1}".format(to_path, file_name))
                    numFile = numFile + 1
                k.delete()

        if not fetch_single_file:
            self.s3.Object(config['BUCKET_WORKING'], from_path).put(Body="")

    def move_to_archive(self, prefix: str):
        """
        Move files from processing to archive (current date) on S3.

        Parameters
        ----------
        prefix: str - Prefix to filter files

        Returns
        ----------
        None
        """
        processing_directory = f"{prefix}/processing/"
        # Assumes archiving is based on underscored dates
        current_date = datetime.now()
        year = str(current_date.year).zfill(4)
        month = str(current_date.month).zfill(2)
        date = str(current_date.day).zfill(2)
        archive_directory = f"{prefix}/archive/{year}/{month}/{date}/"
        
        self.move_s3_file(processing_directory, archive_directory)

    def download_s3_file(self, s3_path: str, local_path: str) -> list[str]:
        """
        Download files under a S3 path to local filesystem.

        Parameters
        ----------
        s3_path: str - S3 path of source files
        local_path: str - Local filepath to download files to

        Returns
        ----------
        list[str] - List of downloaded files
        """
        bucket_src = self.s3.Bucket(config['BUCKET_WORKING'])
        bucketListResultSet = bucket_src.objects.filter(Prefix=s3_path)
        local_file_names = []
        for k in bucketListResultSet:
            if is_s3_file_name(k.key):
                full_name_array = k.key.split('/')
                name = full_name_array[len(full_name_array)-1]
                local_file_name = "{0}{1}".format(
                    local_path,
                    name
                )
                with open(local_file_name, 'wb') as data:
                    bucket_src.download_fileobj(k.key, data)
                local_file_names.append(local_file_name)
        return local_file_names
    
def is_s3_file_name(name: str) -> bool:
    """
    Checks if input is valid filename on S3.

    Parameters
        ----------
        name: str - Path on S3 to check

        Returns
        ----------
        bool - Whether path is valid filename
    """
    full_name_array = name.split('/')
    name = full_name_array[len(full_name_array)-1]
    if name:
        return True
    else:
        return False
