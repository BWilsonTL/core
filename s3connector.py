import os
import sys
import boto
import uuid
import pandas as pd
from datetime import datetime
from boto.s3.key import Key
from boto.s3.connection import Bucket


def aws_key_retrieve_local(file_loc):
    """
    Retrieve the keys from the credentials file that Amazon provides through IAM
    :param file_loc: location of credentials file
    :return: parse the file and return the keys and username.
    """
    file_dir = os.path.join(os.path.expanduser('~'), file_loc)
    s3_keys = pd.read_csv(file_dir)
    user_name = s3_keys['User Name'][0]
    access_key_id = s3_keys['Access Key Id'][0]
    secret_key_id = s3_keys['Secret Access Key'][0]

    return user_name, access_key_id, secret_key_id


def file_name_standardizer(file_mask, version, uuid_use=False):
    """
    Standardize the file name for upload to S3
    :param file_mask: type of data (ex: btq, btq_prep)
    :param version: version of the code generating the file (ex: v3_1)
    :param uuid_use: optional uuid to prevent file collision (example: threading)
    :return: the file name as a str
    """
    current_dt = datetime.utcnow()
    dt_string = current_dt.strftime('%Y%m%d_%H%M%S')
    if uuid_use:
        new_uuid = str(uuid.uuid4())
        return file_mask + '_' + version + '_' + dt_string + '_' + new_uuid
    else:
        return file_mask + '_' + version + '_' + dt_string


def s3_dataframe_upload(aws_access_key, aws_secret_key, dataframe, s3_bucket, s3_path, s3_file_name,
                        content_type=None, validate=True, callback=None, reduced_redundancy=False, md5=None):

    # Convert the dataframe to an in-memory string tsv
    data = dataframe.to_csv(None, sep='|', encoding='utf-8', index=False, header=True)
    # get the size of the string to ensure that all data transferred to S3.
    file_size = sys.getsizeof(data)

    try:
        # get a connection to s3
        conn = boto.connect_s3(aws_access_key, aws_secret_key)
        # get the bucket
        bucket = conn.get_bucket(s3_bucket, validate=validate)
        # get the key for the bucket
        s_path = os.path.join(s3_path, s3_file_name)
        k = Key(bucket)
        k.key = s_path
        if content_type:
            k.set_metadata('Content-Type', content_type)
        # send the data
        k.set_contents_from_string(data, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy)
        success = True
    except Exception as error:
        print("Upload to s3 failed.")
        raise error.with_traceback(sys.exc_info()[2])
    assert success, "Upload of dataframe %s to s3 failed." % dataframe
    return success


def s3_file_cleaner(aws_access_key, aws_secret_key, s3_bucket, filepath):
    try:
        # get connection
        conn = boto.connect_s3(aws_access_key, aws_secret_key)
        # retrieve the bucket
        b = Bucket(conn, s3_bucket)
        # get the key for the bucket
        k = Key(b)
        # set the filepath
        k.key = filepath
        # delete the object on s3 via REST API function.
        b.delete_key(k)
        success = True
    except Exception as error:
        raise error.with_traceback(sys.exc_info()[2])
    assert success, "Deletion of bucket %s failed." % s3_bucket
    return success




