import os
import sys
import boto
import pandas as pd
from boto.s3.key import Key
from boto.s3.connection import Bucket


def aws_key_retrieve(file_loc):
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


def s3_upload(aws_access_key, aws_secret_key, dataframe, s3_bucket, key,
              content_type=None, validate=True, callback=None, reduced_redundancy=False, md5=None):

    # Convert the dataframe to an in-memory string tsv
    data = dataframe.to_csv(None, sep='\t', encoding='utf-8', index=False, header=None)
    # get the size of the string to ensure that all data transferred to S3.
    file_size = sys.getsizeof(data)

    try:
        # get a connection to s3
        conn = boto.connect_s3(aws_access_key, aws_secret_key)
        # get the bucket
        bucket = conn.get_bucket(s3_bucket, validate=validate)
        # get the key for the bucket
        k = Key(bucket)
        k.key = key
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


user, access, secret = aws_key_retrieve('Documents/credentials.csv')



