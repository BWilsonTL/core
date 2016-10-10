
import json
import os
from pathlib import Path


class EnvironmentConfig(object):
    # Set up the configuration for a job by reading in a json file that has connection parameters and job config
    # File path in any environment needs to be set to the user base directory in a folder titles 'config'
    def __init__(self):
        self.json_path = os.path.join(os.path.expanduser("~"), 'config/rdsconfig.json')

    def config_check(self):
        # check that the json file is present in the location that it needs to be in.
        c_file = Path(self.json_path)
        if c_file.is_file():
            return True

    def load_environment(self):
        # parse the json file and return the file's contents as a json object (python dict).
        if self.config_check():
            with open(self.json_path) as json_file:
                config_data = json.load(json_file)
            return config_data
        else:
            raise FileNotFoundError('Config file is not located at %s ' % str(self.json_path))

    def get_snowflake_conn(self):
        # Get the snowflake connection parameters (username and password)
        try:
            snowflake_config = self.load_environment()['ioconfig']['snowflake']
            username = snowflake_config['username']
            password = snowflake_config['password']
            return username, password
        except:
            raise

    def get_s3_conn(self):
        # Get the s3 connection parameters (username, access key, and secret key)
        try:
            s3_config = self.load_environment()['ioconfig']['s3']
            username = s3_config['username']
            access_key = s3_config['accesskey']
            secret_key = s3_config['secretkey']
            return username, access_key, secret_key
        except:
            raise

    def job_config(self, job_name):
        # Get job configuration parameters.
        try:
            return self.load_environment()['jobconfig'][job_name]
        except:
            raise


