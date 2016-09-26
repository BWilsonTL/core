#!/Users/bewilson/PycharmProjects/sfconnector/
# Filename: connector.py
from __future__ import absolute_import
import snowflake.connector
import pandas as pd


def account_details(directory_loc):
    """
    Retrieve a username and password for snowflake from a directory file.
    file requires 2 lines: first line: username, 2nd line: password
    :param directory_loc: location of username / password for snowflake
    :return: tuple of username and password
    """
    with open(directory_loc) as f:
        lines = f.readlines()
    return lines[0].rstrip(), lines[1].rstrip()


class SnowConnect(object):
    def __init__(self, query, username, password, account='ruelala', ijson=False):
        """
        Snowflake connection handler for batch queries
        :param query: block str of the query
        :param username: snowflake username
        :param password: snowflake password
        :param account: snowflake account (default: ruelala)
        :param ijson: for bulk queries that will run into memory issues, set to True (will slow down query)
        """
        self.query = query
        self.user = username
        self.pwd = password
        self.act = account
        self.ijson = ijson

    def execute_query(self):
        """
        execute the query and return the result as a pandas dataframe object.
        :return: the query as a dataframe.
        """
        conn = snowflake.connector.connect(
            user=self.user,
            password=self.pwd,
            account=self.act
        )
        cs = conn.cursor()
        col_list = list()
        try:
            cs.execute(self.query, _use_ijson=self.ijson)
            data_set = pd.DataFrame(cs.fetchall())
            meta = cs.description
            for ele in meta:
                col_list.append(ele[0])
            data_set.columns = col_list
        finally:
            cs.close()
        return data_set

# end of module sfconnector.py
