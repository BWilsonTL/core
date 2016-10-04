

def df_type_check(df_field):
    """
    Converts pandas dataframe dtypes to standard python types
    :param df_field: dataframe field to check
    :return: the type in a python standard type
    """
    type_val = df_field.dtype
    if type_val == 'O':
        return str
    elif type_val == 'float64':
        return float
    elif type_val == 'int64':
        return int
    else:
        return df_field.dtype


def df_field_fill_clean(df, fields, replace_missing_val=None):
    """
    Fills a field with a replacement value, uppercases strings if fields are string type.
    :param df: pandas dataframe object
    :param fields: list of fields in the dataframe
    :param replace_missing_val: replacement value for missing data
    :return: a copy of the original dataframe.
    """
    df_new = df.copy()
    for idx, field in enumerate(fields):
        field_type = df_type_check(df[field])
        if replace_missing_val:
            if field_type == type(replace_missing_val):
                df_new[field].fillna(replace_missing_val, inplace=True)
            else:
                raise TypeError('Frame field incorrect type for target replacement value.')
        if field_type == str:
            df_new[field] = df_new[field].str.upper()
    return df_new


