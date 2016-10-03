import pandas as pd


def _is_categorical(df, field):
    """
    checks to see if a field is categorical already
    :param df: source dataframe
    :param field: field to be checked
    :return: boolean True/False if the dataframe is an object.
    """
    return df[field].dtype.name == 'category'


def _is_object_type(df, field):
    """
    checks if the data type for a field is an object (required to cast to categorical)
    :param df: source dataframe
    :param field: source field to check
    :return: boolean True/False if the dataframe is an object.
    """
    return df[field].dtype.name == 'object'


def categorical_cast(dataframe, cat_field):
    """
    Converts a field to categorical type in order to save memory space and speed up basis expansion.
    :param dataframe: source data frame
    :param cat_field: field to be processed.
    :return: the source dataframe
    """
    if _is_object_type(dataframe, cat_field):
        dataframe[cat_field] = dataframe[cat_field].astype('category')
        return dataframe
    else:
        raise TypeError


def basis_expansion(dataframe, expansion_field, replace=True, sort_flag=False, order=None):
    """
    Expand a categorical dataframe field to split out new fields and fill with boolean values.
    :param dataframe: The source dataframe
    :param expansion_field: the basis expansion field.
    :param replace: bool flag for replacing the original field or leaving it alone.  Defaults to replace.
    :param sort_flag: if True, sort by values for the field creation.
    :param order:
    :return: the original dataframe
    """
    try:
        if not _is_categorical(dataframe, expansion_field):
            categorical_cast(dataframe, expansion_field)
        if _is_categorical(dataframe, expansion_field):
            labels, uniques = pd.factorize(dataframe[expansion_field], sort=sort_flag, order=order)
            unique_count = uniques.size
            for i in range(unique_count):
                field_name = str(expansion_field) + '_' + str(uniques[i])
                dataframe.loc[dataframe[expansion_field] == uniques[i], field_name] = 1
                dataframe[field_name].fillna(0, inplace=True)
            if replace:
                dataframe.drop([expansion_field], inplace=True, axis=1)
            return dataframe
    except TypeError:
        raise
