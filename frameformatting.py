import pandas as pd


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
    labels, uniques = pd.factorize(dataframe[expansion_field], sort=sort_flag, order=order)
    unique_count = uniques.size
    for i in range(unique_count):
        field_name = str(expansion_field) + '_' + str(uniques[i])
        dataframe.loc[dataframe[expansion_field] == uniques[i], field_name] = 1
        dataframe[field_name].fillna(0, inplace=True)
    if replace:
        dataframe.drop([expansion_field], inplace=True, axis=1)
    return dataframe


