import pandas as pd
import numpy as np


def binning(dataframe, source_field, output_field, label_list, bin_define=None, del_check=False):
    """
    Takes a source field and creates a new field that arranges
    :param dataframe: source dataframe
    :param source_field: the continuous int or float field to be binned
    :param output_field: name of the output field for the binned data
    :param label_list: the binning categories
    :param bin_define: (optional) list of ranges.
    :param del_check: bool check for deleting the original continuous value field (source_field)
    :return: the original dataframe with the added field.
    """
    source_dtype = dataframe[source_field].dtype
    if source_dtype == 'float64' or source_dtype == 'int64':
        if bin_define:
            if len(bin_define) != len(label_list) + 1:
                raise ValueError('Defined numeric bin level count must be one higher than labels count.')
            else:
                frame_max = dataframe[source_field].max()
                if bin_define[-1] != frame_max:
                    bin_define[-1] = frame_max
                dataframe[output_field] = pd.cut(x=dataframe[source_field], bins=bin_define, labels=label_list)
        else:
            bin_count = len(label_list) + 1
            bin_space = np.linspace(start=dataframe[source_field].min(), stop=dataframe[source_field].max(),
                                    num=bin_count)
            dataframe[output_field] = pd.cut(x=dataframe[source_field], bins=bin_space, labels=label_list)
        if del_check:
            dataframe.drop(source_field, axis=1, inplace=True)
        return dataframe
    else:
        raise TypeError('Binning can only be performed on int or float.  Source (%s) dtype: %s.' %
                        (source_field, source_dtype))
