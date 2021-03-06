import pandas as pd
import numpy as np
from core.data import frameformatting
import string
import math


def bin_generator(element_count):
    """
    function to build an alpha-based bin naming convention
    :param element_count: number of bin names to generate
    :return: a list of first uppercase, then lowercase names.
    """
    bin_list = list(string.ascii_uppercase + string.ascii_lowercase)
    return bin_list[:element_count]


def unique_list_gen(check_list):
    """
    Returns a unique list since pd.cut will fail in binning if the bins are not unique.
    :param check_list: list of int or float
    :return: a unique list
    """
    unique_list = []
    for a in check_list:
        if a not in unique_list:
            unique_list.append(a)
    return unique_list


def auto_bins(dataframe, field, n_bins):
    """
    Function for calculating bin limits based on percentiles of the data series of the frame.
    :param dataframe: source dataframe
    :param field: field of interest
    :param n_bins: the number of bins that are going to be created.
    :return: full range of bins (returned count will be n_bins + 1 to capture full ranges)
    """
    df_type = dataframe[field].dtype
    if df_type == float or df_type == int:
        series = dataframe[field]
        bins = []
        quants = math.floor(100 / n_bins)
        for i in range(n_bins):
            quan = int(quants * i)
            bins.append(np.percentile(series, quan))
        bins.append(dataframe[field].max())
        return unique_list_gen(bins)
    else:
        raise TypeError('Frame field must be either int or float to calculate bin ranges')


def binning(dataframe, source_field, output_field, label_list, bin_define=None, del_check=False, bin_scale='lin'):
    """
    Takes a source field and creates a new field that arranges
    :param dataframe: source dataframe
    :param source_field: the continuous int or float field to be binned
    :param output_field: name of the output field for the binned data
    :param label_list: the binning categories
    :param bin_define: (optional) list of ranges.
    :param del_check: bool check for deleting the original continuous value field (source_field)
    :param bin_scale: linear scaling (lin) log scaling(log) for data distribution
    :return: the original dataframe with the added field.
    """
    source_dtype = dataframe[source_field].dtype
    if source_dtype == float or source_dtype == int:
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
            if bin_scale == 'lin':
                bin_space = np.linspace(start=dataframe[source_field].min(), stop=dataframe[source_field].max(),
                                        num=bin_count)
            elif bin_scale == 'auto':
                bin_space = auto_bins(dataframe, source_field, bin_count - 1)
            else:
                bin_space = np.logspace(start=np.log10(dataframe[source_field].min()),
                                        stop=np.log10(dataframe[source_field].max()), num=bin_count)
            dataframe[output_field] = pd.cut(x=dataframe[source_field], bins=bin_space, labels=label_list)
        if del_check:
            dataframe.drop(source_field, axis=1, inplace=True)
        return dataframe
    else:
        raise TypeError('Binning can only be performed on int or float.  Source (%s) dtype: %s.' %
                        (source_field, source_dtype))


def bin_expansion(dataframe, source_field, expansion_bins, bin_ranges=None, source_del=True, bin_scale='lin'):
    """
    Roll up both binning and basis expansion in a single function call
    :param dataframe: source dataframe object
    :param source_field: input field to binning
    :param expansion_bins: a list of bin names to use for binning and basis expansion
    :param bin_ranges: list of int / float for setting the binning ranges (optional)
    :param source_del: bool check to delete the source field and the binning field.
    :param bin_scale: binning scaling (lin / log)
    :return: data frame with new frames created.
    """
    bin_field = source_field + '_bin'
    binning(dataframe, source_field, bin_field, expansion_bins, bin_ranges, del_check=source_del, bin_scale=bin_scale)
    frameformatting.basis_expansion(dataframe, bin_field, replace=source_del)
    return dataframe


def deltadate(dataframe, output_field, datetime_newer, datetime_older, date_diff_type):
    """
    return a new field based on the difference between two series
    :param dataframe: the dataframe object for field comparison / creation
    :param datetime_older: series or static val that's older (e.g. dataframe['START'])
    :param datetime_newer: series or static val that's newer (e.g. datetime.today())
    :param output_field: the output to-write field in the dataframe
    :param date_diff_type: the date object type based on timedelta[x] format (day = 'D')
    :return: the original dataframe with the new field appended.

    reference: http://docs.scipy.org/doc/numpy-dev/reference/arrays.datetime.html
    """
    date_diff_series = datetime_newer - datetime_older
    dd_type = "timedelta64[%s]" % date_diff_type
    dataframe[output_field] = date_diff_series.astype(dd_type).astype(int)
    return dataframe




