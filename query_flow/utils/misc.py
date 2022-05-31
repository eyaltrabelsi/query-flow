import numpy as np


def calc_precentage(series, comsum_series):
    return series / comsum_series * 100


def calc_ratio(df, column_a, column_b):
    def calc_row(this, other):
        if this == other:
            return 1
        elif this == 0 or other == 0:
            return np.inf
        else:
            return max([this, other]) / min([this, other])

    return df.apply(lambda x: calc_row(x[column_a], x[column_b]), axis=1)


def listify(val):
    if type(val) in [str, int, float, dict]:
        return [val]
    return val
