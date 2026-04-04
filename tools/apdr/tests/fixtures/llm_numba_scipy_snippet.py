"""Numerical computing with numba, scipy, sklearn — version-sensitive.
These packages must have compatible versions (especially numpy)."""
import numpy as np
import scipy.sparse as sp
import numba
from sklearn.base import BaseEstimator
from sklearn.utils import check_random_state
from sklearn.utils.extmath import safe_sparse_dot

@numba.jit(nopython=True)
def fast_dot(a, b):
    return np.dot(a, b)
