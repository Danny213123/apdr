"""pylearn2 and theano — both are defunct/archived and NOT installable from PyPI."""
from pylearn2.datasets.dataset import Dataset
from pylearn2.datasets.dense_design_matrix import DenseDesignMatrix
from pylearn2.utils.iteration import SequentialSubsetIterator
import functools
import logging
import numpy
import warnings
from pylearn2.space import CompositeSpace, Conv2DSpace, VectorSpace
import scipy.sparse
import theano
