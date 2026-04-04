"""Financial analysis script using Quandl (now Nasdaq Data Link).
The import 'Quandl' maps to the 'quandl' package on PyPI (lowercase)."""
import os
import csv
import numpy as np
import scipy as spy
import sklearn as kit
import pandas
import statsmodels.api as sm
import matplotlib.pyplot as plot
from Quandl import get
from statsmodels.sandbox.regression.predstd import wls_prediction_std
