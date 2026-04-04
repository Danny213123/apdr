"""Image processing with skimage + OpenCV.
skimage maps to scikit-image on PyPI. cv2 to opencv-python-headless."""
import skimage
from skimage import data
from skimage.filters import threshold_otsu
from skimage.segmentation import clear_border
from skimage.measure import label, regionprops
from skimage.morphology import closing, square
from skimage.color import label2rgb
import cv2
import numpy as np
import sys
