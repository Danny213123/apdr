"""Face detection with OpenCV + dlib + imutils.
cv2 should map to opencv-python-headless (no GUI needed, avoids system deps).
dlib needs cmake — pin to version with pre-built wheels if possible."""
import argparse
import imutils
import dlib
import cv2

detector = dlib.get_frontal_face_detector()
img = cv2.imread("face.jpg")
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
rects = detector(gray, 1)
