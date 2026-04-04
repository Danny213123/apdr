"""OpenCV snippet that does NOT use GUI functions — should resolve to
opencv-python-headless (pre-built wheels, no system deps)."""
import cv2
import numpy as np

img = cv2.imread("input.jpg")
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
resized = cv2.resize(gray, (224, 224))
edges = cv2.Canny(resized, 100, 200)
cv2.imwrite("output.jpg", edges)
