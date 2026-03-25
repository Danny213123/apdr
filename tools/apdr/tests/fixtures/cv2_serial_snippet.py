import cv2
import serial
import numpy as np
from PIL import Image

cap = cv2.VideoCapture(0)
ret, frame = cap.read()
img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
port = serial.Serial('/dev/ttyUSB0', 9600)
data = np.array(img)
