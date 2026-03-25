import cv2
import numpy as np
from PIL import Image

img = cv2.imread("photo.jpg")
rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
pil_img = Image.fromarray(rgb)
arr = np.array(pil_img)
