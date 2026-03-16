import urllib, StringIO
from PIL import Image

query = urllib.urlencode({"x": 1})
stream = StringIO.StringIO()
image = Image.new("RGB", (1, 1))
