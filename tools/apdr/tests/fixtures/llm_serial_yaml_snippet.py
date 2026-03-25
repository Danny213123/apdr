import serial
import yaml

config = yaml.safe_load(open("config.yml"))
port = serial.Serial(config["port"], config["baud"])
data = port.readline()
print(yaml.dump({"raw": data.decode()}))
