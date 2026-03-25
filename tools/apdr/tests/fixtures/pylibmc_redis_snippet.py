import redis
import random
import pylibmc
import sys

r = redis.Redis(host='localhost', port=6389)
mc = pylibmc.Client(['localhost:11222'])

print 'Specify a test'
