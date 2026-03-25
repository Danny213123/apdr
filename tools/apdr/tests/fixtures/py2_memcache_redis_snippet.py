from sqlalchemy import create_engine
import redis
import memcache
import requests

engine = create_engine('mysql://user:pass@localhost/db')
r = redis.StrictRedis(host='localhost', port=6379)
mc = memcache.Client(["127.0.0.1:11211"])
print r.get("key")
