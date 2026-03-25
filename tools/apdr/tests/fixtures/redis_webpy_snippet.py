import redis
import web

SESSION = 'SESSION:'

class RedisStore(web.session.Store):
    """Store for saving a session in redis:
    import rediswebpy
    session = web.session.Session(app, rediswebpy.RedisStore(), initializer={'count': 0})
    """
    def __init__(self, ip='localhost', port=6379, db=0, initial_flush=False):
        self.redis_server = redis.Redis(ip, port, db)
        if initial_flush:
            self.redis_server.flushdb()

    def __contains__(self, key):
        return bool(self.redis_server.get(SESSION + key))

    def __getitem__(self, key):
        data = self.redis_server.get(SESSION + key)
        if data:
            self.redis_server.expire(SESSION + key,
                                     web.webapi.config.session_parameters.timeout)
            return self.decode(data)
        else:
            raise KeyError

    def __setitem__(self, key, value):
        self.redis_server.set(SESSION + key, self.encode(value))

    def __delitem__(self, key):
        self.redis_server.delete(SESSION + key)
