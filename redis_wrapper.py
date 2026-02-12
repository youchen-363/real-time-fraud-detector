import redis.asyncio as rd


class RedisClient:
    
    # Singleton connection
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, host='localhost', port=6379, decode_responses=True):
        self._instance = redis.Redis(host=host, port=port, decode_responses=decode_responses)

    def store(self, key, val):
        self._instance.set(key, val)
    
    def retrieve(self, key):
        return self._instance.get(key)

if __name__ == "__main__":
    r = RedisClient('localhost', 6379, True)
    print(r.retrieve('foo'))
    print(r.retrieve('foo'))
