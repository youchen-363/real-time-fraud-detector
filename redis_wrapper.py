# import redis.asyncio as rd
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

res6 = r.zrange("racer_scores", 0, -1, withscores=True)



if __name__ == "__main__":
    r = redis.client('localhost', 6379, True)
    print(r.retrieve('foo'))
    print(r.retrieve('foo'))
