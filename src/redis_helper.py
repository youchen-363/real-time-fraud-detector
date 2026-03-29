# import redis.asyncio as rd
import redis
from data_processing import Transaction, pack_transaction, IN_OUT
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=False)

def add_transaction(id: str, trans: Transaction) -> bool:
    t_byte = pack_transaction(trans)
    res = r.zadd(id, {t_byte: time.time()})
    return res

def get_transaction(id: str) -> list[tuple[bytes, float]]:
    return r.zrange(
        id, 
        byscore=True, 
        min=time.time()-86400, 
        max="+inf",
        withscores=True
        )
    
if __name__ == "__main__":
    r = redis.client('localhost', 6379, True)
    print(r.retrieve('foo'))
    print(r.retrieve('foo'))
