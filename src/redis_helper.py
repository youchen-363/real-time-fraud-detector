import redis
from src.data_processing import pack_transaction, IN_OUT
import time
import numpy as np 

r = redis.Redis(host='localhost', port=6379, decode_responses=False)

def add_transaction(id: str, trans: dict) -> bool:
    t_byte = pack_transaction(trans)
    res = r.zadd(id, {t_byte: time.time()})
    return res

def get_transaction(id: str) -> list[tuple[bytes, float]]:
    return r.zrange(
        id, 
        byscore=True, 
        start=time.time()-86400, 
        end="+inf",
        withscores=True
        )
 
"""
def add_transaction_feature(tx: dict) -> bool:
    # TODO: add a transaction feature
    
    return False

def process_transaction_state(tx: dict) -> dict:
    # 1. Fetches the running totals from Redis.
    # 2. Calculates the new features.
    # 3. Updates Redis for the next transaction.
    step = int(tx['step'])
    amount = float(tx['amount'])
    nameOrig = tx['nameOrig']
    nameDest = tx['nameDest']

    # Define our 3 Redis Keys
    orig_key = f"orig:{nameOrig}"
    dest_key = f"dest:{nameDest}"
    pair_key = f"pair:{nameOrig}:{nameDest}"

    # ==========================================
    # 1. THE FETCH (Pipeline for Latency)
    # ==========================================
    # We use a pipeline to fetch all 3 keys in ONE single network trip.
    pipe = r.pipeline()
    pipe.hgetall(orig_key)
    pipe.hgetall(dest_key)
    pipe.hgetall(pair_key)
    orig_state, dest_state, pair_state = pipe.execute()

    # ==========================================
    # 2. FEATURE ENGINEERING (The Pandas Translation)
    # ==========================================
    # We use .get(key, default) to perfectly mimic your fillna() logic!
    
    # --- Originator Features ---
    orig_tx_cumcount = int(orig_state.get('cumcount', 0)) + 1
    orig_prev_amount = float(orig_state.get('prev_amount', 0.0))
    orig_prev_step = int(orig_state.get('prev_step', -1))
    
    orig_amount_ratio = amount / (orig_prev_amount + 1)
    steps_since_last_tx = (step - orig_prev_step) if orig_prev_step != -1 else -1

    # Same-step velocity: If the last step matches this step, increment the counter.
    if orig_prev_step == step:
        orig_tx_count_same_step = int(orig_state.get('same_step_count', 1)) + 1
    else:
        orig_tx_count_same_step = 1

    # --- Destination Features ---
    dest_tx_cumcount = int(dest_state.get('cumcount', 0)) + 1
    dest_prev_amount = float(dest_state.get('prev_amount', 0.0))
    dest_prev_step = int(dest_state.get('prev_step', -1))
    
    dest_amount_ratio = amount / (dest_prev_amount + 1)
    dest_steps_since_last = (step - dest_prev_step) if dest_prev_step != -1 else -1

    # --- Pair Features ---
    pair_tx_cumcount = int(pair_state.get('cumcount', 0))
    is_new_dest = 1 if pair_tx_cumcount == 0 else 0
    pair_total_amount = float(pair_state.get('total_amount', 0.0)) + amount

    # Compile the final dictionary for the model
    features = {
        'amount': amount,
        'orig_tx_count_same_step': orig_tx_count_same_step,
        'orig_tx_cumcount': orig_tx_cumcount,
        'orig_prev_amount': orig_prev_amount,
        'orig_amount_ratio': orig_amount_ratio,
        'steps_since_last_tx': steps_since_last_tx,
        'dest_tx_cumcount': dest_tx_cumcount,
        'dest_prev_amount': dest_prev_amount,
        'dest_amount_ratio': dest_amount_ratio,
        'dest_steps_since_last': dest_steps_since_last,
        'pair_tx_cumcount': pair_tx_cumcount,
        'is_new_dest': is_new_dest,
        'pair_total_amount': pair_total_amount,
        'hour_of_day': hour_of_day,
        'day': day,
        'is_night': is_night
    }

    # ==========================================
    # 3. UPDATE STATE IN REDIS
    # ==========================================
    # Now we overwrite the old state with the current transaction data
    update_pipe = r.pipeline()
    update_pipe.hset(orig_key, mapping={
        'cumcount': orig_tx_cumcount,
        'prev_amount': amount,
        'prev_step': step,
        'same_step_count': orig_tx_count_same_step
    })
    update_pipe.hset(dest_key, mapping={
        'cumcount': dest_tx_cumcount,
        'prev_amount': amount,
        'prev_step': step
    })
    update_pipe.hset(pair_key, mapping={
        'cumcount': pair_tx_cumcount + 1,
        'total_amount': pair_total_amount
    })
    update_pipe.execute()

    return features
"""    
if __name__ == "__main__":
    r = redis.client('localhost', 6379, True)
    print(r.retrieve('foo'))
    print(r.retrieve('foo'))
