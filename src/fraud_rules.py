from src.data_processing import IN_OUT, HASH_TYPE, unpack_transaction
from src.redis_helper import get_transaction
import bisect 

""" 
Value: Sorted set {"amount|recv_time|nameOrig”}
"""

def filter_transactions_by_time(trans: list[tuple[bytes, float]], cutoff_time: int) -> int:
    """ 
    Filter transactions by time and return the start index (binary search to find the start index)
    """
    return bisect.bisect_left(trans, cutoff_time, key=lambda x: x[-1])

def small_to_large(recv_time: float, out_trans: list[tuple[bytes, float]]) -> bool:
    """ 
    Detect sequences of ≥3 consecutive small transactions
    (<100) followed by a large transfer(>10,000) between the
    same accounts within 10 minutes
    
    filter transactions of last 10 mins
    always get data from redis by score 
    userid, amount 
    check OUT
    """
    threshold_small_txn = 3
    threshold_small_amt = 100
    threshold_large_amt = 10000
    cutoff_time = recv_time - 600 # 10mins -> 600seconds
    start_idx = filter_transactions_by_time(out_trans, cutoff_time)
    count_small = 0
    for i in range(start_idx, len(out_trans)):
        amount, _, _ = unpack_transaction(out_trans[i][0])
        if amount < threshold_small_amt:
            count_small += 1
        if count_small > threshold_small_txn and amount > threshold_large_amt:
            return True
    return False

def money_mule(recv_time: float, out_trans: list[tuple[bytes, float]]) -> bool:
    """ 
    Identify users sending transactions to ≥5 distinct recipients within 2 minutes
    
    filter transactions of last 2 mins
    sum nb of distinct recipients 
    
    select count(DISTINCT recipient)
    from data2mins 
    
    always get data from redis by score 
    
    userid, recipient 
    check OUT
    """
    threshold_dest = 5
    cutoff_time = recv_time - 120 # 2mins -> 120seconds
    start_idx = filter_transactions_by_time(out_trans, cutoff_time)
    count_dest = 0
    dests = set()
    for i in range(start_idx, len(out_trans)):
        _, _, dest = unpack_transaction(out_trans[i][0])
        if dest not in dests:
            count_dest += 1
        if count_dest >= threshold_dest:
            return True
    return False

def pump_and_dump(in_trans: list[dict], out_trans: list[dict]) -> bool:
    """ 
    Detect accounts receiving high credits (>$50k) then transferring out ≥95% within 24 hours
    
    Check all the transactions
    If one >$50 
    then check all the transactions after this transaction 
    take the sum and check if >=95% are transferred out 
    
    user, in & out, amount
    check OUT
    return True if is pump and dump 
    """
    threshold_pump = 50000    
    sum_in = sum(unpack_transaction(trans[0])[0] for trans in in_trans)
    # check the current transaction too 
    sum_out = sum(unpack_transaction(trans[0])[0] for trans in out_trans)
    return sum_in >= threshold_pump and sum_out >= sum_in * 0.95

def is_type_in(tx: dict) -> bool:
    """ 
    Return True(1) if type is cash in
    """
    return tx['type_CASH_IN'] == 1

def check_rules(trans: dict) -> bool:    
    """ 
    Rule-based part with redis
    Cash in always allowed, no fraud transaction
    It is checked by AML (Anti-Money Laundering) Engine
    And bank account limits 
    Only cash out is problematic
    return True if it is a fraud
    """
    id = f"{IN_OUT[0]}{trans['nameOrig']}"
    out_trans = get_transaction(id)
    if small_to_large(trans['recv_time'], out_trans) or money_mule(trans['recv_time'], out_trans):
        return True
    in_trans = get_transaction(f"{IN_OUT[-1]}{trans['nameOrig']}")
    return pump_and_dump(in_trans, out_trans)