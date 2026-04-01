import struct 

HEADER_STRUCT = struct.Struct('<dd')

HASH_TYPE = {
    "CASH-IN": 0,
    "CASH-OUT": 1,
    "DEBIT": 2,
    "PAYMENT": 3,
    "TRANSFER": 4
}

HASH_TYPE_REV = {
    0: "CASH-IN",
    1: "CASH-OUT",
    2: "DEBIT",
    3: "PAYMENT",
    4: "TRANSFER"
}

IN_OUT = ["u:out:", "u:in:"]

# class Transaction:
    
#     def __init__(self, step: int, trans_type: int, amount: float, nameOrig: str, oldbalanceOrig: int, newbalanceOrig: int, nameDest: str, oldbalanceDest: int, newbalaceDest: int, recv_time: float = None):
#         # self.step = step 
#         self.trans_type = trans_type
#         self.amount = amount
#         self.nameOrig = nameOrig
#         # self.oldbalanceOrig = oldbalanceOrig
#         # self.newbalanceOrig = newbalanceOrig 
#         self.nameDest = nameDest
#         # self.oldbalanceDest = oldbalanceDest
#         # self.newbalanceDest = newbalaceDest
#         self.recv_time = recv_time if recv_time is not None else time.time()
        
#         self.orig_tx_count_same_step = 0
#         self.orig_tx_cumcount = 0
#         self.orig_prev_amount = 0
#         self.orig_prev_step = -1
#         self.orig_amount_ratio = 0
#         self.steps_since_last_tx = -1
#         self.dest_tx_cumcount = 0
#         self.dest_prev_amount = 0
#         self.dest_amount_ratio = 0
#         self.dest_prev_step = -1
#         self.dest_steps_since_last = 0
#         self.pair_tx_cumcount = 0
#         self.is_new_dest = 0
#         self.pair_total_amount = 0
        
#         self.hour_of_day = step % 24
#         self.day = step / 24
#         self.is_night = step < 6 or step >= 22

#     def set_orig_features(self, orig_tx_count_same_step: int, orig_tx_cumcount: int, orig_prev_amount: float = 0, orig_amount_ratio: float = 0, orig_prev_step = -1, steps_since_last_tx: float = -1):
#         self.orig_tx_count_same_step = orig_tx_count_same_step
#         self.orig_tx_cumcount = orig_tx_cumcount
#         self.orig_prev_amount = orig_prev_amount
#         self.orig_amount_ratio = orig_amount_ratio
#         self.orig_prev_step = orig_prev_step
#         self.steps_since_last_tx = steps_since_last_tx
    
#     def set_dest_features(self, dest_tx_cumcount: int, dest_prev_amount: float = 0, dest_amount_ratio: float = 0, dest_prev_step: float = -1, dest_steps_since_last: float = -1):
#         self.dest_tx_cumcount = dest_tx_cumcount
#         self.dest_prev_amount = dest_prev_amount
#         self.dest_amount_ratio = dest_amount_ratio
#         self.dest_prev_step = dest_prev_step
#         self.dest_steps_since_last = dest_steps_since_last
        
#     def set_pair_features(self, pair_tx_cumcount: int, is_new_dest: int, pair_total_amount: float):
#         self.pair_tx_cumcount = pair_tx_cumcount
#         self.is_new_dest = is_new_dest
#         self.pair_total_amount = pair_total_amount 
    
def pack_transaction(trans: dict) -> bytes:
    """
    Format data into a string in format 'amount|type|recipient|old_balance|new_balance' 
    1. Pack amount
    2. append recipient str
    """
    bin_header = HEADER_STRUCT.pack(trans['amount'], trans['recv_time'])
    # store amount and type in struct then merge dest with the struct
    # avoid defining the size of string in struct since there is no fixed size of user id
    return bin_header + trans['nameDest'].encode('utf-8')

def unpack_transaction(b_trans: bytes) -> tuple[float, float, str]:
    """ 
    Unpack transaction into amount, type, recipient_id
    """
    amount, recv_time = HEADER_STRUCT.unpack(b_trans[:16])
    dest = b_trans[16:].decode('utf-8')
    return amount, recv_time, dest
