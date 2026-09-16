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
