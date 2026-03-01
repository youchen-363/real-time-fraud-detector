import sys

ENDIEN_TYPE = '<' if sys.byteorder == "little" else ">"

HASH_TYPE = {
    "CASH-IN": 0,
    "CASH-OUT": 1,
    "DEBIT": 2,
    "TRANSFER": 3
}

HASH_TYPE_REV = {
    0: "CASH-IN",
    1: "CASH-OUT",
    2: "DEBIT",
    3: "TRANSFER"
}

FORMAT = ["u:out:", "u:in:"]

class Transaction:
    
    def __init__(self, step: int, trans_type: int, amount: float, nameOrig: str, oldbalanceOrig: int, newbalanceOrig: int, nameDest: str, oldbalanceDest: int, newbalaceDest: int):
        self.step = step 
        self.trans_type = trans_type
        self.amount = amount
        self.nameOrig = nameOrig
        self.oldbalanceOrig = oldbalanceOrig
        self.newbalanceOrig = newbalanceOrig 
        self.nameDst = nameDest
        self.oldbalanceDest = oldbalanceDest
        self.newbalanceDest = newbalaceDest



def filter():
    """
    Optional function 
    Clean data by filling NaN cell or remove the entire row 
    """
    pass

def to_bytes(transaction: Transaction) -> str:
    """
    Format data into a string in format 'amount|type|recipient|old_balance|new_balance'
    """
    
    pass
