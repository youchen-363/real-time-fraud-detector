import pandas as pd 

def main():
    tmp = {"amount": 96994.67, "nameOrig": "C608854176", "isFraud": 0, "type_CASH_IN": 0, "type_CASH_OUT": 1, "type_DEBIT": 0, "type_PAYMENT": 0, "type_TRANSFER": 0, "orig_tx_count_same_step": 1, "orig_tx_cumcount": 1, "orig_prev_amount": 0.0, "orig_amount_ratio": 0.0, "steps_since_last_tx": -1.0, "dest_tx_cumcount": 4, "dest_prev_amount": 32219.37, "dest_amount_ratio": 3.0103524571567615, "dest_steps_since_last": 0.0, "pair_tx_cumcount": 0, "is_new_dest": 1, "pair_total_amount": 96994.67, "hour_of_day": 8, "day": 0, "is_night": 0, "recv_time": 1774978171.4556913}
    df = pd.DataFrame([tmp])
    print(df.columns)     

if __name__ == "__main__":
    main()