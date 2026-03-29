import pandas as pd 
from ml_helper import ml 
from fraud_rules import rule_based
import matplotlib.pyplot as plt 
import seaborn as sns 

def plot_data():
    df = pd.read_csv("data/data.csv")    
    print(df.columns.tolist())
    print(df.iloc[0])
    total = len(df)
    fraud = len(df[df['isFraud'] == 1]) 
    imbalance = (fraud / total) * 100
    fraud_df = df[df['isFraud'] == 1]
    plt.figure(figsize=(8, 6))
    sns.countplot(x='isFraud', data=df, palette='Set2') 
    plt.yscale('log')
    plt.title(f'Valid vs Fraud, Fraud = {imbalance}%', fontsize=14)
    plt.xlabel('0 = Valid, 1 = Fraud', fontsize=12)
    plt.ylabel('Number of Transactions (log)', fontsize=12)
    plt.tight_layout()
    plt.savefig('class_imbalance_log.png', dpi=300)
    
    plt.figure(figsize=(10, 6))
    # Filter to only show the types that actually have fraud
    sns.countplot(x='type', data=fraud_df, palette='Reds_r', order=fraud_df['type'].value_counts().index)
    plt.title('Where does the Fraud happen?\n(Count of Fraud by Transaction Type)', fontsize=14)
    plt.xlabel('Transaction Type', fontsize=12)
    plt.ylabel('Number of Fraudulent Transactions', fontsize=12)
    plt.tight_layout()
    plt.savefig('fraud_by_type.png', dpi=300)
    