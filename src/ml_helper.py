import joblib 
from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
    
def load_random_forest() -> RandomForestClassifier:
    """ 
    Load random forest classifier 
    """
    return joblib.load("./model/random_forest.joblib")

def load_xgboost() -> XGBClassifier:
    """ 
    Load XGBoost classifier
    """
    model = XGBClassifier()
    model.load_model("./model/xgboost.json")
    return model