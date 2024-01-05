import pandas as pd
from sklearn.datasets import make_classification

def get_data():
    X, y = make_classification(n_samples=100, n_features=20, n_informative=2, n_redundant=10, random_state=42)
    features_df = pd.DataFrame(X, columns=[f'feature_{i}' for i in range(X.shape[1])])
    features_df['target'] = y
    return features_df

