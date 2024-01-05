import pandas as pd
from joblib import dump
import json

def save_model(model, model_path):
    dump(model, model_path)

def save_predictions(predictions, y_test, predictions_path):
    predictions_df = pd.DataFrame({'Actual': y_test, 'Predicted': predictions})
    predictions_df.to_csv(predictions_path, index=False)

def save_run_info(run_info, run_info_path):
    with open(run_info_path, 'w') as f:
        json.dump(run_info, f)

