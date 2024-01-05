import os
import data_generation
import model_training
import model_saving
import data_exploration

def main():
    # Create directories if they don't exist
    os.makedirs('feature_store', exist_ok=True)
    os.makedirs('model_runs', exist_ok=True)

    # Data Generation
    features_df = data_generation.get_data()
    feature_store_path = 'feature_store/features.csv'
    features_df.to_csv(feature_store_path, index=False)

    # Exploratory Data Analysis
    #data_exploration.analyze_data(features_df)

    # Model Training and Running
    model, X_test, y_test, predictions, accuracy, class_report = model_training.train_and_evaluate(features_df)

    # Model and Predictions Saving
    model_path = 'model_runs/random_forest.joblib'
    predictions_path = 'model_runs/predictions.csv'
    run_info_path = 'model_runs/run_info.json'

    model_saving.save_model(model, model_path)
    model_saving.save_predictions(predictions, y_test, predictions_path)

    run_info = {
        'model_type': 'RandomForestClassifier',
        'model_path': model_path,
        'feature_store_path': feature_store_path,
        'accuracy': accuracy,
        'predictions_path': predictions_path,
        'performance_metrics_path': 'model_runs/performance_metrics.txt'
    }
    model_saving.save_run_info(run_info, run_info_path)

    # Print Results
    print(f"Accuracy: {accuracy}")
    print("Classification Report:")
    print(class_report)
    print(f"Model and run info saved:\n{run_info}")

if __name__ == "__main__":
    main()

