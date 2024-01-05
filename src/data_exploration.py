import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def plot_feature_distributions(features_df):
    """
    Plots distributions of each feature in the DataFrame.
    """
    num_features = len(features_df.columns) - 1  # excluding target column
    cols = 3  # Number of columns in subplot
    rows = (num_features + cols - 1) // cols  # Calculate the required number of rows

    fig, axes = plt.subplots(nrows=rows, ncols=cols, figsize=(15, rows * 3))
    axes = axes.flatten()

    for i, col in enumerate(features_df.columns[:-1]):  # excluding target column
        sns.histplot(features_df[col], ax=axes[i], kde=True)
        axes[i].set_title(col)
        axes[i].set_ylabel('Count')

    # Hide any unused subplots
    for i in range(num_features, len(axes)):
        axes[i].set_visible(False)

    plt.tight_layout()
    plt.show()


def plot_feature_correlations(features_df):
    """
    Plots the correlation matrix as a heatmap.
    """
    correlation_matrix = features_df.corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
    plt.title('Feature Correlations')
    plt.show()

def analyze_data(features_df):
    """
    Runs the data analysis functions.
    """
    print("Feature Distributions:")
    plot_feature_distributions(features_df)

    print("Feature Correlations:")
    plot_feature_correlations(features_df)

