## Feature Store Example

This project demonstrates a basic machine learning pipeline utilizing a feature store for managing data, model training and evaluation, and tracking ML experiments.

# Setup
1. Create and activate a Python 3.6+ virtual environment:

```
python3 -m venv venv
source venv/bin/activate

```

2. Install requirements:

```
pip install -r requirements.txt
```

3. Install the project in editable mode:

```
pip install -e .

```

## Contents

* data_exploration.py: Functions for analyzing and visualizing the features data

* data_generation.py: Simulates getting features data and outputs to a CSV

* model_training.py: Trains a model on the features data

* model_saving.py: Saves trained models and outputs like predictions

* main.py: Orchestrates the ML pipeline steps

* src/: Source code

* feature_store/: Directory to store the features data
* model_runs/: Directory to store trained models and experiment outputs

## Running

1. Activate the virtual env:

```
source venv/bin/activate

```

2. Run main module:

```
python src/main.py
```

3. Model outputs in model_runs/

4. Deactivate virtual env when done:
```
deactivate
```
