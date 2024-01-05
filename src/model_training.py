from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

def train_and_evaluate(features_df):
    X_train, X_test, y_train, y_test = train_test_split(features_df.iloc[:, :-1], features_df['target'], test_size=0.2, random_state=42)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    class_report = classification_report(y_test, predictions)

    return model, X_test, y_test, predictions, accuracy, class_report

