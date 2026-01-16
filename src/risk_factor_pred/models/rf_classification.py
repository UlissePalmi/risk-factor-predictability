from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
import pandas as pd

def create_labels(df, prediction_col):
    """
    Create 5 return-based classes using quintiles of `prediction_col`.

    Adds a categorical `prediction` column and returns the updated dataframe
    along with the ordered class labels.
    """
    labels = [
        "very_negative",
        "negative",
        "flat",
        "positive",
        "very_positive"
    ]

    df["prediction"] = pd.qcut(
        df[prediction_col],
        q=5,
        labels=labels
    )
    return df, labels


def rf_cat(X, y, labels):
    """
    Train and evaluate a Random Forest classifier for multi-class return labels.

    Splits the data into train/test sets, fits the model, prints performance
    metrics, and reports feature importances.
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=42,
        stratify=y  # important: preserve class proportions
    )

    # Random Forest classifier
    rf_clf = RandomForestClassifier(
        n_estimators=300,
        max_depth=6,
        min_samples_leaf=200,
        n_jobs=-1,
        random_state=42
    )

    rf_clf.fit(X_train, y_train)

    # Predictions on test set
    y_pred = rf_clf.predict(X_test)
    y_proba = rf_clf.predict_proba(X_test)  # class probabilities if you need them

    # Basic evaluation
    print("Random Forest Classifier performance:")
    print(classification_report(y_test, y_pred, target_names=labels))

    print("Confusion matrix (rows=true, cols=pred):")
    print(confusion_matrix(y_test, y_pred))

    feat_imp = pd.Series(
        rf_clf.feature_importances_,
        index=X.columns
    ).sort_values(ascending=False)

    print("\nFeature importances:")
    print(feat_imp)
