"""
Train churn prediction models and persist the best one to disk.

Trains:
  - Logistic Regression
  - Random Forest
  - Gradient Boosting

Evaluates each with Accuracy, Precision, Recall, F1, ROC-AUC and a
confusion matrix, then selects the best model primarily by F1-score and
ROC-AUC (not raw accuracy), per project requirements.

Run directly:
    python -m src.train_models
"""
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, confusion_matrix, f1_score, precision_score,
    recall_score, roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config
from src.feature_engineering import (
    CATEGORICAL_FEATURES, NUMERIC_FEATURES, engineer_features,
)


def load_training_data() -> pd.DataFrame:
    df = pd.read_csv(config.RAW_DATA_PATH)
    if "churn" not in df.columns:
        raise ValueError(
            f"Training data at {config.RAW_DATA_PATH} must contain a 'churn' column "
            "(0/1 or Yes/No)."
        )
    if df["churn"].dtype == object:
        df["churn"] = df["churn"].str.strip().str.lower().map({"yes": 1, "no": 0})
    df = engineer_features(df)
    return df


def build_preprocessor() -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), NUMERIC_FEATURES),
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ]
    )


def build_candidate_models() -> dict:
    return {
        "logistic_regression": LogisticRegression(max_iter=1000, class_weight="balanced"),
        "random_forest": RandomForestClassifier(
            n_estimators=300, max_depth=10, random_state=42, class_weight="balanced"
        ),
        "gradient_boosting": GradientBoostingClassifier(
            n_estimators=200, max_depth=3, learning_rate=0.08, random_state=42
        ),
    }


def evaluate(y_true, y_pred, y_proba) -> dict:
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1_score": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, y_proba)),
        "confusion_matrix": confusion_matrix(y_true, y_pred).tolist(),
    }


def main():
    print("Loading and preparing training data...")
    df = load_training_data()
    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    y = df["churn"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    preprocessor = build_preprocessor()
    candidates = build_candidate_models()

    results = {}
    fitted_pipelines = {}

    for name, model in candidates.items():
        print(f"\nTraining {name}...")
        pipe = Pipeline([("preprocessor", preprocessor), ("model", model)])
        pipe.fit(X_train, y_train)

        y_pred = pipe.predict(X_test)
        y_proba = pipe.predict_proba(X_test)[:, 1]

        metrics = evaluate(y_test, y_pred, y_proba)
        results[name] = metrics
        fitted_pipelines[name] = pipe

        print(
            f"  Accuracy={metrics['accuracy']:.3f}  Precision={metrics['precision']:.3f}  "
            f"Recall={metrics['recall']:.3f}  F1={metrics['f1_score']:.3f}  "
            f"ROC-AUC={metrics['roc_auc']:.3f}"
        )

    # Select best model primarily by F1-score and ROC-AUC (average of the two),
    # not by accuracy alone, per project requirements.
    def combined_score(m):
        return 0.5 * m["f1_score"] + 0.5 * m["roc_auc"]

    best_name = max(results, key=lambda n: combined_score(results[n]))
    best_pipeline = fitted_pipelines[best_name]
    best_metrics = results[best_name]

    print(f"\nBest model selected: {best_name} "
          f"(F1={best_metrics['f1_score']:.3f}, ROC-AUC={best_metrics['roc_auc']:.3f})")

    Path(config.MODEL_PATH).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(best_pipeline, config.MODEL_PATH)
    print(f"Saved trained model pipeline -> {config.MODEL_PATH}")

    metadata = {
        "best_model": best_name,
        "all_results": results,
        "feature_columns": NUMERIC_FEATURES + CATEGORICAL_FEATURES,
        "training_rows": int(len(df)),
        "churn_rate": float(y.mean()),
    }
    with open(config.MODEL_METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved model metadata -> {config.MODEL_METADATA_PATH}")


if __name__ == "__main__":
    main()
