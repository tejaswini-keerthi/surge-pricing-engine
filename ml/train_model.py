"""
ml/train_model.py
=================
Trains an XGBoost model to predict surge multipliers.

Training data comes from Cassandra's ml_features table.
The trained model is saved to ml/models/surge_predictor.pkl

Usage:
    python -m ml.train_model
"""

from __future__ import annotations

import joblib
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Any

from loguru import logger
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import xgboost as xgb

from config.settings import settings
from ml.feature_engineering import FEATURE_COLUMNS


# =============================================================================
# DATA LOADING
# =============================================================================

def load_training_data() -> pd.DataFrame:
    """
    Load historical surge data from Cassandra for model training.

    Returns:
        DataFrame with features and target (actual_multiplier)
    """
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    logger.info("Loading training data from Cassandra")

    try:
        cluster = Cluster(
            contact_points=[settings.cassandra.host],
            port=settings.cassandra.port,
            load_balancing_policy=DCAwareRoundRobinPolicy(
                local_dc="datacenter1"
            ),
        )
        session = cluster.connect(settings.cassandra.keyspace)

        rows = session.execute("""
            SELECT
                demand_last_1min, demand_last_5min, demand_last_15min,
                demand_spike_factor, available_drivers, supply_ratio,
                hour_of_day, day_of_week, is_weekend, is_rush_hour,
                is_late_night, is_lunch_hour, is_holiday,
                days_to_next_holiday, temperature, precipitation,
                weather_severity, historical_avg_demand,
                historical_avg_surge, is_special_event,
                event_demand_multiplier, actual_multiplier
            FROM ml_features
            LIMIT 100000
        """)

        df = pd.DataFrame(list(rows))
        cluster.shutdown()

        logger.info(f"Loaded {len(df):,} training samples")
        return df

    except Exception as e:
        logger.warning(f"Cassandra load failed: {e}. Using synthetic data.")
        return generate_synthetic_data()


def generate_synthetic_data(n_samples: int = 5000) -> pd.DataFrame:
    """
    Generate synthetic training data when Cassandra has insufficient records.

    Creates realistic surge pricing scenarios based on known patterns.

    Args:
        n_samples: Number of synthetic samples to generate

    Returns:
        DataFrame with synthetic features and targets
    """
    logger.info(f"Generating {n_samples:,} synthetic training samples")
    np.random.seed(42)

    data = {
        "demand_last_1min": np.random.poisson(50, n_samples),
        "demand_last_5min": np.random.poisson(200, n_samples),
        "demand_last_15min": np.random.poisson(500, n_samples),
        "demand_spike_factor": np.random.exponential(1.2, n_samples),
        "available_drivers": np.random.poisson(25, n_samples),
        "supply_ratio": np.random.exponential(0.8, n_samples),
        "hour_of_day": np.random.randint(0, 24, n_samples),
        "day_of_week": np.random.randint(0, 7, n_samples),
        "is_weekend": np.random.choice([True, False], n_samples, p=[0.28, 0.72]),
        "is_rush_hour": np.random.choice([True, False], n_samples, p=[0.2, 0.8]),
        "is_late_night": np.random.choice([True, False], n_samples, p=[0.15, 0.85]),
        "is_lunch_hour": np.random.choice([True, False], n_samples, p=[0.12, 0.88]),
        "is_holiday": np.random.choice([True, False], n_samples, p=[0.03, 0.97]),
        "days_to_next_holiday": np.random.randint(0, 8, n_samples),
        "temperature": np.random.normal(15, 10, n_samples),
        "precipitation": np.random.exponential(0.5, n_samples),
        "weather_severity": np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.5, 0.2, 0.15, 0.1, 0.05]),
        "historical_avg_demand": np.random.normal(30, 10, n_samples),
        "historical_avg_surge": np.random.normal(1.3, 0.3, n_samples),
        "is_special_event": np.random.choice([True, False], n_samples, p=[0.05, 0.95]),
        "event_demand_multiplier": np.where(
            np.random.random(n_samples) < 0.05,
            np.random.uniform(2.0, 4.0, n_samples),
            1.0
        ),
    }

    df = pd.DataFrame(data)

    # Generate target based on realistic surge logic
    # Higher demand/supply ratio → higher surge
    ratio = df["demand_last_1min"] / (df["available_drivers"] + 1)
    base_surge = np.where(ratio > 3, 2.0,
                 np.where(ratio > 2, 1.5,
                 np.where(ratio > 1.5, 1.2, 1.0)))

    # Weather boost
    weather_boost = (df["weather_severity"] - 1) * 0.15
    # Rush hour boost
    rush_boost = df["is_rush_hour"].astype(float) * 0.2
    # Event boost
    event_boost = (df["event_demand_multiplier"] - 1) * 0.3
    # Holiday boost
    holiday_boost = df["is_holiday"].astype(float) * 0.3
    # Add noise
    noise = np.random.normal(0, 0.1, n_samples)

    actual_multiplier = base_surge + weather_boost + rush_boost + event_boost + holiday_boost + noise
    actual_multiplier = np.clip(actual_multiplier, 1.0, settings.surge.multiplier_max)

    df["actual_multiplier"] = actual_multiplier

    return df


# =============================================================================
# MODEL TRAINING
# =============================================================================

def train_model(df: pd.DataFrame) -> tuple[xgb.XGBRegressor, StandardScaler, dict]:
    """
    Train XGBoost model on feature DataFrame.

    Args:
        df: DataFrame with FEATURE_COLUMNS and actual_multiplier

    Returns:
        Tuple of (trained model, scaler, metrics dictionary)
    """
    logger.info("Training XGBoost surge prediction model")

    # Validate minimum samples
    if len(df) < settings.ml.training_min_samples:
        logger.warning(
            f"Only {len(df)} samples available. "
            f"Minimum recommended: {settings.ml.training_min_samples}"
        )

    # Prepare features and target
    X = df[FEATURE_COLUMNS].copy()
    y = df["actual_multiplier"].copy()

    # Convert booleans to int for XGBoost
    bool_cols = ["is_weekend", "is_rush_hour", "is_late_night",
                 "is_lunch_hour", "is_holiday", "is_special_event"]
    for col in bool_cols:
        if col in X.columns:
            X[col] = X[col].astype(int)

    # Handle missing values
    X = X.fillna(X.median())
    y = y.fillna(1.0)

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train XGBoost model
    model = xgb.XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
    )

    model.fit(
        X_train_scaled,
        y_train,
        eval_set=[(X_test_scaled, y_test)],
        verbose=False,
    )

    # Evaluate model
    y_pred = model.predict(X_test_scaled)
    y_pred = np.clip(y_pred, 1.0, settings.surge.multiplier_max)

    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    metrics = {
        "mae": round(mae, 4),
        "r2": round(r2, 4),
        "n_samples": len(df),
        "n_features": len(FEATURE_COLUMNS),
    }

    logger.info(f"Model trained — MAE: {mae:.4f}, R²: {r2:.4f}")

    # Log feature importance
    feature_importance = dict(zip(
        FEATURE_COLUMNS,
        model.feature_importances_
    ))
    top_features = sorted(
        feature_importance.items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]
    logger.info(f"Top 5 features: {top_features}")

    return model, scaler, metrics


# =============================================================================
# SAVE MODEL
# =============================================================================

def save_model(
    model: xgb.XGBRegressor,
    scaler: StandardScaler,
    metrics: dict,
) -> None:
    """
    Save trained model and scaler to disk.

    Args:
        model: Trained XGBoost model
        scaler: Fitted StandardScaler
        metrics: Training metrics dictionary
    """
    model_path = Path(settings.ml.model_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)

    # Save model and scaler together
    joblib.dump(
        {"model": model, "scaler": scaler, "metrics": metrics},
        model_path,
    )

    logger.info(f"Model saved to {model_path}")
    logger.info(f"Metrics: {metrics}")


# =============================================================================
# ENTRY POINT
# =============================================================================

def run_training() -> None:
    """Main entry point for model training."""
    logger.info("Starting model training pipeline")

    # Load data
    df = load_training_data()

    # Train model
    model, scaler, metrics = train_model(df)

    # Save model
    save_model(model, scaler, metrics)

    logger.info("Model training complete")


if __name__ == "__main__":
    run_training()