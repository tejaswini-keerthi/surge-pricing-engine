"""
ml/serve_model.py
=================
Loads trained XGBoost model and serves surge predictions.

Used by FastAPI to get ML-based surge multiplier per zone.
Falls back to rule-based pricing when model is unavailable
or confidence is below threshold.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import numpy as np
from loguru import logger

from config.settings import settings
from ml.feature_engineering import FEATURE_COLUMNS


# =============================================================================
# MODEL LOADER
# =============================================================================

class SurgePredictor:
    """
    Loads and serves the trained XGBoost surge prediction model.

    Implements confidence scoring and rule-based fallback.
    Loaded once at startup and reused for all predictions.
    """

    def __init__(self) -> None:
        self.model = None
        self.scaler = None
        self.metrics: dict = {}
        self.is_loaded = False
        self._load_model()

    def _load_model(self) -> None:
        """Load model from disk if it exists."""
        model_path = Path(settings.ml.model_path)

        if not model_path.exists():
            logger.warning(
                f"Model not found at {model_path}. "
                "Run ml/train_model.py first. "
                "Using rule-based pricing until model is available."
            )
            return

        try:
            saved = joblib.load(model_path)
            self.model = saved["model"]
            self.scaler = saved["scaler"]
            self.metrics = saved.get("metrics", {})
            self.is_loaded = True
            logger.info(
                f"Model loaded from {model_path} — "
                f"MAE: {self.metrics.get('mae', 'N/A')}, "
                f"R²: {self.metrics.get('r2', 'N/A')}"
            )
        except Exception as e:
            logger.error(f"Model load failed: {e}")
            self.is_loaded = False

    def _prepare_features(self, features: dict[str, Any]) -> np.ndarray:
        """
        Convert feature dictionary to numpy array for prediction.

        Args:
            features: Feature dictionary with all 21 features

        Returns:
            2D numpy array shaped (1, n_features)
        """
        row = []
        for col in FEATURE_COLUMNS:
            value = features.get(col, 0)
            # Convert booleans to int
            if isinstance(value, bool):
                value = int(value)
            row.append(float(value))

        return np.array(row).reshape(1, -1)

    def _compute_confidence(
        self,
        features: dict[str, Any],
        prediction: float,
    ) -> float:
        """
        Estimate prediction confidence based on feature validity.

        Confidence is reduced when:
        - Demand data is missing or zero
        - Weather data is default (no API response)
        - Historical data is at default values

        Args:
            features: Feature dictionary
            prediction: Raw model prediction

        Returns:
            Confidence score between 0 and 1
        """
        confidence = 1.0

        # Reduce confidence if demand data is sparse
        if features.get("demand_last_1min", 0) == 0:
            confidence *= 0.5

        # Reduce confidence if historical data is at default
        if features.get("historical_avg_demand", 20.0) == 20.0:
            confidence *= 0.8

        # Reduce confidence if prediction is extreme
        if prediction > 2.5 or prediction < 1.0:
            confidence *= 0.7

        return round(min(confidence, 1.0), 3)

    def predict(self, features: dict[str, Any]) -> dict[str, Any]:
        """
        Predict surge multiplier for a zone.

        Args:
            features: Complete 21-feature dictionary

        Returns:
            Dictionary with ml_multiplier and ml_confidence
        """
        if not self.is_loaded:
            return {
                "ml_multiplier": 1.0,
                "ml_confidence": 0.0,
                "ml_available": False,
            }

        try:
            X = self._prepare_features(features)
            X_scaled = self.scaler.transform(X)
            raw_prediction = float(self.model.predict(X_scaled)[0])

            # Clip to valid range
            ml_multiplier = round(
                np.clip(raw_prediction, 1.0, settings.surge.multiplier_max),
                2,
            )

            confidence = self._compute_confidence(features, ml_multiplier)

            return {
                "ml_multiplier": ml_multiplier,
                "ml_confidence": confidence,
                "ml_available": True,
            }

        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return {
                "ml_multiplier": 1.0,
                "ml_confidence": 0.0,
                "ml_available": False,
            }

    def reload(self) -> bool:
        """
        Reload model from disk.

        Called when a new model has been trained.

        Returns:
            True if reload successful
        """
        logger.info("Reloading surge prediction model")
        self._load_model()
        return self.is_loaded


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

surge_predictor = SurgePredictor()