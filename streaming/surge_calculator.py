"""
streaming/surge_calculator.py
==============================
Pure business logic for calculating surge pricing multipliers.

This module is intentionally kept separate from Spark so the
surge logic can be tested independently without any streaming
infrastructure.
"""

from __future__ import annotations

from config.settings import settings


def calculate_rule_based_surge(demand: int, supply: int) -> float:
    """
    Calculate surge multiplier using rule-based thresholds.

    Args:
        demand: Number of ride requests in the window
        supply: Number of available drivers in the zone

    Returns:
        Surge multiplier float (1.0 = no surge)
    """
    if supply <= 0:
        return settings.surge.multiplier_max

    ratio = demand / supply

    if ratio >= settings.surge.threshold_high:
        multiplier = settings.surge.multiplier_high
    elif ratio >= settings.surge.threshold_medium:
        multiplier = settings.surge.multiplier_medium
    elif ratio >= settings.surge.threshold_low:
        multiplier = settings.surge.multiplier_low
    else:
        multiplier = 1.0

    return min(multiplier, settings.surge.multiplier_max)


def calculate_final_surge(
    rule_multiplier: float,
    ml_multiplier: float,
    ml_confidence: float,
) -> float:
    """
    Combine rule-based and ML multipliers into final surge price.

    The rule-based multiplier acts as a safety floor.
    The ML multiplier is used when confidence exceeds threshold.

    Args:
        rule_multiplier: Multiplier from rule-based system
        ml_multiplier: Multiplier predicted by ML model
        ml_confidence: ML model confidence score (0-1)

    Returns:
        Final surge multiplier
    """
    # Use ML prediction if confidence is high enough
    if ml_confidence >= settings.ml.confidence_threshold:
        # ML can improve on rule-based but never go below it
        final = max(rule_multiplier, ml_multiplier)
    else:
        # Fall back to rule-based when ML is uncertain
        final = rule_multiplier

    return min(final, settings.surge.multiplier_max)


def get_surge_tier(multiplier: float) -> str:
    """
    Get human-readable tier label for a surge multiplier.

    Args:
        multiplier: Surge multiplier float

    Returns:
        Tier string: normal, low, medium, high, maximum
    """
    if multiplier >= settings.surge.multiplier_high:
        return "maximum"
    elif multiplier >= settings.surge.multiplier_medium:
        return "high"
    elif multiplier >= settings.surge.multiplier_low:
        return "medium"
    elif multiplier > 1.0:
        return "low"
    return "normal"