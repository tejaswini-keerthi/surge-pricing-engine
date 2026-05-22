"""
tests/test_surge_calculator.py
===============================
Unit tests for surge pricing calculation logic.

Tests rule-based surge, final surge combination,
and surge tier labeling across all scenarios.
"""

from __future__ import annotations

import pytest
from streaming.surge_calculator import (
    calculate_rule_based_surge,
    calculate_final_surge,
    get_surge_tier,
)


class TestRuleBasedSurge:
    """Tests for calculate_rule_based_surge."""

    def test_no_surge_below_threshold(self):
        """Ratio below 1.5 should return 1.0x."""
        assert calculate_rule_based_surge(demand=10, supply=30) == 1.0

    def test_low_surge(self):
        """Ratio between 1.5 and 2.0 should return 1.2x."""
        assert calculate_rule_based_surge(demand=45, supply=30) == 1.2

    def test_medium_surge(self):
        """Ratio between 2.0 and 3.0 should return 1.5x."""
        assert calculate_rule_based_surge(demand=70, supply=30) == 1.5

    def test_high_surge(self):
        """Ratio above 3.0 should return 2.0x."""
        assert calculate_rule_based_surge(demand=100, supply=30) == 2.0

    def test_zero_supply_returns_max(self):
        """Zero supply should return maximum multiplier."""
        result = calculate_rule_based_surge(demand=100, supply=0)
        assert result == 3.0

    def test_zero_demand_no_surge(self):
        """Zero demand should return 1.0x."""
        assert calculate_rule_based_surge(demand=0, supply=30) == 1.0

    def test_never_exceeds_max(self):
        """Multiplier should never exceed surge_multiplier_max."""
        result = calculate_rule_based_surge(demand=10000, supply=1)
        assert result <= 3.0

    def test_equal_demand_supply_no_surge(self):
        """Equal demand and supply (ratio=1.0) should not surge."""
        assert calculate_rule_based_surge(demand=30, supply=30) == 1.0


class TestFinalSurge:
    """Tests for calculate_final_surge."""

    def test_uses_ml_when_confident(self):
        """Should use ML prediction when confidence >= threshold."""
        result = calculate_final_surge(
            rule_multiplier=1.5,
            ml_multiplier=1.8,
            ml_confidence=0.95,
        )
        assert result == 1.8

    def test_falls_back_to_rule_when_uncertain(self):
        """Should use rule-based when ML confidence is low."""
        result = calculate_final_surge(
            rule_multiplier=1.5,
            ml_multiplier=1.8,
            ml_confidence=0.3,
        )
        assert result == 1.5

    def test_rule_based_floor_enforced(self):
        """ML cannot go below rule-based floor."""
        result = calculate_final_surge(
            rule_multiplier=2.0,
            ml_multiplier=0.8,
            ml_confidence=0.95,
        )
        assert result == 2.0

    def test_never_exceeds_max(self):
        """Final multiplier should never exceed max."""
        result = calculate_final_surge(
            rule_multiplier=3.0,
            ml_multiplier=5.0,
            ml_confidence=0.99,
        )
        assert result <= 3.0


class TestSurgeTier:
    """Tests for get_surge_tier."""

    def test_normal_tier(self):
        assert get_surge_tier(1.0) == "normal"

    def test_low_tier(self):
        assert get_surge_tier(1.2) == "low"

    def test_medium_tier(self):
        assert get_surge_tier(1.5) == "medium"

    def test_high_tier(self):
        assert get_surge_tier(2.0) == "high"

    def test_maximum_tier(self):
        assert get_surge_tier(3.0) == "maximum"