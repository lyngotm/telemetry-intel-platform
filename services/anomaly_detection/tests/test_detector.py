"""
Unit tests for anomaly detection logic.
Tests z-score computation and severity classification — pure functions with known inputs.
"""

import pytest

from services.anomaly_detection.app.config import settings
from services.anomaly_detection.app.detector import classify_severity, compute_z_score
from services.anomaly_detection.app.rolling_window import WindowStats


class TestComputeZScore:
    """Tests for the z-score computation function."""

    def test_value_at_mean_returns_zero(self):
        """A value exactly at the mean has z-score 0."""
        stats = WindowStats(mean=65.0, stddev=5.0, count=30, min_value=55.0, max_value=75.0)
        assert compute_z_score(65.0, stats) == 0.0

    def test_value_one_stddev_above(self):
        """A value 1 standard deviation above mean has z-score 1.0."""
        stats = WindowStats(mean=65.0, stddev=5.0, count=30, min_value=55.0, max_value=75.0)
        assert compute_z_score(70.0, stats) == pytest.approx(1.0)

    def test_value_one_stddev_below(self):
        """A value 1 standard deviation below mean has z-score -1.0."""
        stats = WindowStats(mean=65.0, stddev=5.0, count=30, min_value=55.0, max_value=75.0)
        assert compute_z_score(60.0, stats) == pytest.approx(-1.0)

    def test_large_positive_deviation(self):
        """A value far above the mean produces a large positive z-score."""
        stats = WindowStats(mean=65.0, stddev=5.0, count=30, min_value=55.0, max_value=75.0)
        # 65 + (4.5 * 5) = 87.5 → z = 4.5
        assert compute_z_score(87.5, stats) == pytest.approx(4.5)

    def test_large_negative_deviation(self):
        """A value far below the mean produces a large negative z-score."""
        stats = WindowStats(mean=65.0, stddev=5.0, count=30, min_value=55.0, max_value=75.0)
        # 65 - (3.0 * 5) = 50 → z = -3.0
        assert compute_z_score(50.0, stats) == pytest.approx(-3.0)

    def test_zero_stddev_returns_zero(self):
        """When all values are identical (stddev=0), z-score should be 0 to avoid division by zero."""
        stats = WindowStats(mean=65.0, stddev=0.0, count=30, min_value=65.0, max_value=65.0)
        assert compute_z_score(65.0, stats) == 0.0
        # Even a different value should return 0 rather than crash
        assert compute_z_score(100.0, stats) == 0.0



class TestClassifySeverity:
    """Tests for the severity classification function."""

    def test_below_threshold_returns_none(self):
        """Z-score below the lowest threshold is not anomalous."""
        assert classify_severity(settings.z_score_threshold_low - 0.1) is None
        assert classify_severity(0.0) is None

    def test_low_severity(self):
        """Z-score at or above low threshold but below medium."""
        assert classify_severity(settings.z_score_threshold_low) == "low"
        assert classify_severity(settings.z_score_threshold_medium - 0.1) == "low"

    def test_medium_severity(self):
        """Z-score at or above medium threshold but below high."""
        assert classify_severity(settings.z_score_threshold_medium) == "medium"
        assert classify_severity(settings.z_score_threshold_high - 0.1) == "medium"

    def test_high_severity(self):
        """Z-score at or above high threshold but below critical."""
        assert classify_severity(settings.z_score_threshold_high) == "high"
        assert classify_severity(settings.z_score_threshold_critical - 0.1) == "high"

    def test_critical_severity(self):
        """Z-score at or above critical threshold."""
        assert classify_severity(settings.z_score_threshold_critical) == "critical"
        assert classify_severity(settings.z_score_threshold_critical + 5.0) == "critical"

    def test_uses_absolute_value(self):
        """Negative z-scores are classified by their absolute value."""
        assert classify_severity(-settings.z_score_threshold_high) == classify_severity(settings.z_score_threshold_high)

