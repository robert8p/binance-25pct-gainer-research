from app.main import health


def test_health_version_v4_25pct():
    assert health() == {"status": "ok", "version": "4.0.1-25pct", "target_gain_pct": "25"}
