from transform_service.app.pipeline.normalizer import normalize


def test_normalize_lowers_event_type_and_trims_metadata() -> None:
    event = {
        "event_id": " abc ",
        "source": " S ",
        "event_type": " User.SignUp ",
        "timestamp": "2024-01-01T00:00:00+00:00",
        "payload": {" Name ": "  x  "},
        "metadata": {" Env ": " prod "},
    }
    enrichments = {"timestamp_normalized": "2024-01-01T00:00:00+00:00"}
    out = normalize(event, enrichments)
    assert out["event_type"] == "user.signup"
    assert out["source"] == "S"
    assert out["event_id"] == "abc"
    assert out["metadata"] == {"env": "prod"}
    assert out["payload"]["Name"] == "x"
