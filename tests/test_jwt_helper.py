"""Tests for JWTHelper utilities."""

from py_utils.encode_utils.jwt_helper import JWTHelper


def test_generate_and_verify_token():
    helper = JWTHelper("secret")
    token = helper.generate_token({"user_id": 123}, expires_in_seconds=120)
    decoded = helper.verify_token(token)
    assert decoded["user_id"] == 123
    assert "exp" in decoded


def test_verify_expired_token_raises():
    helper = JWTHelper("secret")
    token = helper.generate_token({"scope": "test"}, expires_in_seconds=-1)
    try:
        helper.verify_token(token)
        raise AssertionError("Expected verify_token to raise for expired token")
    except ValueError as exc:
        assert "TOKEN_EXPIRED" in str(exc)


def test_is_token_expired_detects_expired_and_valid_tokens():
    helper = JWTHelper("secret")
    fresh_token = helper.generate_token({"scope": "test"}, expires_in_seconds=60)
    assert helper.is_token_expired(fresh_token) is False

    expired_token = helper.generate_token({"scope": "test"}, expires_in_seconds=-1)
    assert helper.is_token_expired(expired_token) is True


def test_decode_token_without_signature_validation():
    helper = JWTHelper("secret")
    token = helper.encode({"role": "admin"})
    decoded = helper.decode_token(token)
    assert decoded["role"] == "admin"

    assert helper.decode_token("invalid.token.value") is None


def test_decode_invalid_signature_raises():
    helper = JWTHelper("secret")
    token = helper.encode({"role": "admin"})
    other_helper = JWTHelper("different-secret")
    try:
        other_helper.decode(token)
        raise AssertionError("Expected decode to raise on invalid signature")
    except ValueError as exc:
        assert "Invalid token" in str(exc)


def run():
    test_generate_and_verify_token()
    test_verify_expired_token_raises()
    test_is_token_expired_detects_expired_and_valid_tokens()
    test_decode_token_without_signature_validation()
    test_decode_invalid_signature_raises()
    print("test_jwt_helper: all checks passed.")


if __name__ == "__main__":
    run()
