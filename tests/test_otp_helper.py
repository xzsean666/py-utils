"""Tests for OTPHelper utilities."""

from py_utils.encode_utils.otp_helper import OTPHelper, OTPOptions


def test_new_secret_returns_otpauth_and_qr_data():
    helper = OTPHelper()
    secret_result = helper.new_secret("user@example.com", "MyService")
    assert isinstance(secret_result.secret, str) and len(secret_result.secret) > 0
    assert "otpauth://" in secret_result.otpauth
    assert secret_result.image_url.startswith("data:image/png;base64,")


def test_generate_and_verify_token_round_trip():
    helper = OTPHelper(OTPOptions(step=30, digits=6, window=1))
    secret = helper.new_secret("user", "Service").secret
    token = helper.get_token(secret)
    assert helper.verify_token(token, secret) is True
    result = helper.verify_token_with_detail(token, secret)
    assert result.is_valid is True
    assert result.delta is not None
    assert 0 <= result.delta <= helper.options.step


def test_timer_returns_remaining_seconds():
    helper = OTPHelper(OTPOptions(step=30))
    remaining = helper.timer()
    assert 1 <= remaining <= helper.options.step


def run():
    test_new_secret_returns_otpauth_and_qr_data()
    test_generate_and_verify_token_round_trip()
    test_timer_returns_remaining_seconds()
    print("test_otp_helper: all checks passed.")


if __name__ == "__main__":
    run()
