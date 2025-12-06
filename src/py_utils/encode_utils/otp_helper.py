"""
One-time password (OTP) helper utilities built on top of pyotp.
"""

import base64
import hashlib
import io
import time
from dataclasses import dataclass
from typing import Optional

import pyotp
import qrcode


@dataclass
class OTPOptions:
    window: int = 1
    step: int = 30
    algorithm: str = "sha1"
    digits: int = 6


@dataclass
class OTPSecretResult:
    secret: str
    otpauth: str
    image_url: str


@dataclass
class VerifyResult:
    is_valid: bool
    delta: Optional[int] = None


class OTPHelper:
    """Helper for generating and verifying TOTP codes."""

    def __init__(self, options: Optional[OTPOptions] = None):
        self.options = options or OTPOptions()
        if self.options.step <= 0:
            raise ValueError("step must be a positive integer")
        if self.options.digits <= 0:
            raise ValueError("digits must be a positive integer")
        if self.options.window is not None and self.options.window < 0:
            raise ValueError("window must be zero or a positive integer")

    def _digest(self):
        try:
            return getattr(hashlib, self.options.algorithm.lower())
        except AttributeError as exc:
            raise ValueError(f"Unsupported hash algorithm: {self.options.algorithm}") from exc

    def _totp(self, secret: str) -> pyotp.TOTP:
        return pyotp.TOTP(
            secret,
            interval=self.options.step,
            digits=self.options.digits,
            digest=self._digest(),
        )

    def new_secret(self, user: str, service: str) -> OTPSecretResult:
        """
        Generate a new secret, otpauth URI, and corresponding QR code data URL.
        """
        secret = pyotp.random_base32()
        totp = self._totp(secret)
        otpauth = totp.provisioning_uri(name=user, issuer_name=service)
        qr_image = qrcode.make(otpauth)
        buffer = io.BytesIO()
        qr_image.save(buffer, format="PNG")
        encoded_image = base64.b64encode(buffer.getvalue()).decode("utf-8")
        image_url = f"data:image/png;base64,{encoded_image}"
        return OTPSecretResult(secret=secret, otpauth=otpauth, image_url=image_url)

    def timer(self) -> int:
        """
        Remaining seconds in the current OTP window.
        """
        elapsed = int(time.time()) % self.options.step
        return self.options.step - elapsed if elapsed != 0 else self.options.step

    def get_token(self, secret: str) -> str:
        """
        Generate the current OTP token for the given secret.
        """
        return self._totp(secret).now()

    def verify_token(self, token: str, secret: str, window: Optional[int] = None) -> bool:
        """
        Verify an OTP token.
        """
        totp = self._totp(secret)
        return bool(
            totp.verify(token, valid_window=window if window is not None else self.options.window)
        )

    def verify_token_with_detail(
        self, token: str, secret: str, window: Optional[int] = None
    ) -> VerifyResult:
        """
        Verify an OTP token and return the validation result with timing metadata.
        """
        is_valid = self.verify_token(token, secret, window)
        delta = int(time.time()) % self.options.step if is_valid else None
        return VerifyResult(is_valid=is_valid, delta=delta)
