"""
Helper utilities for working with JSON Web Tokens (JWT).
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import jwt
from jwt import (
    DecodeError,
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidSignatureError,
    InvalidTokenError,
)

JWTPayload = Dict[str, Any]


class JWTHelper:
    """Helper for generating and verifying JWT tokens."""

    def __init__(self, secret_key: str):
        self.secret_key = secret_key
        self.algorithm = "HS256"

    def generate_token(self, payload: JWTPayload, expires_in_seconds: int = 3600) -> str:
        """
        Generate a signed JWT with an expiration claim.

        Args:
            payload: Claims to embed in the token.
            expires_in_seconds: Lifetime in seconds (defaults to 1 hour).

        Returns:
            Encoded JWT string.
        """
        claims = dict(payload)
        claims["exp"] = datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)
        return jwt.encode(claims, self.secret_key, algorithm=self.algorithm)

    def verify_token(self, token: str) -> JWTPayload:
        """
        Verify a JWT and return its payload.

        Raises:
            ValueError: If the token is invalid or expired.
        """
        try:
            return jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
        except ExpiredSignatureError as exc:
            raise ValueError("TOKEN_EXPIRED") from exc
        except ImmatureSignatureError as exc:
            raise ValueError("TOKEN_NOT_ACTIVE") from exc
        except (DecodeError, InvalidSignatureError, InvalidTokenError) as exc:
            raise ValueError("INVALID_TOKEN") from exc

    def decode_token(self, token: str) -> Optional[JWTPayload]:
        """
        Decode a JWT without validating the signature.

        Args:
            token: JWT string.

        Returns:
            Decoded payload or None if decode fails.
        """
        try:
            return jwt.decode(token, options={"verify_signature": False, "verify_exp": False})
        except Exception:
            return None

    def is_token_expired(self, token: str) -> bool:
        """
        Determine whether a JWT is expired by inspecting its payload.
        """
        try:
            decoded = jwt.decode(token, options={"verify_signature": False, "verify_exp": False})
        except Exception:
            return True
        exp = decoded.get("exp")
        if exp is None:
            return True
        if isinstance(exp, (int, float)):
            exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
        elif isinstance(exp, datetime):
            exp_dt = exp if exp.tzinfo else exp.replace(tzinfo=timezone.utc)
        else:
            return True
        return exp_dt < datetime.now(timezone.utc)

    def encode(self, payload: JWTPayload) -> str:
        """
        Encode a payload without adding an expiry.
        """
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

    def decode(self, token: str) -> JWTPayload:
        """
        Decode a JWT verifying its signature.

        Raises:
            ValueError: If the token cannot be verified.
        """
        try:
            return jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
        except Exception as exc:
            raise ValueError("Invalid token") from exc
