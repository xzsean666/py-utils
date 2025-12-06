"""
Utility helpers for common cryptographic tasks.
"""

import base64
import hashlib
import json
import math
import secrets
from typing import Any

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


class CryptoHelper:
    """Collection of cryptographic helper methods."""

    @staticmethod
    def calculate_object_md5(obj: Any) -> str:
        """
        Compute the MD5 hash of a JSON-serializable object using sorted keys.

        Args:
            obj: Any JSON-serializable object.

        Returns:
            The MD5 hex digest of the normalized JSON representation.
        """
        normalized = json.dumps(obj, sort_keys=True, separators=(",", ":"))
        return CryptoHelper.calculate_md5(normalized)

    @staticmethod
    def calculate_md5(text: str) -> str:
        """
        Compute the MD5 hash of a string.

        Args:
            text: Input string.

        Returns:
            The MD5 hex digest.
        """
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    @staticmethod
    def calculate_buffer_md5(buffer: bytes) -> str:
        """
        Compute the MD5 hash of a bytes buffer.

        Args:
            buffer: Bytes to hash.

        Returns:
            The MD5 hex digest.
        """
        return hashlib.md5(buffer).hexdigest()

    @staticmethod
    def generate_random_string(length: int) -> str:
        """
        Generate a random hexadecimal string of the requested length.

        Args:
            length: Desired length of the output string.

        Returns:
            A random hexadecimal string.
        """
        if length <= 0:
            raise ValueError("length must be positive")
        hex_len = math.ceil(length / 2)
        random_hex = secrets.token_hex(hex_len)
        return random_hex[:length]

    @staticmethod
    def encrypt_aes(text: str, key: str) -> str:
        """
        Encrypt text using AES-256-CBC with a SHA-256 derived key.

        Args:
            text: Plaintext to encrypt.
            key: Key material used to derive a 32-byte AES key.

        Returns:
            Base64 encoded IV and ciphertext separated by a colon.
        """
        key_bytes = hashlib.sha256(key.encode("utf-8")).digest()
        iv = secrets.token_bytes(16)
        cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(text.encode("utf-8")) + padder.finalize()
        encrypted_bytes = encryptor.update(padded_data) + encryptor.finalize()
        iv_b64 = base64.b64encode(iv).decode("utf-8")
        encrypted_b64 = base64.b64encode(encrypted_bytes).decode("utf-8")
        return f"{iv_b64}:{encrypted_b64}"

    @staticmethod
    def decrypt_aes(encrypted: str, key: str) -> str:
        """
        Decrypt an AES-256-CBC encrypted payload created by `encrypt_aes`.

        Args:
            encrypted: Encrypted payload in `iv:ciphertext` base64 format.
            key: Key material used to derive the 32-byte AES key.

        Returns:
            The decrypted plaintext.
        """
        try:
            iv_b64, encrypted_b64 = encrypted.split(":", 1)
        except ValueError as exc:
            raise ValueError("Encrypted payload must contain IV and ciphertext separated by ':'") from exc

        key_bytes = hashlib.sha256(key.encode("utf-8")).digest()
        iv = base64.b64decode(iv_b64)
        encrypted_bytes = base64.b64decode(encrypted_b64)
        cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(encrypted_bytes) + decryptor.finalize()
        unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
        plaintext_bytes = unpadder.update(padded_plaintext) + unpadder.finalize()
        return plaintext_bytes.decode("utf-8")

    @staticmethod
    def calculate_sha256(text: str) -> str:
        """
        Compute the SHA-256 hash of a string.

        Args:
            text: Input string.

        Returns:
            The SHA-256 hex digest.
        """
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
