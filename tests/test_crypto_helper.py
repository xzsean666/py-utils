"""Tests for CryptoHelper utilities."""

import base64
import re

from py_utils.encode_utils.crypto_helper import CryptoHelper


def test_calculate_object_md5_ignores_key_order():
    payload_a = {"b": 2, "a": 1}
    payload_b = {"a": 1, "b": 2}
    assert CryptoHelper.calculate_object_md5(payload_a) == CryptoHelper.calculate_object_md5(
        payload_b
    )


def test_calculate_md5_and_sha256_known_values():
    assert CryptoHelper.calculate_md5("hello") == "5d41402abc4b2a76b9719d911017c592"
    assert (
        CryptoHelper.calculate_sha256("hello")
        == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )


def test_calculate_buffer_md5():
    data = b"\x00\x01\x02"
    assert CryptoHelper.calculate_buffer_md5(data) == "b95f67f61ebb03619622d798f45fc2d3"


def test_generate_random_string_length_and_charset():
    token = CryptoHelper.generate_random_string(16)
    assert len(token) == 16
    assert re.fullmatch(r"[0-9a-f]+", token) is not None


def test_encrypt_and_decrypt_round_trip():
    secret_key = "super-secret-key"
    plaintext = "Sensitive information"
    encrypted = CryptoHelper.encrypt_aes(plaintext, secret_key)
    iv_b64, cipher_b64 = encrypted.split(":")
    assert base64.b64decode(iv_b64)  # IV decodes without error
    assert base64.b64decode(cipher_b64)  # Ciphertext decodes without error

    decrypted = CryptoHelper.decrypt_aes(encrypted, secret_key)
    assert decrypted == plaintext


def run():
    test_calculate_object_md5_ignores_key_order()
    test_calculate_md5_and_sha256_known_values()
    test_calculate_buffer_md5()
    test_generate_random_string_length_and_charset()
    test_encrypt_and_decrypt_round_trip()
    print("test_crypto_helper: all checks passed.")


if __name__ == "__main__":
    run()
