"""Authentication hooks for Chainlit (JWT header and password modes)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any

import chainlit as cl


AUTH_MODE = os.environ.get("CHAINLIT_AUTH_MODE", "").lower()
JWT_SECRET = os.environ.get("CHAINLIT_JWT_SECRET", "")
JWT_AUDIENCE = os.environ.get("CHAINLIT_JWT_AUDIENCE", "")
JWT_ISSUER = os.environ.get("CHAINLIT_JWT_ISSUER", "")
AUTH_USERNAME = os.environ.get("CHAINLIT_USERNAME", "")
AUTH_PASSWORD = os.environ.get("CHAINLIT_PASSWORD", "")
AUTH_PASSWORD_HASH = os.environ.get("CHAINLIT_PASSWORD_HASH", "")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _verify_jwt_signature(token: str, secret: str) -> dict[str, Any] | None:
    parts = token.split(".")
    if len(parts) != 3:
        return None

    header_b64, payload_b64, signature_b64 = parts
    try:
        header = json.loads(_b64url_decode(header_b64))
        payload = json.loads(_b64url_decode(payload_b64))
    except (ValueError, json.JSONDecodeError):
        return None

    if header.get("alg") != "HS256":
        return None

    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    expected_sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    expected_b64 = _b64url_encode(expected_sig)

    if not hmac.compare_digest(expected_b64, signature_b64):
        return None

    return payload


def _validate_jwt_claims(payload: dict[str, Any]) -> bool:
    now = int(time.time())
    exp = payload.get("exp")
    if isinstance(exp, (int, float)) and now > int(exp):
        return False

    nbf = payload.get("nbf")
    if isinstance(nbf, (int, float)) and now < int(nbf):
        return False

    if JWT_AUDIENCE:
        aud = payload.get("aud")
        if aud != JWT_AUDIENCE:
            return False

    if JWT_ISSUER:
        iss = payload.get("iss")
        if iss != JWT_ISSUER:
            return False

    return True


def _check_password(username: str, password: str) -> bool:
    if not AUTH_USERNAME:
        return False

    if not hmac.compare_digest(username, AUTH_USERNAME):
        return False

    if AUTH_PASSWORD_HASH:
        digest = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(digest, AUTH_PASSWORD_HASH)

    if AUTH_PASSWORD:
        return hmac.compare_digest(password, AUTH_PASSWORD)

    return False


if AUTH_MODE in {"jwt", "header", "both"}:

    @cl.header_auth_callback
    async def header_auth_callback(headers) -> cl.User | None:
        if not JWT_SECRET:
            return None

        auth_header = headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None

        token = auth_header[len("Bearer ") :].strip()
        payload = _verify_jwt_signature(token, JWT_SECRET)
        if not payload or not _validate_jwt_claims(payload):
            return None

        identifier = payload.get("sub") or payload.get("email") or "jwt-user"
        display_name = payload.get("name") or payload.get("email")
        return cl.User(
            identifier=str(identifier),
            display_name=str(display_name) if display_name else None,
            metadata=payload,
        )


if AUTH_MODE in {"password", "both"}:

    @cl.password_auth_callback
    async def password_auth_callback(username: str, password: str) -> cl.User | None:
        if _check_password(username, password):
            return cl.User(identifier=username, display_name=username)
        return None
