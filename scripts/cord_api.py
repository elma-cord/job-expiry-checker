"""Small client for the Cord admin API used by the expiry workflow.

Credentials come from the CORD_ADMIN_EMAIL / CORD_ADMIN_PASSWORD secrets.
Nothing here ever prints a token, cookie or password.
"""
import os
import sys

import requests

BASE_URL = os.environ.get("CORD_BASE_URL", "https://cord.com").rstrip("/")
TOKEN_KEYS = ("jwtCookie", "token", "accessToken", "jwt")


def _find_token(body):
    if isinstance(body, dict):
        for key in TOKEN_KEYS:
            if isinstance(body.get(key), str) and body[key]:
                return body[key]
        for value in body.values():
            found = _find_token(value)
            if found:
                return found
    return None


def login():
    email = os.environ.get("CORD_ADMIN_EMAIL", "").strip()
    password = os.environ.get("CORD_ADMIN_PASSWORD", "")
    if not email or not password:
        sys.exit("CORD_ADMIN_EMAIL / CORD_ADMIN_PASSWORD secrets are not set.")

    session = requests.Session()
    resp = session.post(
        f"{BASE_URL}/api/v2/auth/admin/login",
        json={"email": email, "password": password},
        timeout=60,
    )
    if resp.status_code >= 400:
        sys.exit(f"Cord admin login failed: HTTP {resp.status_code}")

    token = None
    try:
        token = _find_token(resp.json())
    except ValueError:
        pass
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    elif not session.cookies:
        sys.exit("Cord admin login returned neither a token nor a cookie.")
    return session


def _unwrap(body):
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("data", "rows", "result"):
            if isinstance(body.get(key), list):
                return body[key]
    raise ValueError("Unexpected response shape from Cord")


def get_external_listings(session):
    resp = session.get(f"{BASE_URL}/api/v2/admin/external-listings", timeout=300)
    if resp.status_code >= 400:
        sys.exit(f"Fetching external listings failed: HTTP {resp.status_code}")
    return _unwrap(resp.json())


def bulk_delete(session, listing_ids):
    resp = session.post(
        f"{BASE_URL}/api/v2/admin/external-listings/bulk-delete",
        json={"listingIDs": listing_ids},
        timeout=120,
    )
    return resp.status_code
