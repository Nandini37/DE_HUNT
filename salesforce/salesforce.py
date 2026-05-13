"""
Connect to Salesforce and run a SOQL query for the first 100 Account rows.

JWT Bearer (matches Connected App + certificate / principal):
  SF_CONSUMER_KEY       Connected App Consumer Key (client id)
  SF_USERNAME           Integration user (e.g. principal / login email)
  SF_PRIVATE_KEY_PATH   Path to PEM private key that signs the JWT
  SF_DOMAIN             Optional: set to "test" for sandbox

Password flow (simpler for local trials; no PEM):
  SF_USERNAME
  SF_PASSWORD           Login password only (no token appended here)
  SF_SECURITY_TOKEN     User security token (appended by this script)

Optional:
  SF_INSTANCE_URL       If set, used after login (usually auto from org)

Note: A Java .jks keystore is not read by Python. Export the integration
user's private key to PEM for SF_PRIVATE_KEY_PATH.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

try:
    from simple_salesforce import Salesforce
except ImportError as e:  # pragma: no cover
    print("Install deps: pip install -r requirements.txt", file=sys.stderr)
    raise e


def _load_env() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")


def connect_salesforce() -> Salesforce:
    _load_env()

    username = os.environ.get("SF_USERNAME")
    password = os.environ.get("SF_PASSWORD")
    security_token = os.environ.get("SF_SECURITY_TOKEN", "")
    consumer_key = os.environ.get("SF_CONSUMER_KEY")
    private_key_path = os.environ.get("SF_PRIVATE_KEY_PATH")
    domain = os.environ.get("SF_DOMAIN") or None  # "test" for sandbox

    if not username:
        raise ValueError("SF_USERNAME is required.")

    # Password flow when password + token are present
    if password is not None and str(password).strip() != "":
        combined = f"{password}{security_token or ''}"
        kwargs: dict = {"username": username, "password": combined}
        if domain:
            kwargs["domain"] = domain
        return Salesforce(**kwargs)

    # JWT Bearer flow (Connected App + PEM private key)
    if not consumer_key or not private_key_path:
        raise ValueError(
            "For JWT, set SF_CONSUMER_KEY and SF_PRIVATE_KEY_PATH. "
            "For password login, set SF_PASSWORD (and usually SF_SECURITY_TOKEN)."
        )
    key_file = Path(private_key_path).expanduser()
    if not key_file.is_file():
        raise FileNotFoundError(f"Private key not found: {key_file}")

    kwargs = {
        "username": username,
        "consumer_key": consumer_key,
        "privatekey_file": str(key_file),
    }
    if domain:
        kwargs["domain"] = domain
    return Salesforce(**kwargs)


def query_top_accounts(sf: Salesforce, limit: int = 100) -> dict:
    soql = f"SELECT Id, Name, Type, Industry FROM Account LIMIT {int(limit)}"
    return sf.query(soql)


def main() -> None:
    sf = connect_salesforce()
    out = query_top_accounts(sf, 100)
    total = out.get('totalSize', 0)
    print(f"done: {total} record(s) (queryLocator empty = single page)")
    for row in out.get("records", []):
        row = {k: v for k, v in row.items() if k != "attributes"}
        print(row)


if __name__ == "__main__":
    main()
