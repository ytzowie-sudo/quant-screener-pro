"""
Reads API keys from .streamlit/secrets.toml directly.
Works both inside Streamlit (st.secrets) and in subprocess (run_fund.py).
"""
import os
import re


def _read_secrets_toml() -> dict:
    """Parses .streamlit/secrets.toml and returns a dict of key=value pairs."""
    toml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".streamlit", "secrets.toml")
    result = {}
    try:
        with open(toml_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                m = re.match(r'^(\w+)\s*=\s*["\'](.+)["\']$', line)
                if m:
                    result[m.group(1)] = m.group(2)
    except FileNotFoundError:
        pass
    return result


def get_secret(key: str, default: str = "") -> str:
    """
    Returns the value of a secret key using this priority:
      1. Environment variable
      2. st.secrets (if running inside Streamlit)
      3. .streamlit/secrets.toml (direct parse — works in subprocess)
      4. default
    """
    # 1. Environment variable
    val = os.environ.get(key, "")
    if val and val != "your-perplexity-api-key-here":
        return val

    # 2. st.secrets (Streamlit context)
    try:
        import streamlit as st
        val = st.secrets.get(key, "")
        if val and val != "your-perplexity-api-key-here":
            return val
    except Exception:
        pass

    # 3. Direct parse of secrets.toml (subprocess context)
    secrets = _read_secrets_toml()
    val = secrets.get(key, "")
    if val and val != "your-perplexity-api-key-here":
        return val

    return default
