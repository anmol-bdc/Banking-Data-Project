"""
Currency API client.

Handles:
- Fetching FX rates from API
- Applying fallback logic
- Normalizing rates for USD conversion
"""

from __future__ import annotations

import requests
from typing import Dict

from src.transformation.fx_config import (
    API_URL,
    BASE_CURRENCY,
    SUPPORTED_CURRENCIES,
    REQUEST_TIMEOUT_SECONDS,
    VERIFY_SSL,
    USE_API_KEY,
    API_KEY,
    FALLBACK_RATES
)


# ============================================================
# Construct the FX API request URL
# Builds a query string for the base currency and supported symbols.
# ============================================================

def build_api_url(base: str, symbols: list[str]) -> str:
    symbols_str = ",".join(symbols)
    return f"{API_URL}?base={base}&codes={symbols_str}"


# ============================================================
# Fetch and normalize FX rates
# Retrieves current exchange rates from the API and converts them into
# a dictionary keyed by uppercase currency codes.
# ============================================================

def fetch_latest_rates() -> Dict[str, float]:
    """
    Fetch latest FX rates.

    Returns:
        Dict[currency, rate_to_base]
    """

    try:
        url = build_api_url(BASE_CURRENCY, SUPPORTED_CURRENCIES)

        headers = {}
        if USE_API_KEY and API_KEY:
            headers["Authorization"] = f"Bearer {API_KEY}"

        response = requests.get(
            url,
            timeout=REQUEST_TIMEOUT_SECONDS,
            verify=VERIFY_SSL,
            headers=headers
        )

        response.raise_for_status()

        data = response.json()

        if "rates" not in data:
            raise ValueError("Invalid API response format")

        raw_rates = data["rates"]

        # Normalize keys to uppercase
        normalized_rates = {
            k.upper(): float(v) for k, v in raw_rates.items()
        }

        # Ensure base currency included
        normalized_rates[BASE_CURRENCY] = 1.0

        print("[INFO] FX rates fetched from API")

        return normalized_rates

    except Exception as e:
        print(f"[WARNING] API failed, using fallback rates. Error: {e}")

        return FALLBACK_RATES


# ============================================================
# Convert an amount into the base currency
# Uses the provided exchange rate map to translate a value into the
# configured base currency such as USD.
# ============================================================

def convert_to_base(currency: str, amount: float, rates: Dict[str, float]) -> float:
    """
    Convert amount from source currency to base currency.

    Example:
    If BASE = USD and EUR rate = 1.09
    → EUR amount * 1.09 = USD
    """

    if currency is None:
        return amount

    currency = currency.upper()

    rate = rates.get(currency)

