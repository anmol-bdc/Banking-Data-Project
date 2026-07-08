"""
FX (Foreign Exchange) Configuration.

This file defines how currency conversion behaves in the transformation layer.
No API keys are hardcoded for security reasons.
"""

# ============================================================
# Base currency and supported conversion targets
# These values define the default currency and the set of currencies
# that the transformation pipeline can convert.
# ============================================================

BASE_CURRENCY = "USD"

# Only include currencies used in synthetic data
SUPPORTED_CURRENCIES = ["USD", "EUR", "JPY", "AED", "GBP", "CAD"]

# ============================================================
# API connection settings
# These settings control how the FX client reaches the remote service
# and whether an API key is required for requests.
# ============================================================

API_URL = "https://currencyrateapi.com/api/latest"

REQUEST_TIMEOUT_SECONDS = 30
VERIFY_SSL = True

# API Key (optional → DO NOT hardcode in prod)
USE_API_KEY = False
API_KEY = ""

# ============================================================
# Fallback rate strategy
# These static values are used when the API is unavailable or a rate
# cannot be retrieved for a requested currency.
# ============================================================

# Fallback values used when API fails or currency missing
# These are QUOTE → BASE rates:
# Example: 1 EUR = 1.09 USD
FALLBACK_RATES = {
    "USD": 1.0,
    "EUR": 1.09,
    "JPY": 0.0068,
    "AED": 0.2723,
    "GBP": 1.27,
    "CAD": 0.73,
}

# ============================================================
# Rate source selection
# Indicates whether the pipeline should prefer live API data or the
# static fallback values during local or offline execution.
# ============================================================

# Available options:
# "api"       → live API
# "fallback"  → static fallback only (safe local mode)

DEFAULT_RATE_SOURCE = "api"
