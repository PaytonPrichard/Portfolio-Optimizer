# financials package
import logging

# Suppress yfinance log noise. The 404s from its quoteSummary endpoint and
# "possibly delisted" warnings are mostly false alarms — yfinance falls back
# to other endpoints and the data still arrives. Silencing keeps the terminal
# readable so real errors stand out.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
