"""Vercel serverless entry point — exposes the Flask app as a WSGI handler."""

import sys
import os

# Ensure project root is on the Python path so imports resolve correctly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app  # noqa: E402
