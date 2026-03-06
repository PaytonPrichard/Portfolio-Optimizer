"""Flask entry point for MarketMosaic web app."""

import math
import os
import secrets

from dotenv import load_dotenv
load_dotenv()

from flask import Flask
from flask.json.provider import DefaultJSONProvider

from routes.home import home_bp
from routes.dashboard import dashboard_bp
from routes.tracker import tracker_bp
from routes.info import info_bp
from routes.download import download_bp
from routes.picks import picks_bp
from routes.portfolio import portfolio_bp
from routes.portfolio_widgets import portfolio_widgets_bp
from routes.alpha import alpha_bp


def _sanitize_nan(obj):
    """Recursively replace NaN/Inf floats with None."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_nan(v) for v in obj]
    return obj


class _SafeJSONProvider(DefaultJSONProvider):
    """JSON provider that converts NaN/Infinity to null instead of
    emitting bare JS tokens that break JSON.parse() in the browser."""

    def default(self, o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        return super().default(o)

    def dumps(self, obj, **kwargs):
        kwargs.setdefault("default", self.default)
        # Recursively sanitize before serialization so NaN/Infinity
        # never reach json.dumps (which emits them as bare tokens).
        obj = _sanitize_nan(obj)
        return super().dumps(obj, **kwargs)


def create_app():
    app = Flask(__name__)
    app.json_provider_class = _SafeJSONProvider
    app.json = _SafeJSONProvider(app)
    app.secret_key = os.environ.get("SECRET_KEY") or secrets.token_hex(32)

    # Secure session cookies
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    if os.environ.get("FLASK_ENV") == "production" or not app.debug:
        app.config["SESSION_COOKIE_SECURE"] = True

    app.register_blueprint(home_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(tracker_bp)
    app.register_blueprint(info_bp)
    app.register_blueprint(download_bp)
    app.register_blueprint(picks_bp)
    app.register_blueprint(portfolio_bp)
    app.register_blueprint(portfolio_widgets_bp)
    app.register_blueprint(alpha_bp)

    @app.after_request
    def set_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' cdn.tailwindcss.com cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self'; "
            "connect-src 'self'; "
            "frame-ancestors 'none'"
        )
        if os.environ.get("FLASK_ENV") == "production" or not app.debug:
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )
        return response

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
