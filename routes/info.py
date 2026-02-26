"""Info page — glossary, how-to, and data sources."""

from flask import Blueprint, render_template

info_bp = Blueprint("info", __name__)


@info_bp.route("/info")
def info():
    return render_template("info.html")
