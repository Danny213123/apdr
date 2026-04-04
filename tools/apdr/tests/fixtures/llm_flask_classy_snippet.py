"""Old-style Flask extension import using flask.ext.classy.
flask.ext.classy maps to Flask-Classy on PyPI."""
import flask
from flask.ext.classy import FlaskView, route
from functools import wraps

class WidgetView(FlaskView):
    @route('/widgets/')
    def index(self):
        return "Widgets"
