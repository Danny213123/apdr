import json


def render_message(name):
    return json.dumps({"hello": name})
