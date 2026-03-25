import jsonpickle
import domain

"""
Based on the premise that any object can be identified by its class name
and the value of a unique attribute (primary key).
'domain' is a local project module, NOT a PyPI package.
"""

def save(db, obj):
    db.set(obj.__class__.__name__, jsonpickle.encode(obj))

def read(db, class_name, identity):
    obj = getattr(domain, class_name)()
    return obj
