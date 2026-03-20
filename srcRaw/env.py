# env.py
import os

variables = {}

_DEFAULT_ENV = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")


def load(path=None):
    if path is None:
        path = _DEFAULT_ENV
    for line in open(path, "r"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        variables[key] = value

def get(key):
    return variables.get(key)
