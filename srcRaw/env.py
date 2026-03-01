# env.py

variables = {}

def load(path=".env"):
    for line in open(path, "r"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        variables[key] = value

def get(key):
    return variables.get(key)
