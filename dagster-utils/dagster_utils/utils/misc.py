import os

import yaml


def logical_xor(*args):
    return sum([bool(arg) for arg in args]) == 1


def load_confs(path):
    confs = {}
    for filename in os.listdir(path):
        if filename.endswith(".yaml"):
            with open(os.path.join(path, filename)) as f:
                confs[filename.split(".")[0]] = yaml.safe_load(f)
    return confs
