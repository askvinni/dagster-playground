import os

import yaml


def config_assets(assets, configs_folder):
    confs = {}
    for file in os.listdir(configs_folder):
        with open(os.path.join(configs_folder, file), "r") as f:
            confs[file] = yaml.safe_load(f.read())

    configured_assets = []
    for asset in assets:
        asset.config = confs[asset.name]
        configured_assets.append(asset)

    return configured_assets
