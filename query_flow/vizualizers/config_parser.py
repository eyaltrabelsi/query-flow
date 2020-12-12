import json
from pathlib import Path


class ConfigParser(object):

    def __init__(self, defualt_colors, path='example_config.json'):
        config_path = Path(path)
        extra_config = json.load(config_path.open()) if config_path.exists() else {}

        for key, value in extra_config.items():
            if extra_config.get(key, None):
                defualt_colors[key] = extra_config[key]
        self.config = defualt_colors

    def __getitem__(self, key):
        return self.config[key]
