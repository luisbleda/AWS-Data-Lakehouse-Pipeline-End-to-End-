def get_value(config, path):
    keys = path.split(".")
    value = config
    for key in keys:
        value = value[key]
    return value
