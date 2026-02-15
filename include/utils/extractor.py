# extract.py

def get_nested_value(data, key_path):
    """Safely get nested values from a dictionary using dot notation."""
    keys = key_path.split('.')
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data

def map_record_fields(record, mapping, defaults=None):
    """
    Map fields from a (possibly nested) record using a mapping dict.

    Args:
        record (dict): The source data record.
        mapping (dict): Dict where key = output field name, value = dot-notated path in record.
        defaults (dict, optional): Default values for output fields if not found or empty.

    Returns:
        dict: Mapped fields with empty values converted to None.
    """
    defaults = defaults or {}

    result = {}
    for out_key, in_path in mapping.items():
        value = get_nested_value(record, in_path)
        if value in ('', [], {}, None):
            result[out_key] = defaults.get(out_key) if defaults.get(out_key) is not None else None
        else:
            result[out_key] = value
    return result
