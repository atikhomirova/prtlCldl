from __future__ import absolute_import

def trim_whitespaces(dct, name):
    line = dct[name]
    dct[name] = line.strip()
    return dct

def remove_extra_whitespaces(dct, name):
    line = dct[name]
    dct[name] = ' '.join(line.split())
    return dct

def skip_row_with_value(dct, name, arg):
    if arg in dct[name]:
        return None
    else:
        return dct