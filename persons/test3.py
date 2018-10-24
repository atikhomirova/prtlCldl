def trim_whitespaces(dct, name):
    line = dct[name]
    dct[name] = line.strip()
    return dct


def remove_extra_whitespaces(dct, name):
    line = dct[name]
    dct[name] = ' '.join(line.split())
    return dct


def skip_row_by_value(dct, name, arg):
    if arg in dct[name]:
        return None
    else:
        return dct


def call_function_from_str(str_function, dct, name):
    try:
        function, arg = str_function.split(',') #How an argument should be put in config?
        dct = eval(function)(dct, name, arg)
    except:
        function = str_function
        dct = eval(function)(dct, name)

    return dct


config = {'FirstName': ['trim_whitespaces'],
          'LastName': ['trim_whitespaces'],
          'Address': ['trim_whitespaces', 'remove_extra_whitespaces', 'skip_row_by_value,42']}


def process():
    dct = {}
    dct["ID"] = 1
    dct["FirstName"] = '    _Anastasiya_       '
    dct["LastName"] = '  _Tsikhamirava_   '
    dct["Address"] = '       _P    s       3       2     42_                '


    for name in config.keys():
        functions = config[name]
        for function in functions:
            if dct is not None:
                dct = call_function_from_str(function, dct, name)
                print(dct)

    return [dct]

d = process()
print(d)