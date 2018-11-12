from __future__ import absolute_import


def call_function_from_str(str_function, dct, name):

    arg = None
    
    if ':' in str_function:
        function_name, arg = str_function.split(':') #How an argument should be put in config?
        arg = arg.strip()
    else:
        function_name = str_function

    try:
        function = cleansing_functions[function_name]
    except KeyError:
        print "Function not found"

    if arg:
        dct = function(dct, name, arg)
    else:
        dct = function(dct, name)

    return dct


def trim_whitespaces(dct, name):
    dct[name] = dct[name].strip() if dct.get(name) else ''
    return dct


def remove_extra_whitespaces(dct, name):
    dct[name] = ' '.join(dct[name].split()) if dct.get(name) else ''
    return dct


def skip_row_with_value(dct, name, arg):
    if dct.get(name) is not None and arg in dct.get(name):
        dct = None
    return dct
    
    
cleansing_functions = {'trim_whitespaces': trim_whitespaces,
                       'remove_extra_whitespaces': remove_extra_whitespaces,
                       'skip_row_with_value': skip_row_with_value
                   }