from __future__ import absolute_import


def call_function_from_str(str_function, value):

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
        return function(value, arg)
    else:
        return function(value)

#    return dct


def trim_whitespaces(value):
    return value.strip() if value else ''


def remove_extra_whitespaces(value):
    return ' '.join(value.split()) if value else ''


def skip_row_with_value(value, arg):
    return 'FILTERED' if value and arg in value else value


cleansing_functions = {'trim_whitespaces': trim_whitespaces,
                       'remove_extra_whitespaces': remove_extra_whitespaces,
                       'skip_row_with_value': skip_row_with_value
                   }