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

1) dct is none
2) name is none
3) name not in dct
4) name[dct] is None
5) name[dct] is ''
6) name[dct] contains only '   '
7) name[dct] doesn't contain '   '
8) name[dct] is ok

def remove_extra_whitespaces(dct, name):
    dct[name] = ' '.join(dct[name].split()) if dct.get(name) else ''
    return dct

1) dct is none
2) name is none
3) name not in dct
4) name[dct] is None
5) name[dct] is ''
6) name[dct] contains only '   '
7) name[dct] doesn't contain '   '
8) normal situation


def skip_row_with_value(dct, name, arg=''):
    return 'FILTERED' if (dct.get(name) and arg in dct.get(name)) else dct

1) dct is none
2) name is none
3) name not in dct
4) name[dct] is None
5) name[dct] is ''
6) arg is defaulf
7) arg is custom
8) normal situation


def skip_row_by_two_values(dct, name, arg):
    args = arg.split(',')
    if dct.get(name) == args[0] and dct.get(args[1]) == args[2]:
        return 'FILTERED'
    else:
        return dct

1) dct is none
2) name1, name2, value1, value2 is none
3) name1, name2 not in dct
4) name1[dct], name2[dct] is None
5) name1[dct], name2[dct] is ''
6) no arg 
7) arg is custom
8) normal situation

cleansing_functions = {'trim_whitespaces': trim_whitespaces,
                       'remove_extra_whitespaces': remove_extra_whitespaces,
                       'skip_row_with_value': skip_row_with_value,
                       'skip_row_by_two_values': skip_row_by_two_values
                   }