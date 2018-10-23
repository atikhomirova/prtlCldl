class myTableRow:

    def __init__(self, row):
        self.row = row

    def trim_whitespaces(self, name):
        line = self.row[name]
        self.row[name] = line.strip()

    def remove_extra_whitespaces(self, name):
        line = self.row[name]
        self.row[name] = ' '.join(line.split())

    def skip_row_by_value(self, name):
        if '42' in self.row[name]:
            return None
        else:
            pass


config = {'FirstName': ('trim_whitespaces',), 
          'LastName': ('trim_whitespaces',),
          'Address': ('trim_whitespaces', 'remove_extra_whitespaces')}


def process():
    dct = {}
    dct["ID"] = 1
    dct["FirstName"] = '    _Anastasiya_       '
    dct["LastName"] = '  _Tsikhamirava_   '
    dct["Address"] = '       _Parnikovaya    s       3       2     42_                '

    row = myTableRow(dct)
    print(row)

    for name in config.keys():
        print(name)
        functions = config[name]
        for function in functions:
            print(function)
            method = getattr(row, function)
            print(method)
            row.method(name)

    return row

d = process()
print(d)