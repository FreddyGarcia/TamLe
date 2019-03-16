import pandas
from ast import literal_eval
from dateparser import parse as dateparse
from json import loads as json_decode
from rdflib import Graph
from zipfile import ZipFile


def dataframe_from_json(filename):
    # the file must be opened and parsed into a dict
    with open(filename) as f: json = json_decode(f.read())

    # extract the data attribute
    rows = json['data']
    # extract the columns meta data
    columns = json['meta']['view']['columns']
    # columns names in meta data
    column_names = list(map(lambda c: c['name'], columns))
    # finally, create the dataframe
    dataframe = pandas.DataFrame(rows, columns=column_names)

    return dataframe


def dataframe_from_xml(filename):
    import xml.etree.ElementTree as et

    xtree = et.parse(filename)
    xroot = xtree.getroot()[0]

    dataframe = pandas.DataFrame()

    for node in xroot:
        row = []
        for elem in node.getchildren():
            if elem is not None:
                row.append(elem.text)

        df_cols = list(map(lambda c: c.tag, node.getchildren()))
        dataframe = dataframe.append(pandas.Series(row, index=df_cols),
                                     ignore_index = True)

    return dataframe


def dataframe_from_rfd(filename):
    # rdflib reader
    g = Graph()
    # convert into row list
    r = g.parse('rows.rdf')
    # finally, create the dataframe
    dataframe = pandas.DataFrame(r)

    return dataframe


def read_file(filename, file_type):
    '''
        Given the input file, generate a dataframe depeding on the file type
    '''

    if file_type in ('xlsx', 'xls') :
        dataframe = pandas.read_excel(filename)

    elif file_type == 'csv':
        dataframe = pandas.read_csv(filename)

    elif file_type == 'xml':
        dataframe = dataframe_from_xml(filename)

    elif file_type == 'json':
        dataframe = dataframe_from_json(filename)

    elif file_type == 'rdf':
        dataframe = dataframe_from_rfd(filename)

    else:
        raise Exception('Bad Argument')

    return dataframe


def export_csv(dataframe, output_name):
    dataframe.to_csv(output_name + '.csv', encoding='utf-8', index=False)


def max_column_lenght(dataframe, column):
    df_column = df['sex']
    max_number = int(df_column.str.encode(encoding='utf-8').str.len().max())
    return max_number


def choose_type_priority(types):

    if 'str' in types:
        return 'VARCHAR(100)'

    elif 'float' in types:
        return 'DECIMAL(17,4)'

    elif 'int' in types:
        return 'INT'

    elif 'date' in types:
        return 'DATETIME'

    else:
        return 'VARCHAR(100)'


def guess_str_type(value):
    '''
        based in the given value, guess the value type
    '''

    _str = str(value)
    # if 'nan' is received (the numpy None value)
    # there's nothing to do
    if _str == 'nan':
        return None
    elif dateparse(_str) is not None:
        return 'date'

    try:
        # try to cast str and then get value type
        return type(literal_eval(_str)).__name__
    except Exception as e:
        return 'str'


def identify_colummns_types(dataframe):

    _types = dict()
    for column in dataframe:

        types = dataframe[column].apply(lambda x: guess_str_type(x)) \
                                    .drop_duplicates() \
                                    .to_list()
        _types[column] = choose_type_priority(types)

    return _types
