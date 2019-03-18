import os
import pandas
import xml.etree.ElementTree as et
from ast import literal_eval
from dateparser import parse as dateparse
from json import loads as json_decode
from rdflib import Graph

def dataframe_from_json(filename):
    try:
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
    except Exception as e:
        return None


def dataframe_from_xml(filename):

    try:
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
    except Exception as e:
        return None


def dataframe_from_rfd(filename):
    try:
        # rdflib reader
        g = Graph()
        # convert into row list
        r = g.parse(filename)
        # finally, create the dataframe
        dataframe = pandas.DataFrame(r)

        return dataframe
    except Exception as e:
        return None


def read_file(filename):
    '''
        Given the input file, generate a dataframe depeding on the file type
    '''
    file_type = filename.rpartition('.')[-1]

    if file_type in ('xlsx', 'xls') :
        dataframe = pandas.read_excel(filename)

    elif file_type == 'csv':
        dataframe = pandas.read_csv(filename)

    elif file_type == 'xml':
        dataframe = dataframe_from_xml(filename)

    elif file_type == 'json':
        dataframe = dataframe_from_json(filename)

    elif file_type == 'rdf':
        import ipdb; ipdb.set_trace(context=25)
        dataframe = dataframe_from_rfd(filename)

    elif file_type == 'zip':
        dataframe = None

    else:
        raise Exception('Unssuported File Type')

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

    _str = str(value).strip()
    # if 'nan' is received (the numpy None value)
    # there's nothing to do
    if _str == 'nan':
        return None
    elif any([x in _str for x in ('-', '/')]) \
        and len(_str) > 5 \
        and len(_str) < 9 \
        and dateparse(_str) is not None:
        return 'date'

    try:
        # try to cast str and then get value type
        return type(literal_eval(_str)).__name__
    except Exception as e:
        return 'str'


def identify_colummns_types(dataframe):

    _types = dict()
    df_partial = dataframe[:10]
    for column in df_partial:
        types = df_partial[column].apply(lambda x: guess_str_type(x)) \
                                  .drop_duplicates() \
                                  .to_list()
        _types[column] = choose_type_priority(types)

    return _types


def basename(filename):
    base = os.path.basename(filename)
    return base.split('.')


def generate_sql(dataframe, table_name):
    df_columns = identify_colummns_types(dataframe)
    sql = pandas.io.sql.get_schema(dataframe,
                                   table_name,
                                   dtype=df_columns)
    return sql


def write_sql(dataframe, filename):
    b_name = os.path.basename(filename)
    sql = generate_sql(dataframe, b_name)

    with open(f'{filename}.sql', 'w') as f:
        f.write(sql)
