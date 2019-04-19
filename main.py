'''
    This script download files from `https://catalog.data.gov/dataset`,
    convert them to csv and generate the sql create table script.

    ALLOWED FILES EXTENSION = XLS XLSX JSON XML RDF ZIP

    To run this script please install the dependencies
    in the requirements.txt file:

    `$ pip install -r requirements.txt`

    author: Freddy Garcia Abreu
    email: freddie-wpy@outlook.es
    date: 03/24/2019
'''

import pandas
import os
import math
import numpy as np
from csv import QUOTE_ALL
from datetime import datetime
from argparse import ArgumentParser
from ast import literal_eval
from dateparser import parse as dateparse
from json import loads as json_decode
from mechanicalsoup import StatefulBrowser
from os.path import join as path_join
from rdflib import Graph
from re import compile as re_compile
from requests import get as requests_get
from xml.etree import ElementTree as et
from zipfile import ZipFile

# hide pandas warnings (all warnings in general)
import warnings
warnings.filterwarnings('ignore')


ALLOWED_EXTS = ('xls', 'xlsx', 'json', 'xml', 'rdf')
ERROR_FILE_NAME = 'bad_files.txt'
SAMPLE_DATA = 150
date_expected_format = re_compile(r"^\d{2}(?:\/|\-)\d{2}(?:\/|\-)(?:\d{4}|\d{2})$")

def is_valid_date(str_date):
    return (date_expected_format
                .match(str_date) is not None
        )

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
        # read the xml file
        xtree = et.parse(filename)
        # get the root node
        xroot = xtree.getroot()[0]

        dataframe = pandas.DataFrame()

        # iterate for root node children
        for node in xroot:
            row = []
            for elem in node.getchildren():
                if elem is not None:
                    row.append(elem.text)

            # extract tags from nodes
            df_cols = list(map(lambda c: c.tag, node.getchildren()))
            # create pandas serie with nodes row
            pd_serie = pandas.Series(row, index=df_cols)
            # add row to dataframe
            dataframe = dataframe.append(pd_serie, ignore_index = True)

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
        dataframe = pandas.read_excel(filename, dtype=str)

    elif file_type == 'csv':
        dataframe = pandas.read_csv(filename, dtype=str)

    elif file_type == 'xml':
        dataframe = dataframe_from_xml(filename)

    elif file_type == 'json':
        dataframe = dataframe_from_json(filename)

    elif file_type == 'rdf':
        dataframe = dataframe_from_rfd(filename)

    else:
        dataframe = None

    return dataframe


def export_csv(dataframe, output_name):
    '''
        Export given dataframe to csv
    '''
    dataframe.to_csv(output_name + '.csv', encoding='utf-8', index=False,
                     header=False, quotechar='"', quoting=QUOTE_ALL)


def choose_type_priority(types):
    if 'str' in types:
        return 'VARCHAR'

    elif 'float' in types:
        return 'DECIMAL'

    elif 'int' in types:
        return 'INT'

    elif any(['format' in str(x) for x in types]):
        val = next((x for x in types if 'format' in x), '')
        return f'DATETIME {val}'

    else:
        return 'VARCHAR'


def to_sql_field(type, field_max=None):
    '''
        The columns may look having multple types
        so we choose the most relevant.
    '''

    precision = f'({field_max})' if field_max is not None else ''

    return f'{type} {precision}'


def guess_str_type(value):
    '''
        based in the given value, guess the value type
    '''

    _str = str(value).strip()

    try:
        # if 'nan' is received (the numpy None value)
        # there's nothing to do
        if _str == 'nan':
            return 'str'
        elif is_valid_date(_str):
            return f"format '{str_to_frmt(_str)}'"
    except Exception as e:
        pass

    try:
        # trying to figure out value data type
        return type(literal_eval(_str)).__name__
    except Exception as e:
        # can't cast? we assume it as string
        return 'str'


def decimal_frmt(value):
    '''
        Based on a decimal value, get the current sql lenght code,
        so 23.234 is translated as 5,3
    '''
    _str = str(value).replace('-','')
    i = _str.index('.')
    int_ = len(_str[:i])
    float_ = len(_str[i+1:])
    total_ = int_ + float_
    cad = f'{total_},{float_}'
    return cad


def identify_colummns_types(dataframe):
    '''
        Iterate each dataframe column to get its types
    '''

    _types = []
    # perform the identification process with first 150 rows
    df_partial = dataframe[:SAMPLE_DATA]

    for column in df_partial:
        # get types
        types = df_partial[column].apply(lambda x: guess_str_type(x)) \
                                  .drop_duplicates() \
                                  .to_list()

        try:
            values = list(df_partial[column]
                            .fillna('0')
                            .sort_values(ascending=False)
                        )

            max_values =  list(map(lambda x: len(x), values))
            max_posi = max(max_values)
            max_index = max_values.index(max_posi)
            max_value = values[max_index]
            max_lenght = len(max_value)
        except Exception as e:
            pass

        type_ = choose_type_priority(types)

        if type_ == 'DECIMAL':
            try:
                precision = decimal_frmt(max_value)
            except Exception as e:
                pass

        elif type_ != 'DATETIME':
            precision = max_lenght

        else:
            precision = None

        sql_field = to_sql_field(type_, precision)

        column = str(column).replace(' ', '_') \
                            .replace('-', '_') \
                            .replace('.', '') \
                            .replace('?', '')

        _types.append(f'{column} {sql_field}')

    # putting all together
    columns = ',\n\t'.join(_types)

    return f'''(
    {columns}
)'''


def filename_and_ext(filename):
    '''
        Split filename in basename and extension
    '''

    base = os.path.basename(filename)
    return base.split('.')[:1] + base.split('.')[-1:]


def write_sql(dataframe, filename):
    '''
        Save sql script and write it in filename
    '''

    sql = identify_colummns_types(dataframe)

    with open(f'{filename}.sql', 'w') as f:
        f.write(sql)


def single_file(search_tag, filename):
    '''
        Get files with filename included.

        Because in the page could appear same file name multiple times,
        we will get all files with same file name.
    '''

    filename = str(filename)
    root = search_tag.find_all('a', {'title': re_compile(filename)})
    urls = list()

    # iterate over the page content
    for elem in root:
        li = elem.find_parent('li')
        data_format = elem.parent.find('span').attrs['data-format']

        name = li.find('a', {'class' : 'heading'}) \
                     .find(text=True) \
                     .strip() \
                     .replace(' ', '_')

        url = li.find('i', { 'class' : 'icon-download-alt'}) \
                  .parent.attrs['href']

        if name == '':
            name = filename.strip().replace(' ', '_')

        name = f'{name}.{data_format}'
        urls.append((name, url))

    return urls


def many_files(search_tag):
    '''
        Get all file urls in the page body
    '''
    urls = list()
    for elem in search_tag.find_all('i', { 'class' : 'icon-download-alt'}):
        root = elem.find_parent('li')
        name = root.find('a', {'class' : 'heading'}) \
                     .find(text=True) \
                     .strip() \
                     .replace(' ', '_')

        # get file extension
        data_format = elem.parent.attrs['data-format']
        # download url
        url = elem.parent.attrs['href']

        # the file extension is often included
        # if not, then add it
        if f'.{data_format}' not in name:
            name = f'{name}.{data_format}'

        urls.append((name, url))

    return urls


def retreive_download_url(url, filename=None):
    '''
        Retrieve files url from page body.

        If filename is given, filter by it.
    '''

    try:
        br = StatefulBrowser()
        response = br.open(url)
        soup = response.soup
        search_tag = soup.find('ul', {'class' : 'resource-list'})
        title = soup.find('h1', {'itemprop' : 'name'}).text.strip()

        if filename is None:
            urls = many_files(search_tag)
        else:
            urls = single_file(search_tag, filename)
        return title, urls

    except Exception as e:
        raise Exception('Bad URL')


def download_file(destine, filename, url):
    '''
        Download file from url and place it in given path.
    '''

    response = requests_get(url, stream=True)
    f_path = path_join(destine, filename)

    # we expect a different content type
    if 'text/html' in response.headers.get('Content-Type', 'text/html') :
        return False

    # write file by chunks
    f = open(f_path, 'wb')
    for chunk in response.iter_content(chunk_size=1024):
        f.write(chunk)
    f.close()

    return True


def create_folder_structure(root_folder_name):
    '''
        Given the page title, create the folder structure to save the results.
    '''

    if not os.path.exists(root_folder_name):
        os.mkdir(root_folder_name)
        os.mkdir(path_join(root_folder_name, 'download'))
        os.mkdir(path_join(root_folder_name, 'csv'))
        os.mkdir(path_join(root_folder_name, 'sql'))


def arguments():
    '''
        Parse arguments, -h for help
    '''
    parser = ArgumentParser(description='Retrieve files from http://catalog.data.gov'
                                     ' and convert them to csv')
    parser.add_argument('url', type=str, help='site url')
    parser.add_argument('--filename', '-f', nargs='?',
                        help='specify filename pattern to download')

    return parser.parse_args()


def log_unssuported(title, filename):
    with open(ERROR_FILE_NAME, 'a+') as f:
        f.write(filename)
        f.write('\n')
    print(f'\t # Bad File')


def process_zip(title, filename):
    '''
        Given the zip file, extract valid files and process them.
    '''

    # download folder
    dwn_dest = path_join(title, 'download')

    f_zip = ZipFile(path_join(dwn_dest, filename))
    z_name, z_ext = filename_and_ext(filename)

    # create folder to extract
    if not os.path.exists(path_join(dwn_dest, z_name)):
        os.mkdir(path_join(dwn_dest, z_name))


    # given each file..
    for f_name in f_zip.namelist():
        b_name, b_ext = filename_and_ext(f_name)

        # .. if valid file, then extract it and process it
        if b_ext in ALLOWED_EXTS:
            f_zip.extract(f_name, path_join(dwn_dest, z_name))
            process_file(title, path_join(dwn_dest, z_name, f_name))


def process_file(title, filename):
    '''
        Given the file path and filename, export to csv and sql.
    '''

    b_name, b_ext = filename_and_ext(filename)

    # folders
    csv_dest = path_join(title, 'csv')
    sql_dest = path_join(title, 'sql')
    dwn_dest = path_join(title, 'download')

    df = read_file(path_join(dwn_dest, filename))
    df = df.replace({'PrivacySuppressed': np.nan})

    import ipdb; ipdb.set_trace(context=20)
    if df is not None:
        print('\tExporting to csv')
        export_csv(df, path_join(csv_dest, b_name))

        print('\tExporting to sql')
        write_sql(df, path_join(sql_dest, b_name))

    else:
        log_unssuported(title, filename)


def sanity_name(filename):
    '''
        Remove invalid characters for file name.
    '''

    to_replace = r'\/:?*|"'
    for chr_ in to_replace:
        filename = filename.replace(chr_, '')
    return filename


def str_to_frmt(str):
    '''
        Based on a datetime, get the current date format,
        so 21/02/2019 is translated as dd/mm/yyyy
    '''
    date_formats = ["%Y-%m-%d", "%Y-%d-%m", "%m-%d-%Y", "%m-%d-%y",
                    "%d-%m-%Y", "%Y/%m/%d", "%Y/%d/%m", "%m/%d/%Y",
                    "%m/%d/%y", "%d/%m/%Y", "%Y.%m.%d", "%Y.%d.%m",
                    "%m.%d.%Y", "%d.%m.%Y"]
    match = []
    for fmt in date_formats:
        try:
            datetime.strptime(str, fmt)
        except ValueError as e:
            continue

        match.append(fmt)
        break

    try:
        date_fmt = match[0]
        date_fmt = date_fmt.replace('%d', 'dd') \
                           .replace('%Y', 'yyyy') \
                           .replace('%y', 'yy') \
                           .replace('%m', 'mm')
        return date_fmt
    except Exception as e:
        return None


def main():
    args = arguments()

    url = args.url
    filename = args.filename
    title, urls = retreive_download_url(url, filename)

    title = sanity_name(title)
    create_folder_structure(title)
    dwn_dest = path_join(title, 'download')

    for file_info in urls:

        f_name = sanity_name(file_info[0])
        b_name, b_ext = filename_and_ext(f_name)
        f_name = '.'.join([b_name, b_ext])
        url = file_info[1]

        print(f'\nProcessing "{f_name}"')
        print('\tDownloading')
        success = download_file(dwn_dest, f_name, url)

        if success:
            if b_ext == 'zip':
                process_zip(title, f_name)
            else:
                process_file(title, f_name)
        else:
            log_unssuported(title, f_name)


if __name__ == '__main__':
        main()
