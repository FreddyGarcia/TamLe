import pandas
import sys
from os import mkdir as os_mkdir



def unzip(filename):
    with ZipFile(filename,"r") as zip_ref:
        zip_ref.extractall(".")


def create_folder_structure():
    os_mkdir('downloaded_files')


def generate_sql(dataframe, table_name):
    df_columns = identify_colummns_types(dataframe)
    sql = pandas.io.sql.get_schema(dataframe,
                                   table_name,
                                   dtype=df_columns)
    return sql


def main():
    import argparse
    cod = lambda x: print(f'hello')

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('integers', metavar='N', type=int, nargs='+',
                        help='an integer for the accumulator')
    parser.add_argument('--sum', dest='accumulate', action='store_const',
                        const=cod, default=max,
                        help='sum the integers (default: find the max)')

    args = parser.parse_args()
    print (args.accumulate(args.integers))


if __name__ == '__main__':
    main()
