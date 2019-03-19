import os
import argparse
import download
import process
from zipfile import ZipFile
from os.path import join as path_join

# hide pandas warnings (all warnings in general)
import warnings
warnings.filterwarnings('ignore')


ALLOWED_EXTS = ('xls', 'xlsx', 'json', 'xml', 'rdf')


def create_folder_structure(root_folder_name):

    if not os.path.exists(root_folder_name):
        os.mkdir(root_folder_name)
        os.mkdir(path_join(root_folder_name, 'download'))
        os.mkdir(path_join(root_folder_name, 'csv'))
        os.mkdir(path_join(root_folder_name, 'sql'))


def arguments():
    parser = argparse.ArgumentParser(description='Retrieve files from http://catalog.data.gov'
                                     ' and convert them to csv')
    parser.add_argument('url', type=str, help='site url')
    parser.add_argument('--filename', '-f', nargs='?',
                        help='specify filename pattern to download')

    return parser.parse_args()


def log_unssuported(title, filename):
    print(f'\t # Bad File')


def process_zip(title, filename):
    dwn_dest = path_join(title, 'download')

    f_zip = ZipFile(path_join(dwn_dest, filename))
    z_name, z_ext = process.basename(filename)

    if not os.path.exists(path_join(dwn_dest, z_name)):
        os.mkdir(path_join(dwn_dest, z_name))

    for f_name in f_zip.namelist():
        b_name, b_ext = process.basename(f_name)

        if b_ext in ALLOWED_EXTS:
            f_zip.extract(f_name, path_join(dwn_dest, z_name))
            process_file(title, path_join(dwn_dest, z_name, f_name))


def process_file(title, filename):
    f_name = filename
    b_name, b_ext = process.basename(f_name)
    csv_dest = path_join(title, 'csv')
    sql_dest = path_join(title, 'sql')
    dwn_dest = path_join(title, 'download')

    df = process.read_file(path_join(dwn_dest, f_name))
    if df is not None:
        print('\tExporting to csv')
        process.export_csv(df, path_join(csv_dest, b_name))
        print('\tExporting to sql')
        process.write_sql(df, path_join(sql_dest, b_name))
    else:
        log_unssuported(title, f_name)


def sanity_name(filename):
    to_replace = r'\/:?*|"'
    for chr_ in to_replace:
        filename = filename.replace(chr_, '')
    return filename


def main():
    args = arguments()

    url = args.url
    filename = args.filename
    title, urls = download.retreive_download_url(url, filename)

    title = sanity_name(title)
    create_folder_structure(title)
    dwn_dest = path_join(title, 'download')

    for file_info in urls:

        f_name = sanity_name(file_info[0])
        b_name, b_ext = process.basename(f_name)
        f_name = '.'.join([b_name, b_ext])
        url = file_info[1]

        print(f'\nProcessing "{f_name}"')
        print('\tDownloading')
        success = download.download_file(dwn_dest, f_name, url)

        if success:
            if b_ext == 'zip':
                process_zip(title, f_name)
            else:
                process_file(title, f_name)
        else:
            log_unssuported(title, f_name)


if __name__ == '__main__':
    main()
1