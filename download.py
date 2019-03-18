import time
import os
from click import progressbar
from mechanicalsoup import StatefulBrowser
from re import compile as re_compile
from requests import get as requests_get

def single_file(search_tag, filename):
    filename = str(filename)
    root = search_tag.find_all('a', {'title': re_compile(filename)})

    urls = list()
    for elem in root:
        li = elem.find_parent('li')
        data_format = elem.parent.find('span').attrs['data-format']
        name = li.find('p', {'class' : 'description'}).text.strip()
        url = li.find('i', { 'class' : 'icon-download-alt'}) \
                  .parent.attrs['href']

        if name == '':
            f_name = filename.strip().replace(' ', '_')
            name = f'{f_name}.{data_format}'
        urls.append((name, url))

    return urls


def many_files(search_tag):
    urls = list()
    for elem in search_tag.find_all('i', { 'class' : 'icon-download-alt'}):
        root = elem.find_parent('li')
        name = root.find('p', {'class' : 'description'}) \
                   .find(text=True).strip()

        data_format = elem.parent.attrs['data-format']
        url = elem.parent.attrs['href']

        if name == '':
            f_name = root.find('a', {'class' : 'heading'}) \
                         .find(text=True) \
                         .strip() \
                         .replace(' ', '_')
            name = f'{f_name}.{data_format}'

        urls.append((name, url))
    return urls


def retreive_download_url(url, filename=None):
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
        raise Exception('There was something wrong')


def download_file(destine, filename, url):
    response = requests_get(url, stream=True)
    f_path = os.path.join(destine, filename)
    length = int(response.headers.get('Content-Length', 10**4))
    f = open(f_path, 'wb')

    with progressbar(label=f'Downloading {filename}', length=length) as bar:
       for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
            bar.update(len(chunk))

    f.close()
