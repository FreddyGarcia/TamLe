from os.path import join as path_join
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

        name = li.find('a', {'class' : 'heading'}) \
                     .find(text=True) \
                     .strip() \
                     .replace(' ', '_')

        # name = li.find('p', {'class' : 'description'}).text.strip()
        url = li.find('i', { 'class' : 'icon-download-alt'}) \
                  .parent.attrs['href']

        if name == '':
            name = filename.strip().replace(' ', '_')

        name = f'{name}.{data_format}'
        urls.append((name, url))

    return urls


        # if name == '':
        #     name = root.find('p', {'class' : 'description'}) \
        #                .find(text=True).strip()
        #     name = f'{name}.{data_format}'
def many_files(search_tag):
    urls = list()
    for elem in search_tag.find_all('i', { 'class' : 'icon-download-alt'}):
        root = elem.find_parent('li')
        name = root.find('a', {'class' : 'heading'}) \
                     .find(text=True) \
                     .strip() \
                     .replace(' ', '_')

        data_format = elem.parent.attrs['data-format']
        url = elem.parent.attrs['href']

        if f'.{data_format}' not in name:
            name = f'{name}.{data_format}'

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
        raise Exception('Bad URL')


def download_file(destine, filename, url):
    response = requests_get(url, stream=True)
    f_path = path_join(destine, filename)
    length = int(response.headers.get('Content-Length', 10**4))

    if 'text/html' in response.headers.get('Content-Type', 'text/html') :
        return False

    f = open(f_path, 'wb')
    for chunk in response.iter_content(chunk_size=1024):
        f.write(chunk)

    f.close()

    return True
