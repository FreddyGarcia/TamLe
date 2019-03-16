from mechanicalsoup import StatefulBrowser
from re import compile as re_compile
from requests import get as requests_get


def single_file(search_tag, filename):
    filename = str(filename)
    root = search_tag.find(text=re_compile(filename)) \
                      .find_parent('li')

    name = root.find('p', {'class' : 'description'}).text.strip()
    url = root.find(text=re_compile('Download')) \
              .parent.attrs['href']

    return [(name, url)]


def many_files(search_tag):
    urls = list()
    for elem in search_tag.find_all(text=re_compile('Download')):
        name = elem.find_parent('li') \
                   .find('p', {'class' : 'description'}) \
                   .find(text=True).strip()
        url = elem.parent.attrs['href']
        urls.append((name, url))
    return urls


def retreive_download_url(url, filename=None):
    try:
        br = StatefulBrowser()
        response = br.open(url)
        search_tag = response.soup.find('ul', {'class' : 'resource-list'})

        if filename is None:
            urls = many_files(search_tag)
        else:
            urls = single_file(search_tag, filename)

        return urls
    except Exception as e:
        raise Exception('There was something wrong')


def download_file(url, filename):
    response = requests_get(url, stream=True)
    f = open(filename, 'wb')

    for chunk in response.iter_content(chunk_size=128):
        f.write(chunk)

    f.close()
