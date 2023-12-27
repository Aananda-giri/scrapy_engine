'''
# crawl newspaper links
for url in newspaper_urls:
  crawl
  save data to google drive
  remove url from newspaper_urls
'''

import subprocess
from urllib.parse import urlparse
import os
import json
import shutil

def get_one_start_url():
    with open("news_start_urls.json",'r') as file:
        urls = json.load(file)

    # one_url = urls.pop()
    return urls[0]

def remove_one_url(one_url):
    with open("news_start_urls.json",'r') as file:
        urls = json.load(file)
    if one_url in urls:
        urls.remove(one_url)

    with open("news_start_urls.json",'w') as file:
        urls = json.dump(urls, file)

    return one_url

def get_domain_name(url):
    parsed_url = urlparse(url)
    domain_name = parsed_url.netloc   # https://www.example.com
    return domain_name
    # print(domain_name)
    # return domain_name.split('.')[1]  # example

def run_scrapy_with_new_start_url(new_start_url, file_name):
    spider_name = 'ekantipur'
    subprocess.run(['scrapy', 'crawl', spider_name, '-a', 'start_url=' + new_start_url])
    # output to file to resume later
    # subprocess.run(['scrapy', 'crawl', spider_name, '-O', '/content/' + file_name, '-a', 'start_url=' + new_start_url])

# Example usage:
new_start_url = get_one_start_url()

while new_start_url != []:
    print(f'\n\n new_start_url:{new_start_url} \n\n')
    # new_start_url = 'https://onlinetvnepal.com/'
    file_name = get_domain_name(new_start_url) + '.json'

    if not 'google' in os.uname().release:
        # running locally

        # run scrapy
        run_scrapy_with_new_start_url(new_start_url, file_name)
        
        # remove url crawled from start_urls
        remove_one_url(new_start_url)

        # get new start url
        new_start_url = get_one_start_url()
        
    else:
        # running on google colab
        
        # continue if already crawled
        if os.path.exists('/content/drive/MyDrive/scrapy_engine/crawled_data/'+file_name):
            # remove url crawled from start_urls
            remove_one_url(new_start_url)
            continue

        # run scrapy
        run_scrapy_with_new_start_url(new_start_url, file_name)

        # copy crawled data to google drive
        if os.path.exists('/content/drive/MyDrive/scrapy_engine/crawled_data'):
            shutil.copy('/content/'+file_name, '/content/drive/MyDrive/scrapy_engine/crawled_data/'+file_name)

            # copy updated start_urls to google drive
            shutil.copy('/content/scrapy_engine/news_start_urls.json', '/content/drive/MyDrive/scrapy_engine/news_start_urls.json')

            # remove url crawled from start_urls
            remove_one_url(new_start_url)

            # get new start url
            new_start_url = get_one_start_url()

        else:
            print("\n\n google drive not mounted \n\n")
            break