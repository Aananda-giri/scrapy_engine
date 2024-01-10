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

if 'REPL_SLUG' in os.environ:
    print("Code is running in Replit!")
    from flask_app import keep_alive
    keep_alive()

print("running.....")
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

def remove_file_if_empty(file_path):
    """Checks if a file is empty and removes it if it is.

    Args:
        file_path (str): The path to the file to check.

    Returns:
        bool: True if the file was empty and removed, False otherwise.
    """
    if os.path.exists(file_path) and os.path.getsize(file_path) == 0:
        try:
            os.remove(file_path)
            print(f"Removed empty file: {file_path}")
            return True
        except OSError as e:
            print(f"Error removing file: {e}")
            return False
    else:
        print(f"File is not empty or does not exist: {file_path}")
        return False

def get_file_name(url):
    parsed_url = urlparse(url)
    domain_name = parsed_url.netloc   # https://www.example.com
    file_name = domain_name + '.json'
    
    index = 0
    remove_file_if_empty(file_name)

    while os.path.exists(file_name):
        file_name = domain_name + f'_{index}.json'
        index += 1
    
    remove_file_if_empty(file_name)
    
    return file_name, domain_name

    # print(domain_name)
    # return domain_name.split('.')[1]  # example

def run_scrapy_with_new_start_url(new_start_url, file_name, domain_name_to_resume_from):
    
    spider_name = 'ekantipur'
    if file_name == domain_name_to_resume_from + '.json':
        # new crawling. not resuming
        command = ['scrapy', 'crawl', spider_name, '-O', file_name, '-a', 'start_url=' + new_start_url]
        print(f'\n\n -------------- \n command:{command}\n ------------------------------')
        subprocess.run(command)
    else:
        # resuming
        
        command = ['scrapy', 'crawl', spider_name, '-O', file_name, '-a', 'start_url=' + new_start_url, '-a', 'domain_name_to_resume_from=' + domain_name_to_resume_from]
        
        subprocess.run(command)
    # scrapy crawl ekantipur -O ujyaaloonline.com.json -a start_url="https://ujyaaloonline.com/"
    # scrapy crawl ekantipur -O nepalipost.com.json -a start_url="https://www.ekantipur.com" -a domain_name_to_resume_from="nepalipost.com"
    # !scrapy crawl ekantipur  -O file_name.json -a start_url="https://www.ekantipur.com" -a domain_name_to_resume_from="ekantipur"
    # output to file to resume later
    # subprocess.run(['scrapy', 'crawl', spider_name, '-O', '/content/' + file_name, '-a', 'start_url=' + new_start_url])

# Example usage:
new_start_url = get_one_start_url()

while new_start_url != []:
    print(f'\n\n new_start_url:{new_start_url} \n\n')
    # new_start_url = 'https://onlinetvnepal.com/'
    start_file_name, domain_name = get_file_name(new_start_url)
    print(f'\n\n file_name: {start_file_name} \n\n')

    if not 'google' in os.uname().release:
        # running locally

        try:
            # run scrapy
            run_scrapy_with_new_start_url(new_start_url, start_file_name, domain_name)
            
            # remove url crawled from start_urls
            remove_one_url(new_start_url)
        except Exception as Ex:
            print(f"\n-------------------\n Exceptino1: {Ex} \n ------------------------ \n")
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