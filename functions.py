import os, json
from urllib.parse import urlparse
def get_file_name(url, domain_name = None):
    if not domain_name:
        # Get domain name from url
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

def get_one_start_url(domain_name=None):
    if not domain_name:
        # get start_url from 
        with open("news_start_urls.json",'r') as file:
            urls = json.load(file)

        # one_url = urls.pop()
        return urls[0]

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
