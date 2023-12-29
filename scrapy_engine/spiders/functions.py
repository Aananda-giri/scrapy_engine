import langid
import re
import os


from pathlib import Path
from urllib.parse import urlparse





def get_resume_urls(domain_name_to_resume_from):
    '''
    -------------
     pseudocode:
    -------------

    domain_name_to_resume_from = ekantipur

    while there is file ['ekantipur.json', 'ekantipur_0.json', 'ekantipur_1.json', ...]:
        urls_to_visit.add(to_visited_urls)
        urls_visited.add(visited_urls)
    
    fro url in urls_to_visit:
        add url to news_urls if url is not present in urls_visited
    
    return list(news_urls)
    '''
    
    new_file_name = domain_name_to_resume_from + '.json'
    
    urls_to_visit = set()
    urls_visited = self.visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)
    news_start_ulrs = set()
    
    while os.path.exists(new_file_name):
        try:
            with open(new_file_name, 'r') as file:
                data = json.load(file)
        except:
            '''
               file is corrupt when scrapy is terminated while it is still crawling.
               while corrupt, file is terminated with: `{dome_data},`
               adding : `""]` at the end of file to make it valid json file

            '''
            with open('log_file.json', 'a') as file:
                file.write("\"\"]")
            
            with open(new_file_name, 'r') as file:
                data = json.load(file)
        for each_data in data:
            if type(each_data) == dict and 'to_visit' in each_data.keys():
                urls_to_visit.add(each_data['to_visit'])  # each_data['to_visit'] is a of urls
            
            if type(each_data) == dict and 'visited' in data.keys():
                urls_visited.add(each_data['visited'])  # each_data['visited'] is a url
        
        new_file_name = domain_name + f'_{index}.json'
        index += 1
    
    for to_visit in urls_to_visit:
        if to_visit not in urls_visited:
            news_start_ulrs.add(to_visit)
    
    return news_start_ulrs


def is_nepali_language(text):
    lang, confidence = langid.classify(text)
    return lang == 'hi', confidence
    # is_nepali("बल्ल बुझायो एनसेलले खरिद-बिक्री विवरण")

def is_google_drive_link(link):
        # return id of drive link

        if link.startswith('https://drive.google.com/file/d/'):
            drive_id = link.split('https://drive.google.com/file/d/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/folders/'):
            drive_id = link.split('https://drive.google.com/drive/folders/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/open?id='):
            drive_id = link.split('https://drive.google.com/open?id=')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/u/0/folders/'):
            drive_id = link.split('https://drive.google.com/drive/u/0/folders/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/u/1/folders/'):
            drive_id = link.split('https://drive.google.com/drive/u/1/folders/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/u/2/folders/'):
            drive_id = link.split('https://drive.google.com/drive/u/2/folders/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/u/3/folders/'):
            drive_id = link.split('https://drive.google.com/drive/u/3/folders/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/u/0/file/d'):
            drive_id = link.split('https://drive.google.com/drive/u/0/file/d/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/u/1/file/d/'):
            drive_id = link.split('https://drive.google.com/drive/u/1/file/d/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/u/2/file/d/'):
            drive_id = link.split('https://drive.google.com/drive/u/2/file/d/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/u/3/file/d/'):
            drive_id = link.split('https://drive.google.com/drive/u/3/file/d/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/drive/mobile/folders/'):
            drive_id = link.split('https://drive.google.com/drive/mobile/folders/')[1].split('/')[0].split('?')[0]
        elif link.startswith('https://drive.google.com/folderview?id='):
            drive_id = link.split('https://drive.google.com/folderview?id=')[1].split('/')[0].split('?')[0]
        else:
            # return original link
            return False, link
        
        return True, drive_id

def is_same_domain(url1, url2):
    return urlparse(url1).netloc == urlparse(url2).netloc

def is_np_domain(url):
    # return true if it is one of first 1000 .np domain
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    
    # crawl it if .np domain and len(visited_urls_base) < 1000
    
    return base_url[-3:] == '.np'

def should_we_crawl_it(new_url, visited_urls_base=None):
    # return true if it is one of first 1000 .np domain
    parsed_url = urlparse(new_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    
    # crawl it if .np domain and len(visited_urls_base) < 50
    
    if base_url[-3:] == '.np' and (visited_urls_base==None or len(visited_urls_base)<50):
        return base_url, True
    else:
        return base_url, False


# import re
def is_social_media_link(link):
    social_media_patterns = {
        'facebook': r"facebook\.com",
        'instagram': r"instagram\.com",
        'twitter': r"twitter\.com",
        'x': r"x\.com",
        'tiktok': r"tiktok\.com",
        'linkedin': r"linkedin\.com",
        'pinterest': r"pinterest\.com",
        'youtube': r"youtube\.com|youtu\.be",
        'reddit': r"reddit\.com",
        'snapchat': r"snapchat\.com",
        'quora': r"quora\.com",
        'tumblr': r"tumblr\.com",
        'flickr': r"flickr\.com",
        'medium': r"medium\.com",
        'vimeo': r"vimeo\.com",
        'vine': r"vine\.co",
        'periscope': r"periscope\.tv",
        'meetup': r"meetup\.com",
        'mix': r"mix\.com",
        'soundcloud': r"soundcloud\.com",
        'behance': r"behance\.net",
        'dribbble': r"dribbble\.com",
        'vk': r"vk\.com",
        'weibo': r"weibo\.com",
        'ok': r"ok\.ru",
        'deviantart': r"deviantart\.com",
        'slack': r"slack\.com",
        'telegram': r"t\.me|telegram\.me",
        'whatsapp': r"whatsapp\.com",
        'line': r"line\.me",
        'wechat': r"wechat\.com",
        'kik': r"kik\.me",
        'discord': r"discord\.gg",
        'skype': r"skype\.com",
        'twitch': r"twitch\.tv",
        'myspace': r"myspace\.com",
        'badoo': r"badoo\.com",
        'tagged': r"tagged\.com",
        'meetme': r"meetme\.com",
        'xing': r"xing\.com",
        'renren': r"renren\.com",
        'skyrock': r"skyrock\.com",
        'livejournal': r"livejournal\.com",
        'fotolog': r"fotolog\.com",
        'foursquare': r"foursquare\.com",
        'cyworld': r"cyworld\.com",
        'gaiaonline': r"gaiaonline\.com",
        'blackplanet': r"blackplanet\.com",
        'care2': r"care2\.com",
        'cafemom': r"cafemom\.com",
        'nextdoor': r"nextdoor\.com",
        'kiwibox': r"kiwibox\.com",
        'cellufun': r"cellufun\.com",
        'tinder': r"tinder\.com",
        'bumble': r"bumble\.com",
        'hinge': r"hinge\.co",
        'match': r"match\.com",
        'okcupid': r"okcupid\.com",
        'zoosk': r"zoosk\.com",
        'plentyoffish': r"pof\.com",
        'eharmony': r"eharmony\.com",
        'coffee_meets_bagel': r"coffeemeetsbagel\.com",
        'her': r"weareher\.com",
        'grindr': r"grindr\.com",
        'happn': r"happn\.com",
        'hily': r"hily\.com",
        'huggle': r"huggle\.com",
        'jdate': r"jdate\.com",
        'lovoo': r"lovoo\.com",
        'meetmindful': r"meetmindful\.com",
        'once': r"once\.com",
        'raya': r"raya\.app",
        'ship': r"getshipped\.com",
        'silversingles': r"silversingles\.com",
        'tastebuds': r"tastebuds\.fm",
        'the_league': r"theleague\.com",
        'tudder': r"tudder\.com",
        'twoo': r"twoo\.com",
    }

    for social_media, pattern in social_media_patterns.items():
        if (re.search(r'https://www\.' + pattern, link, flags=re.IGNORECASE)) or ((re.search(r'https://www\.m\.' + pattern, link, flags=re.IGNORECASE))):
            return True, social_media

    return False, None

# is_social_media_link('https://www.facebook.com/sth.s/ste') # (True, 'facebook')
# is_social_media_link('https://www.m.youtube.com/')# (True, 'youtube') # (True, 'youtube')
# is_social_media_link('https://www.google.com/search?q=youtube.com') # (False, None)


def is_document_link(link):    
    # document extensions
    document_extensions = ['.pdf', '.odt', '.docx', '.doc', '.pst', '.ai', '.drw', '.dxf', '.eps', '.ps', '.svg', '.cdr', '.odg']
    # presentation extensions
    presentation_extensions = ['.ppt', '.pptx', '.odp', '.pps']
    # spreadsheet extensions
    spreadsheet_extensions = ['.xls', '.xlsx', '.ods']
    # archive extensions
    archive_extensions = ['.zip', '.rar', '.tar', '.gz', '.7z', '.7zip', '.bz2', '.tar.gz', '.xz']
    # video extensions
    video_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.mng', '.mpg', '.qt', '.rm', '.swf', '.m4v', '.webm']
    # audio extensions
    audio_extensions = ['.mp3', '.wav', '.ogg', '.wma', '.ra', '.aac', '.mid', '.au', '.aiff', '.3gp', '.asf', '.asx', '.m4a']
    # image extensions
    image_extensions = ['.bmp', '.gif', '.jpg', '.jpeg', '.png', '.tif', '.tiff', '.ico', '.webp', '.mng', '.pct', '.psp']
    # other extensions
    other_extensions = ['.css', '.exe', '.bin', '.rss', '.dmg', '.iso', '.apk']

    extensions = {
        'document': document_extensions,
        'presentation': presentation_extensions,
        'spreadsheet': spreadsheet_extensions,
        'archive': archive_extensions,
        'video': video_extensions,
        'audio': audio_extensions,
        'image': image_extensions,
        'other': other_extensions
    }

    for doc_type, extension_list in extensions.items():
        for extension in extension_list:
            if link.lower().endswith(extension):
                return True, doc_type, (doc_type=='other')   # doc_type == 'image'  ignore image or other docs
    return False, None, True
# is_document_link('https://acem.edu.np/uploads/userfiles/files/old_question/8th%20Semester/BCTBEXBEL/Wind%20Energy%202.PDF')
# is_document, document_type = is_document_link('https://www.google.com/link.pdf')