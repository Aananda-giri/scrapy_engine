from scrapy_engine.spiders.functions import get_resume_urls



list(set([
      file.split('.json')[0].split('_')[0] for file in os.listdir() if (file.endswith('.json')) and (
          file not in
          ['test.json', 'news_start_urls copy.json', 'news_start_urls.json'])
]))


# resume_urls, visited_urls = get_resume_urls('www.nepalipaisa.com')