import subprocess
import os
from functions import get_file_name

def get_url(domain_name):
    all_urls = ['https://khabarhub.com/', 'https://glocalkhabar.com/', 'https://ejanakpurtoday.com/', 'https://saptahik.com.np/', 'https://bigulnews.tv/', 'https://nayapage.com/', 'https://nepalpress.com/', 'https://farakdhar.com/', 'https://nepalsamaya.com/', 'https://www.ratopati.com/', 'https://www.janaaastha.com/', 'https://nepalitimes.com/', 'https://samacharpati.com/', 'https://imagekhabar.com/', 'https://www.nepalipatra.com/', 'https://setopati.com/', 'https://www.nepalipaisa.com/', 'https://annapurnapost.com/', 'https://bigulnews.com/', 'https://radiosagarmatha.org.np/', 'https://arthasarokar.com/', 'https://kathmandupress.com/', 'https://www.khojtalashonline.com/', 'https://www.dainiknepal.com/', 'https://www.chitawan.com/', 'https://onlinekhabar.com/', 'http://epaperhimalayadarpan.com/', 'https://kathmandutribune.com/', 'https://rajdhanidaily.com/', 'https://nepaliheadlines.com/', 'https://www.himalkhabar.com/', 'https://www.arghakhanchi.com/', 'https://everestdainik.com/', 'https://reportersnepal.com/', 'https://www.setopati.com/', 'https://www.hamrokhelkud.com/', 'https://www.koshionline.com/', 'https://realkhabar.net/', 'https://thehimalayantimes.com/', 'https://lokaantar.com/', 'https://emountaintv.com/', 'https://radionepal.gov.np/', 'https://hamrokhotang.com/', 'https://www.karobardaily.com/', 'https://dcnepal.com/', 'https://hamrakura.com/', 'https://www.eadarsha.com/', 'https://pahilopost.com/', 'https://aarthiknews.com/', 'https://onlinetvnepal.com/', 'https://www.himalini.com/', 'https://gorkhapatraonline.com/', 'https://www.newbusinessage.com/', 'https://nayapatrikadaily.com/', 'http://nepalipost.com/beta/', 'https://hellokhabar.com/', 'https://kendrabindu.com/', 'https://www.onlinekhabar.com/', 'https://onlinemajdoor.com/', 'https://kantipath.com/', 'https://dhangadhikhabar.com/', 'https://abhiyandaily.com/', 'https://www.goalnepal.com/', 'https://www.nayapatrikadaily.com/', 'https://www.khasokhas.com/', 'https://ekantipur.com/', 'https://baahrakhari.com/', 'https://www.nagariknetwork.com/', 'https://www.souryaonline.com/', 'https://www.onsnews.com/', 'https://nepalihimal.com/', 'http://www.saharasansar.com/', 'https://nagariknews.com/', 'https://ratopati.com/', 'https://www.peoplesreview.com.np/', 'https://sancharkendra.com/', 'https://www.newsofnepal.com/', 'https://www.nayapage.com/', 'https://www.osnepal.com/', 'https://hamrokhelkud.com/', 'https://nepallive.com/', 'https://topnepalnews.com/', 'https://www.news24nepal.tv/', 'https://lokpath.com/', 'https://risingnepaldaily.com/', 'https://samudrapari.com/', 'https://ujyaaloonline.com/', 'https://kathmandupost.com/', 'https://dainiknewsnepal.com/', 'https://www.bizshala.com/', 'https://bizkhabar.com/', 'https://www.harekpalnews.com/', 'https://himalkhabar.com/', 'https://www.bbc.com/nepali', 'https://shilapatra.com/', 'https://nagariknews.nagariknetwork.com/', 'https://kalakarmi.com/', 'https://thahakhabar.com/', 'https://nepalkhabar.com/']
    
    matching_urls = [url for url in all_urls if (domain_name in url) ]
    if matching_urls:
        return list(set(matching_urls))[0]
    else:
        return None



domain_name_to_resume_from = list(set([
      file.split('.json')[0].split('_')[0] for file in os.listdir() if (file.endswith('.json')) and (
          file not in
          ['test.json', 'news_start_urls copy.json', 'news_start_urls.json'])
]))

for domain_name in domain_name_to_resume_from:
    file_name, _ = get_file_name(url=None, domain_name = domain_name)
    # print(f'\n\n {file_name}')
    new_start_url = get_url(domain_name = domain_name)
    spider_name = 'ekantipur'
    if not new_start_url:
        continue
    
    command = ['scrapy', 'crawl', spider_name, '-O', file_name, '-a', 'start_url=' + new_start_url, '-a', 'domain_name_to_resume_from=' + domain_name]
    subprocess.run(command) 