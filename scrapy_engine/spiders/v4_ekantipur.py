from .functions import is_social_media_link, is_nepali_language, is_document_link, is_google_drive_link, should_we_crawl_it, is_same_domain, is_np_domain, get_resume_urls
import scrapy
import pybloom_live


from pathlib import Path

import scrapy

import csv
import dotenv
import json
import os
import redis
import threading
import time
dotenv.load_dotenv()
from scrapy import signals, Spider
from scrapy.linkextractors import LinkExtractor

class EkantipurSpider(scrapy.Spider):
    name = "ekantipur_v4"
    crawled_data = []
    to_visit = []
    def publisher(self):
        # print(f'published: {self.crawled_data}')
        while not self.stop_publisher.is_set() or self.crawled_data or self.to_visit:
            print(f'crawled:{len(self.crawled_data)} redis:{self.redis_client.llen(self.name)}')
            
            # Save crawled data to redis
            if self.crawled_data:
                pushed = 0
                for _ in range(len(self.crawled_data)):
                    self.redis_client.lpush(self.name, json.dumps(self.crawled_data.pop()))
                    pushed += 1
                print(f'---published: {pushed}---')
            
            # Save to_visit urls to redis
            if self.to_visit:
                pushed = 0
                for _ in range(len(self.to_visit)):
                    self.redis_client.lpush('to_visit', self.to_visit.pop())
                    pushed += 1
                print(f'---to_visit: {pushed}---')
            
            time.sleep(1)  # sleep for a while
            
    def __init__(self, *args, **kwargs):
        super(EkantipurSpider, self).__init__(*args, **kwargs)

        self.redis_client = redis.Redis(
            host=os.environ.get('REDIS_HOST', 'localhost'),
            port = int(os.environ.get('REDIS_PORT', 6379)),
            password=os.environ.get('REDIS_PASSWORD', None),
        )
        self.stop_publisher = threading.Event()
        # Create producer and consumer threads
        self.publisher_thread = threading.Thread(target=self.publisher)
        # start the threads
        self.publisher_thread.start()
        
        self.start_urls = [kwargs.get('start_url')] 
        # using bloom filter to store visited urls for faster lookup
        self.visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)    # pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH means
        domain_name_to_resume_from = kwargs.get('domain_name_to_resume_from')
        if domain_name_to_resume_from:
            resume_urls, visited_urls = get_resume_urls(domain_name_to_resume_from)
            if resume_urls:
                self.start_urls = resume_urls
                self.visited_urls = visited_urls
                print(f'\n\n Loaded:{len(self.start_urls)} urls.\n\n')
            
        print(f'\n-----------------------------\n start_urls: {self.start_urls if len(self.start_urls)<=1 else len(self.start_urls)} domain_name_to_resume_from : {domain_name_to_resume_from}\n-----------------------------\n')

    def parse(self, response):
        links = LinkExtractor(deny_extensions=[]).extract_links(response)
        drive_links = []    # Drive links
        site_links = []     # website links
        
        yield {
            'visited': response.url,
            }

        # yield every paragraph from current page
        paragraphs = response.css('p::text').getall()
        for paragraph in paragraphs:
            is_nepali, confidence = is_nepali_language(paragraph)
            if is_nepali:
                # print(paragraph)
                # yield {
                #         'parent_url': response.url,
                #         'page_title': response.css('title::text').get(),
                #         'paragraph': paragraph,
                #         'is_nepali_confidence': confidence
                #     }
                self.crawled_data.append(
                    {
                        'parent_url': response.url,
                        'page_title': response.css('title::text').get(),
                        'paragraph': paragraph,
                        'is_nepali_confidence': confidence
                    }
                )

        
        # Saving drive links
        for link in links:
            is_drive_link, _ = is_google_drive_link(link.url)     #DriveFunctions.is_google_drive_link(link.url)
            if is_drive_link:
                pass
                # yield {
                #     'parent_url': response.url,
                #     'url': link.url,
                #     'text': link.text,
                #     'is_drive_link': True
                # }
            else:
                is_document, document_type, ignore_doc = is_document_link(link.url)
                if is_document:
                    if not ignore_doc:  # and doc_type != 'image'
                        pass
                        # yield {
                        #     'parent_url': response.url,
                        #     'url': link.url,
                        #     'text': link.text,
                        #     'is_document': is_document,
                        #     'document_type': document_type
                        # }
                else:
                    is_social_media, social_media_type = is_social_media_link(link.url)
                    if is_social_media:
                        pass
                        # yield {
                        #     'parent_url': response.url,
                        #     'url': link.url,
                        #     'text': link.text,
                        #     'is_social_media': is_social_media,
                        #     'social_media_type': social_media_type

                        # }
                    else:
                        site_links.append(link)
        
        # Follow Next Page: 
        for site_link in site_links:
            
            # base_url, crawl_it = should_we_crawl_it(site_link.url)  # self.visited_urls_base,
            # self.visited_urls_base.add(base_url)
            if is_same_domain(response.url, site_link.url): # or crawl_it:
                # only follow urls from same domain
                
                # check if url not already visited
                if site_link.url not in self.visited_urls:
                        # if self.length<10:
                        self.visited_urls.add(site_link.url)
                        yield scrapy.Request(site_link.url, callback=self.parse)
                        # print(f'yielded: {site_link.url}')
                        
                        self.to_visit.append(site_link.url)
                        # yield {
                        #     'to_visit': site_link.url
                        #     }
            
            elif is_np_domain(site_link.url):
                pass
                # yield {
                #         'parent_url': response.url,
                #         'url': site_link.url,
                #         'text': site_link.text,
                #         'np_domain': True,
                #     }
            
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(EkantipurSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider, reason):
        print(f"almost sakyo muji. reason:{reason}")
        # Wait for both threads to finish
        self.stop_publisher.set()
        # producer_thread.join()
        self.publisher_thread.join()
        # consumer_thread.join()

        # This method will be called when the spider is closed
        self.logger.info('Spider closed. Joining publisher threads.')

        self.logger.info('Custom threads joined. Spider closing gracefully.')

