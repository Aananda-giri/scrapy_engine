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
class QuotesSpider(scrapy.Spider):
    name = "quotes"
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
    def __init__(self):
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

        self.start_urls = [
                "https://quotes.toscrape.com/page/1/",
                "https://quotes.toscrape.com/page/2/",
            ]
        # url = "https://quotes.toscrape.com/"
        # tag = getattr(self, "tag", None)
        # if tag is not None:
        #     url = url + "tag/" + tag
        # yield scrapy.Request(url, self.parse)

    def parse(self, response):
        for quote in response.css("div.quote"):
            self.crawled_data.append(
                {
                    "text": quote.css("span.text::text").get(),
                    "author": quote.css("small.author::text").get(),
                    "url": response.url,
                }
            )
            
            yield {
                "text": quote.css("span.text::text").get(),
                "author": quote.css("small.author::text").get(),
            }
        next_page = response.css("li.next a::attr(href)").get()
        if next_page is not None:
            self.to_visit.append(response.urljoin(next_page))
            yield response.follow(next_page, self.parse)
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(QuotesSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider, reason):
        print("almost sakyo muji.")
        # Wait for both threads to finish
        self.stop_publisher.set()
        # producer_thread.join()
        self.publisher_thread.join()
        # consumer_thread.join()

        # This method will be called when the spider is closed
        self.logger.info('Spider closed. Joining publisher threads.')

        self.logger.info('Custom threads joined. Spider closing gracefully.')

