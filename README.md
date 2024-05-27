# Scrapy hub
* Code to deploy scrapy_engine to [scrapy hub](https://www.zyte.com/).

## To run locally
`scrapy crawl worker_spider_v2 -o worker_spider_v2.json`
##  Set values for
* `scrapy_engine/spiders/worker_spider_v2.py`
```
    host='<redis-host>',        # replace <redis-host> with real host from redis website
    port = <redis-port>,
    password="<redis-password>",
```

* `scrapy_engine/spiders/mongo.py`
replace `<username>:<password>` with real username and password.

## To Deploy:
```
shub login  # Enter key from site
# create files "requirements.txt" and "scrapinghub.yml"
shub deploy
```

# ToDo
[ ] use .env file to store secrets: Coudn't make it work with shub deployment

## Resources:
* [Deploying to scrapy cloud](https://support.zyte.com/support/solutions/articles/22000200400-deploying-python-dependencies-for-your-projects-in-scrapy-cloud)

[X] Version of scrapy used by default was giving error: 
* [1. Changing scrapy version](https://support.zyte.com/support/solutions/articles/22000200402-changing-the-deploy-environment-with-scrapy-cloud-stacks)

* Remove `scrapy==...` from requirements.txt: it will give error