# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class MongoPipeline:
    #specifies the name of the MongoDB collection where you want to store the items. This should match the name of the collection that you set up earlier.
    COLLECTION_NAME = "books"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
    #initializes the pipeline with the MongoDB URI and database name. You can access this information because you’re fetching it from the Crawler using the .from_crawler() class method.
    
    @classmethod
    #.from_crawler() is a class method that gives you access to all core Scrapy components, such as the settings. In this case, you use it to retrieve the MongoDB settings from settings.py through the Scrapy crawler.
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE"),
        )

    #opens a connection to MongoDB when the spider starts.
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    #closes the MongoDB connection when the spider finishes.
    def close_spider(self, spider):
        self.client.close()

    #inserts each scraped item into the MongoDB collection. This method usually contains the core functionality of a pipeline.
    def process_item(self, item, spider):
        self.db[self.COLLECTION_NAME].insert_one(ItemAdapter(item).asdict())
        return item
