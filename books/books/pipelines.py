# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
import hashlib
from scrapy.exceptions import DropItem


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

        #calls .compute_item_id() and assigns the hashed output to item_id.
        item_id = self.compute_item_id(item)
        #ab hier upsert Methode
#        item_dict = ItemAdapter(item).asdict()

 #       self.db[self.COLLECTION_NAME].update_one(
  #          filter={"_id": item_id},
   #         update={"$set": item_dict},
    #        upsert=True
     #   )

      #  return item
       
        #query the MongoDB collection to check if an item with the same _id already exists. If Python finds a duplicate, then the code raises a DropItem exception, which tells the framework to discard this item and not to process it further. If it doesn’t find a duplicate, then it proceeds to the next steps.
        if self.db[self.COLLECTION_NAME].find_one({"_id": item_id}):
            raise DropItem(f"Duplicate item found: {item}")

        #make up the else condition, where the scraped item doesn’t yet exist in the database. Before inserting the item into your collection, you add the calculated item_id as the value for the ._id attribute to your Item.
        else:
            item["_id"] = item_id
            self.db[self.COLLECTION_NAME].insert_one(ItemAdapter(item).asdict())
            return item

    def compute_item_id(self, item):
        url = item["url"]
        return hashlib.sha256(url.encode("utf-8")).hexdigest()
