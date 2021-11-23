import json

import requests
from elasticsearch import Elasticsearch


class DataProcessor:
    def __init__(self):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


    def create_index_in_es(self):
        """
        It sends data to Elasticsearch
        """
        Books = [{"id":1,"title":"C# Programming in a nutshell","pages":200},
                 {"id":2,"title":"C++ Programming","pages":150},
                 {"id":3,"title":"Core Python","pages":250}]
        for doc in Books:
            self.es.index(index='books', doc_type='programming_books', id=doc["id"], body=json.dumps(doc))

    def show_indexes_in_es(self):
        """
        It shows the existing indexes in ElasticSearch
        """
        r = requests.get("http://localhost:9200/_cat/indices?v")
        print(r.content)

    def show_types_in_es(self):
        """
        It shows the existing types for the index
        """
        r = requests.get("http://localhost:9200/books/_mapping")
        json_result = json.loads(r.content)
        print(json.dumps(json_result, indent=4, sort_keys=True))

    def show_all(self):
        print("All documents")
        r = requests.get("http://localhost:9200/books/_search?pretty")
        json_result = json.loads(r.content)
        print(json.dumps(json_result, indent=4, sort_keys=True))


    def number_of_pages_search(self):
        print("Number of pages range search")
        docs = self.es.search(index="books",
                              body={"query": {"range": {'pages': {"gte": 171 }}}})
        print(docs)

    def custom_fuzzy_full_text_search(self):
        print("Full text fuzzy search")
        docs = self.es.search(index="books",
                              body={"query": {"fuzzy": {'title': 'Programming'}}})
        print(docs)

    def close(self):
        self.es.close()

    def run(self):
        """
        It is to run the entire process
        """
        #self.create_index_in_es()
        self.show_indexes_in_es()
        self.show_types_in_es()
        self.show_all()
        self.custom_fuzzy_full_text_search()
        self.number_of_pages_search()
        self.close()


data_processor = DataProcessor()
data_processor.run()