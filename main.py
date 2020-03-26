import csv
from pprint import pprint
from pymongo import MongoClient
from datetime import datetime

class Ticket:
    
    def __init__(self, file, db_name):
        self.file = file
        self.client = MongoClient()
        self.db = self.client[db_name]
        self.raw_collection = self.db.raw_ticket
        self.collection = self.db.ticket

    def read_data(self):
        with open(self.file, encoding='utf8') as file:
            reader = csv.DictReader(file)
            self.raw_collection.insert_many([row for row in reader]).inserted_ids
        self.convert_date()
        self.raw_collection.drop()
                
    def cheapest(self, find_doc = {}):
        cursor = self.collection.find(find_doc, {'_id': False}).sort('Цена').collation({'locale': 'en_US', 'numericOrdering': True})
        return [elem for elem in cursor]
    
    def find_artist(self, artist_name):
        return self.cheapest({'Исполнитель':{'$regex': f'/*{artist_name}', '$options': '$i'}})

    
    def find_by_date(self, start_date, end_date):
        return self.cheapest({'Дата': {'$gte': datetime(*start_date), '$lte': datetime(*end_date)}})

    
    def convert_date(self):
        pipeline = [{'$set': {'Дата': {'$dateFromString':{'dateString': {'$concat': ['$Дата', '.2020']}, 'format': '%d.%m.%Y'}}}}, {'$out': 'ticket'}]
        cursor = self.raw_collection.aggregate(pipeline)
        
file = 'artists.csv'
db_name = 'homework'

if __name__ == '__main__':
                    
    ticket = Ticket(file, db_name)

    ticket.read_data()

    pprint(ticket.cheapest())
    pprint(ticket.find_artist('ча'))
    pprint(ticket.find_artist('mar'))
    pprint(ticket.find_by_date((2020, 7, 1), (2020, 7, 30)))
    pprint(ticket.find_by_date((2020, 1, 1), (2020, 6, 30)))


