# This is some messing around with pymongo
# more messing around
#even more messing around

import pymongo
import datetime
import pprint
import csv
import uuid

# establish connection with mongo
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')

# select database to use
db = client['test']

# make a dictionary to be inserted into mongo
post = {"author": "Sam", \
        "text": "My first blog post!", \
        "tags": ["mongodb", "python", "pymongo"], \
        "date": datetime.datetime.utcnow() \
       }

# post_id = db['posts'].insert_one(post)
# print str(post_id)

# this findOne query returns a python dictionary
post =  db['posts'].find_one({"author" : "Sam"})
pprint.pprint(post,width=1)

# can do bulk inserts with insert_many
new_posts = [{"author": "Mike",
              "text": "Another post!",
              "tags": ["bulk", "insert"],
              "date": datetime.datetime(2009, 11, 12, 11, 14)},
             {"author": "Eliot",
              "title": "MongoDB is fun",
              "text": "and pretty easy too!",
              "date": datetime.datetime(2009, 11, 10, 10, 45)}]

# result = db['posts'].insert_many(new_posts)
# print result.inserted_ids

# this loops over documents in mongo and writes to a csv or a tsv
file  = open('test.tsv', "wb")
writer = csv.writer(file, delimiter='\t', quotechar='"', quoting=csv.QUOTE_NONE)
count = 0
writer.writerow(["Count", "Name"])
for post in db['posts'].find():

   count = count + 1
   print("%s %s" %(count, post['author']))
   row = [count, post['author']]
   writer.writerow(row)

file.close()

# now I want to read tsv and write to a new collection in mongo called tsvtest
file  = open('test.tsv', "r")
next(file)     #this skips the first row
reader = csv.reader(file, delimiter='\t', quotechar='"', quoting=csv.QUOTE_NONE)
for row in reader:

   id = str(uuid.uuid4())
   tsvitem = { "_id" : id, \
               "count": row[0], \
               "name" : row[1] \
             }
   
   db['tsvtest'].insert_one(tsvitem)

file.close()


# now I want to read the tsvtest collections 4 records at a time using limit and skip
# I haven't had time to do this yet
length = db.tsvtest.count()
print length



