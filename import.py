import csv
import json
import requests
import sys

from pyArango.connection import *
from pyArango.collection import Collection, Edges, Field
from pyArango.graph import Graph, EdgeDefinition
from pyArango.collection import BulkOperation as BulkOperation

def create_genres(action , adventure , animation , childrens , comedy , crime , documentary , drama , fantasy , noir , horror , musical , mystery , romance , scifi , thriller , war , western):
    genre = []
    if action == '1':
        genre.append('action')
    if adventure == '1':
        genre.append('adventure')
    if animation == '1':
        genre.append('animation')
    if childrens == '1':
        genre.append('childrens')
    if comedy == '1':
        genre.append('comedy')
    if crime == '1':
        genre.append('crime')
    if documentary == '1':
        genre.append('documentary')
    if drama == '1':
        genre.append('drama')
    if crime == '1':
        genre.append('crime')
    if fantasy == '1':
        genre.append('fantasy')
    if noir == '1':
        genre.append('noir')
    if horror == '1':
        genre.append('horror')
    if musical == '1':
        genre.append('musical')
    if romance == '1':
        genre.append('romance')
    if horror == '1':
        genre.append('horror')
    if scifi == '1':
        genre.append('scifi')
    if thriller == '1':
        genre.append('thriller')
    if war == '1':
        genre.append('war')
    if western == '1':
        genre.append('western')
    return genre

class Users(Collection):
    _fields = {
        "user_id": Field(),
        "age": Field(),
        "gender": Field()
    }
    
class Movies(Collection):
    _fields = {
        "movie_id": Field(),
        "movie_title": Field(),
        "release_date": Field(),
        "genres": Field(),
        "description": Field(),
        "tagline": Field(),
        "studio": Field()
    }

class Ratings(Edges): 
    _fields = {
        #user_id and item_id are encoded by _from, _to 
        "ratings": Field(),
        "timestamp": Field()
    }

class IMDBGraph(Graph) :
    _edgeDefinitions = [EdgeDefinition("Ratings", fromCollections=["Users"], toCollections=["Movies"])]
    _orphanedCollections = []

# get connection server 
conn = Connection(username="root", password="root") 

# create school database if it does not exist
db = conn.createDatabase(name="imdb2")

db.createCollection("Users")
db.createCollection("Movies")
db.createCollection("Ratings")
iMDBGraph = db.createGraph("IMDBGraph", replicationFactor=3)

print("Collection/Graph Setup done.")

collection = db["Users"]
with BulkOperation(collection, batchSize=100) as col:
    with open('data/users.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        #Skip header
        next(reader)
        for row in reader:
            user_id,age,gender,occupation,zip = tuple(row)
            doc = col.createDocument()
            doc["_key"] = user_id
            doc["age"] = age
            doc["gender"] = gender
            doc.save()

collection = db["Movies"]
with BulkOperation(collection, batchSize=100) as col:
    with open('data/movies.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        #Skip header
        next(reader)
        for row in reader:
            movie_id, movie_title , release_date , video_release_date , url , unknown , action , adventure , animation , childrens , comedy , crime , documentary , drama , fantasy , noir , horror , musical , mystery , romance , scifi , thriller , war , western, description, tagline, studio = tuple(row)
            doc = col.createDocument()
            doc["_key"] = movie_id
            doc["movie_title"] = movie_title
            doc["release_date"] = release_date
            doc["description"] = description
            doc["tagline"] = tagline
            doc["studio"] = studio
            doc["genres"] = create_genres(action , adventure , animation , childrens , comedy , crime , documentary , drama , fantasy , noir , horror , musical , mystery , romance , scifi , thriller , war , western)
            doc.save()

collection = db["Ratings"]
with BulkOperation(collection, batchSize=1000) as col:
    with open('data/ratings.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        #Skip header
        next(reader)
        for row in reader:
            user_id,movie_id,rating,timestamp = tuple(row)
            doc = col.createDocument()
            doc["_from"] = "Users/"+user_id
            doc["_to"] = "Movies/"+movie_id
            doc["ratings"] = rating
            doc["timestamp"] = timestamp
            doc.save()
        
print("Import Done")
