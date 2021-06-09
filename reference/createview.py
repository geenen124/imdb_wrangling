import json
import requests
import sys
# import oasis
import time

# from pyArango.connection import *
from arango import ArangoClient

# Initialize the ArangoDB client.
client = ArangoClient()

# Connect to "test" database as root user.
database = client.db('imdb2', username='root', password='root')

# Create an ArangoSearch view.
database.create_arangosearch_view(
    name='v_imdb',
    properties={'cleanupIntervalStep': 0}
)

# print(database["v_imdb"])

link = { 
  "includeAllFields": True,
  "fields" : { 
    "description" : { "analyzers" : [ "text_en" ] }, 
    "movie_title" : { "analyzers" : [ "text_en" ] } 
  }
}

database.update_arangosearch_view(
    name='v_imdb',
    properties={'links': { 'Movies': link }}
)

print("Search View Created - Done")

print(database["v_imdb"])
