import pathlib

import pandas as pd
from pyArango.collection import BulkOperation, Collection, Edges, Field
from pyArango.graph import Graph, EdgeDefinition
from pyArango.connection import Connection

if __name__ == "__main__":
    FILE_DIR = pathlib.Path(__file__).parent.absolute()
    DATA_DIR = FILE_DIR / "data"

    conn = Connection(username="root", password="")

    if conn.hasDatabase("imdb"):
        db = conn["imdb"]
    else:
        db = conn.createDatabase(name="imdb")

    db.dropAllCollections()

    class Users(Collection):
        _fields = {
            "type": Field(),
            "age": Field(),
            "gender": Field(),
            "zip_code": Field()
        }

    class imdb_vertices(Collection):
        """ Messy collection """
        _fields = {
            'runtime': Field(),
            'version': Field(),
            'title': Field(),
            'label': Field(),
            'id': Field(),
            'language': Field(),
            'type': Field(),
            'description': Field(),
            'imdbId': Field(),
            'trailer': Field(),
            'homepage': Field(),
            'lastModified': Field(),
            'imageUrl': Field(),
            'studio': Field(),
            'releaseDate': Field(),
            'tagline': Field(),
            'released': Field(),
            'name': Field(),
            'birthplace': Field(),
            'profileImageUrl': Field(),
            'biography': Field(),
            'birthday': Field(),
            'genres': Field(),
            'IMDb URL': Field()
        }

    class imdb_edges(Edges):
        _fields = {
            "$label": Field(),
            "type": Field(),
            "name": Field()
        }

    class Ratings(Edges):
        _fields = {
            "rating": Field(),
            "timestamp": Field(),
            "$label": Field(),
            "type": Field()
        }

    class IMDBGraph(Graph):
        _edgeDefinitions = [
            EdgeDefinition("Ratings", fromCollections=["Users"], toCollections=["imdb_vertices"]),
            EdgeDefinition("imdb_edges", fromCollections=["imdb_vertices"], toCollections=["imdb_vertices"])
        ]
        _orphanedCollections = []

    vertex_col = db.createCollection(name="imdb_vertices")
    users_col = db.createCollection(name="Users")
    edges_col = db.createCollection(name="imdb_edges", className="Edges")
    ratings_col = db.createCollection(name="Ratings", className="Edges")

    g = db.createGraph("IMDBGraph")

    # Upload the vertices first
    vertex_df = pd.read_csv(DATA_DIR / "final_vertices.csv")

    possible_keys_to_include = [
        '_key', 'runtime', 'version', 'title', 'label', 'id', 'language', 'type', 'description',
        'imdbId', 'trailer', 'homepage', 'lastModified', 'imageUrl', 'studio', 'releaseDate', 'tagline', 'released',
        'name', 'birthplace', 'profileImageUrl', 'biography', 'birthday', 'genres',
        'IMDb URL'
    ]

    with BulkOperation(vertex_col, batchSize=100) as col:
        for i, v in vertex_df.iterrows():
            doc = col.createDocument()
            for p in possible_keys_to_include:
                if pd.notna(v[p]):
                    doc[p] = v[p]

            doc.save()

    # Then upload the users
    users_df = pd.read_csv(DATA_DIR / "final_users.csv")
    with BulkOperation(users_col, batchSize=100) as col:
        for i, user in users_df.iterrows():
            doc = col.createDocument()
            doc["_key"] = str(user["_key"])
            doc["type"] = user["type"]
            doc["age"] = user["Age"]
            doc["gender"] = user["Gender"]
            doc["zip_code"] = user["zip_code"]
            doc.save()

    # Now we can start to add edges!
    edges_df = pd.read_csv(DATA_DIR / "final_edges.csv")

    non_ratings_edges = edges_df[edges_df.type != "Rating"]
    with BulkOperation(edges_col, batchSize=1000) as col:
        for i, edge in non_ratings_edges.iterrows():
            doc = col.createDocument()
            doc["_from"] = str(edge["_from"])
            doc["_to"] = str(edge["_to"])
            doc["$label"] = str(edge["$label"])
            doc["type"] = str(edge["type"])
            doc["name"] = str(edge["name"])
            doc.save()

    ratings_edges = edges_df[edges_df.type == "Rating"]
    with BulkOperation(ratings_col, batchSize=1000) as col:
        for i, edge in ratings_edges.iterrows():
            doc = col.createDocument()
            doc["_from"] = "Users/" + str(edge["_from"])
            doc["_to"] = "imdb_vertices/" + str(edge["_to"])
            doc["$label"] = str(edge["$label"])
            doc["type"] = str(edge["type"])
            doc["rating"] = edge["Rating"]
            doc["timestamp"] = edge["Timestamp"]
            doc.save()

    # Should be done with imports now!