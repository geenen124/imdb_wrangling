import json
import pathlib

import pandas as pd


FILE_DIR = pathlib.Path(__file__).parent.absolute()
DATA_DIR = FILE_DIR / "data"
FIRST_DSET_DIR = DATA_DIR / "first_dataset"
SECOND_DSET_DIR = DATA_DIR / "second_dataset"


if __name__ == "__main__":
    # First we want to extract the JSONL to a common format
    # Start with the vertices
    with open(SECOND_DSET_DIR / "imdb_vertices.data.json", "r") as raw_jsonl:
        v_jsonl = list(raw_jsonl)

    def parse_jsonl_line(json_str):
        vertex_line = json.loads(json_str)
        vertex_data = vertex_line["data"]
        return vertex_data

    parsed_vertices = [parse_jsonl_line(j) for j in v_jsonl]

    second_v_df = pd.DataFrame(parsed_vertices)

    # initial genre prep
    second_v_df["genres"] = second_v_df["genre"].apply(lambda x: [] if pd.isna(x) else [x])
    print(second_v_df)

    # Then load the movie vertices - Movies are what these 2 datasets have in common, so that's what we'll be joining on
    first_m_df = pd.read_csv(FIRST_DSET_DIR / "movies.csv")

    # second genre prep (leading and trailing due to whitespace in first_m_df
    genres = [
        " Action ", " Adventure ", " Animation ", " Children's ", " Comedy ", " Crime ", " Documentary ", " Drama ",
        " Fantasy ", " Film-Noir ", " Horror ", " Musical ", " Mystery ", " Romance ", " Sci-Fi ", " Thriller ",
        " War ", " Western "
    ]

    first_m_df["genres"] = first_m_df.apply(lambda x: [genre.strip() for genre in genres if x[genre] == 1], axis=1)
    first_m_df = first_m_df.drop(columns=genres)

    # Join the two together
    in_common = pd.merge(first_m_df, second_v_df, left_on=" movie title ", right_on="title")

    # combine the columns!
    in_common["genres"] = in_common.apply(lambda x: list(set(x["genres_x"] + x["genres_y"])), axis=1)

    # Since dataset 2 is bigger, we can fold dataset 1 in. This means backfilling fields correctly as follows:

    # next load the users in
    # and do the user mappings here too!

    # we also need to create unique keys for each of the movies that weren't linked to the bigger dataset
    # ...and also for the Users

    # now that we have the updated ids - we can load in the ratings from the first dataset

    # Join the ratings to the movies
    # next join these to the users
    # Replace the _from and _to key fields with the updated ids

    # Now we should be good to go!

    # TODO: Add edges to the various genres
    # Persist the data

