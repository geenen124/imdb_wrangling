import json
import pathlib

import pandas as pd
from Levenshtein import distance as lev


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
    # TODO: This is broken!! Can't just merge on the movie title, as these aren't unique
    in_common = pd.merge(first_m_df, second_v_df, left_on=" movie title ", right_on="title")

    # first replace descriptions if they're useful
    no_descrip_idx = in_common.description == "No overview found."
    in_common.loc[no_descrip_idx, "description"] = in_common[no_descrip_idx][" description"]

    # deduplicate titles based on description Levenshtein distance
    in_common["distance"] = in_common.apply(lambda x: lev(str(x["description"]), str(x[" description"])), axis=1)
    to_keep_idx = in_common.groupby("title")["distance"].transform(min) == in_common["distance"]
    should_be = in_common[to_keep_idx]
    # and then on release year (account for rereleases)
    remaining_conflict_ids = should_be.groupby("movie_id").size().sort_values(ascending=False)
    remaining_conflict_ids = remaining_conflict_ids[remaining_conflict_ids > 1].index.tolist()

    remaining_conflict = should_be[should_be.movie_id.isin(remaining_conflict_ids)]

    r_year = remaining_conflict[" release date "].apply(lambda x: int(str(x)[-4:]))
    start_year = remaining_conflict["released"].apply(lambda x: 3000 if pd.isna(x) else int(str(x)[:4]))
    end_year = remaining_conflict["released"].apply(lambda x: 0 if pd.isna(x) else int(str(x)[-4:]))

    resolved = remaining_conflict[((r_year >= start_year) & (r_year <= end_year))]

    in_common = pd.concat([
        should_be[~should_be.movie_id.isin(remaining_conflict_ids)],
        resolved
    ])

    # combine the columns!
    in_common["genres"] = in_common.apply(lambda x: list(set(x["genres_x"] + x["genres_y"])), axis=1)

    # Since dataset 2 is bigger, we can fold dataset 1 in. This means backfilling fields correctly as follows:
    in_common["tagline"] = in_common[["tagline", " tagline"]].bfill(axis=1).iloc[:, 0]
    in_common["title"] = in_common[["title", " movie title "]].bfill(axis=1).iloc[:, 0]
    in_common["description"] = in_common[["description", " description"]].bfill(axis=1).iloc[:, 0]
    in_common["studio"] = in_common[["studio", " studio "]].bfill(axis=1).iloc[:, 0]
    in_common["IMDb URL"] = in_common[" IMDb URL "]

    # TODO: Figure out something useful for release dates - python/pandas parsers really don't like the (non-unix?) timestamp from the larger set

    # drop remaining columns
    in_common.drop(columns=[" tagline", " movie title ", " description", " studio ", " IMDb URL ", "genres_x", "genres_y", " unknown ", " release date ", " video release date "])

    # use the same formatting for the remainder and
    m_remainder = first_m_df[~first_m_df.movie_id.isin(in_common.movie_id)].copy().reset_index(drop=True)
    m_remainder["type"] = "Movie"
    m_remainder.rename(columns={
        " tagline": "tagline",
        " movie title ": "title",
        " description": "description",
        " studio ": "studio",
        " IMDb URL ": "IMDb URL",
    })
    # again drop remaining columns
    m_remainder.drop(columns=[" tagline", " movie title ", " description", " studio ", " IMDb URL ", " unknown ", " release date ", " video release date "])

    # we also need to create unique keys for each of the movies that weren't linked to the bigger dataset
    start = len(second_v_df)
    end = start + len(m_remainder)
    m_remainder["_key"] = list(range(start, end))

    # now combine all of the vertices
    v_remainder = second_v_df[~second_v_df._key.isin(in_common._key)]
    v_df = pd.concat([v_remainder, in_common, m_remainder])

    # next load the users in
    users_df = pd.read_csv(FIRST_DSET_DIR / "users.csv")

    # add the mappings
    users_df["type"] = "User"
    # and also update the keys for the Users
    start = end
    end = start + len(users_df)
    users_df["_key"] = list(range(start, end))

    # now that we have the updated ids - we can load in the ratings from the first dataset
    ratings_df = pd.read_csv(FIRST_DSET_DIR / "ratings.csv")

    # And join the ratings to the movies + users
    l_rate_df = ratings_df.merge(v_df[["movie_id", "_key"]], how="left", left_on="item_id", right_on="movie_id")
    l_rate_df["_to"] = l_rate_df["_key"]
    l_rate_df = l_rate_df[["user_id", "Rating", "Timestamp", "_to"]]
    full_rate_df = pd.merge(users_df[["user_id", "_key"]], l_rate_df, on="user_id")
    full_rate_df["_from"] = full_rate_df["_key"]
    full_rate_df = full_rate_df[["_from", "_to", "Rating", "Timestamp"]]
    full_rate_df["type"] = "Rating"
    full_rate_df["$label"] = "RATED"

    # Then add these ratings to the full edges dataset
    with open(SECOND_DSET_DIR / "imdb_edges.data.json", "r") as raw_jsonl:
        e_jsonl = list(raw_jsonl)

    parsed_edges = [parse_jsonl_line(j) for j in e_jsonl]

    edge_df = pd.DataFrame(parsed_edges)

    all_edges_df = pd.concat([full_rate_df, edge_df])

    # Persist the data
    v_df.to_csv(DATA_DIR / "final_vertices.csv", index=False)
    all_edges_df.to_csv(DATA_DIR / "final_edges.csv", index=False)

