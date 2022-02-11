"""
Extract mflix data from MongoDB Atlas Cluster
and return the list of collected Data.
"""
from utils import connect_with_mongodb


def collect_movies_data_into_list():
    client = connect_with_mongodb()
    movies_data = client.sample_mflix.movies
    
    # prepare movies list
    movies_list = []
    for movie in movies_data.find():
        movies_list.append(movie)

    return movies_list
