"""
Extract mflix data from MongoDB Atlas Cluster
and return the list of collected Data.
"""
from utils import connect_with_mongodb


def create_movies_data_list():
    client = connect_with_mongodb()
    movies_data = client.sample_mflix.movies
    
    # movies list
    movies_list = []
    for movie in movies_data.find():
        movies_list.append(movie)

    return movies_list
