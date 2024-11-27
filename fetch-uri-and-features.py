import mysql.connector
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
import time
from itertools import islice

# MySQL Configuration
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '',
    'database': 'localdemo_data'
}

# Spotify API Configuration
spotify_client_id = ''
spotify_client_secret = ''

# Spotify setup
auth_manager = SpotifyClientCredentials(client_id=spotify_client_id, client_secret=spotify_client_secret)
spotify = Spotify(auth_manager=auth_manager)

# Throttle parameters
REQUEST_DELAY = 1  # Delay between API calls in seconds

def fetch_batch_song_features(spotify_uris):
    """Fetch audio features for a batch of Spotify URIs."""
    try:
        features = spotify.audio_features(spotify_uris)
        return {f['uri']: f for f in features if f}  # Return a mapping of URI to features
    except Exception as e:
        print(f"Error fetching features for batch: {e}")
        return {}

def search_spotify_uri(song_title, artist_name):
    """Search for a Spotify URI using the Spotify Search API."""
    try:
        query = f"track:{song_title} artist:{artist_name}"
        results = spotify.search(q=query, type='track', limit=1)
        tracks = results.get('tracks', {}).get('items', [])
        if tracks:
            print(f"MATCH FOUND for song: {song_title} by artist: {artist_name}")
            return tracks[0]['uri']
        # else:
        #     print(f"No match found for song: {song_title} by artist: {artist_name}")            
    except Exception as e:
        print(f"Error searching for song '{song_title}' by '{artist_name}': {e}")
        return None

    return None

# def fetch_song_features(spotify_uri):
#     """Fetch song features from Spotify API."""
#     try:
#         features = spotify.audio_features(spotify_uri)
#         if features and features[0]:  # Ensure valid response
#             # print(features[0])
#             return features[0]
#         else:
#             print(f"No features found for URI: {spotify_uri}")
#             return None
#     except Exception as e:
#         print(f"Error fetching features for URI {spotify_uri}: {e}")
#         return None

def update_row_with_features(cursor, table_name, row_id, features, spotify_uri=None):
    """Update the row in the database with fetched features and optionally the Spotify URI."""
    if not features:
        return
    try:
        # Include Spotify URI in the update if provided
        columns = []
        values = []
        
        if spotify_uri:
            columns.append("spotify_uri = %s")
            values.append(spotify_uri)
        
        for key, value in features.items():
            if key != 'id':
                columns.append(f"`{key}` = %s")
                values.append(value)
        
        values.append(row_id)
        query = f"UPDATE {table_name} SET {', '.join(columns)} WHERE id = %s"
        # print(query)
        # print(values)
        cursor.execute(query, values)
    except Exception as e:
        print(f"Error updating row {row_id}: {e}")

def batch(iterable, n=100):
    """Batch an iterable into chunks of size n."""
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch

def main():
    # MySQL connection
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
    except Exception as e:
        print(f"Database connection error: {e}")
        return
    
    table_name = "radio_tracker_tracks"  # Replace with your table name
    spotify_uri_column = "spotify_uri"  # Replace with the column name for Spotify URIs
    id_column = "id"  # Replace with your table's primary key column
    title_column = "song"  # Replace with the column for song titles
    artist_column = "artist"  # Replace with the column for artist names
    
    try:
        # Select all rows
        cursor.execute(f"SELECT {id_column}, {spotify_uri_column}, {title_column}, {artist_column} FROM {table_name} WHERE spotify_uri != '' AND `type` IS NULL;")#`type` IS NULL AND id > 50000 ORDER BY id ASC LIMIT 2000")
        
        rows = cursor.fetchall()
        
        # Dictionary to store rows needing Spotify features
        rows_needing_features = {}

        
        for row in rows:
            row_id = row[id_column]
            spotify_uri = row[spotify_uri_column]
            song_title = row[title_column]
            artist_name = row[artist_column]
            
            print(f"Processing row ID {row_id} with URI {spotify_uri}")
            
            # if not spotify_uri:
            #     # Attempt to find Spotify URI
            #     #print(f"Spotify URI missing for row ID {row_id}. Searching by song title and artist...")
            #     spotify_uri = search_spotify_uri(song_title, artist_name)

            #     cursor.execute(f"UPDATE {table_name} SET uri_fetch_tried = 1 WHERE {id_column}={row_id};")
            #     connection.commit()
            #     if not spotify_uri:                     
            #         #print(f"Failed to find Spotify URI for row ID {row_id}. Skipping...")
            #         continue  # Skip to the next row
            
            rows_needing_features[row_id] = spotify_uri

            if len(rows_needing_features) == 100:
                features_map = fetch_batch_song_features(rows_needing_features.values())
            
                for row_id, spotify_uri in rows_needing_features.items():
                    if spotify_uri in features_map:
                        update_row_with_features(cursor, table_name, row_id, features_map[spotify_uri], spotify_uri)
                connection.commit()  # Commit after each batch

                rows_needing_features = {}
            
        # Batch process Spotify URIs
        # for batch_uris in batch(rows_needing_features.values(), 100):
        #     features_map = fetch_batch_song_features(batch_uris)
            
        #     for row_id, spotify_uri in rows_needing_features.items():
        #         if spotify_uri in features_map:
        #             update_row_with_features(cursor, table_name, row_id, features_map[spotify_uri], spotify_uri)
        #     connection.commit()  # Commit after each batch
        #     time.sleep(REQUEST_DELAY)  # Throttle requests
    except Exception as e:
        print(f"Error processing rows: {e}")
    finally:
        cursor.close()
        connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()