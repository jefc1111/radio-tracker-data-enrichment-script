import random
import mysql.connector

# Establish database connection
db = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='',
    database='localdemo_data'
)
cursor = db.cursor(dictionary=True)

# Fetch all unique artists from radio_tracker_tracks_a
cursor.execute("SELECT DISTINCT artist FROM radio_tracker_tracks_a")
artists = cursor.fetchall()

for artist_row in artists:
    artist = artist_row['artist']
    print(artist)
    # Fetch up to 3 random songs for the current artist
    cursor.execute("""
        SELECT 
            a.artist_name AS artist,
            a.track_name AS song,
            a.track_uri AS spotify_uri,
            a.duration_ms,
            a.danceability,
            a.energy,
            a.`key`,
            a.loudness,
            a.`mode`,
            a.speechiness,
            a.acousticness,
            a.instrumentalness,
            a.liveness,
            a.valence,
            a.tempo,
            a.`type`,
            a.`uri`,
            a.`track_href`,
            a.`analysis_url`,
            a.`fduration_ms`,
            a.`time_signature`
        FROM radio_tracker_two_million_songs_a a
        WHERE a.artist_name = %s
          AND a.track_name NOT IN (SELECT song FROM radio_tracker_tracks_a)
          AND a.track_uri NOT IN (SELECT spotify_uri FROM radio_tracker_tracks_a)
    """, (artist,))
    possible_songs = cursor.fetchall()
    print(len(possible_songs))
    # Randomly select up to 3 songs
    selected_songs = random.sample(possible_songs, min(3, len(possible_songs)))

    # Insert selected songs into radio_tracker_tracks_a
    for song in selected_songs:
        print(song)
        cursor.execute("""
            INSERT INTO radio_tracker_tracks_a (
                artist, song, spotify_uri, duration_ms, danceability, energy, `key`,
                loudness, `mode`, speechiness, acousticness, instrumentalness, liveness,
                valence, tempo, `type`, `uri`, `track_href`, `analysis_url`,
                `fduration_ms`, `time_signature`
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            song['artist'], song['song'], song['spotify_uri'], song['duration_ms'], 
            song['danceability'], song['energy'], song['key'], song['loudness'], 
            song['mode'], song['speechiness'], song['acousticness'], 
            song['instrumentalness'], song['liveness'], song['valence'], 
            song['tempo'], song['type'], song['uri'], song['track_href'], 
            song['analysis_url'], song['fduration_ms'], song['time_signature']
        ))
        db.commit()

# Close the connection
cursor.close()
db.close()
