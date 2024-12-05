# radio-tracker-data-enrichment-script

## These scripts are not production ready and include hard-coded table names and such. The repo exists only to accompany a university assignment. Drop me a message if you have any questions about this code. 

`fetch-uri-and-features.py` takes an artist and song title and uses the Spotify search API to try to get a Spotify URI. It then gets audio features if a Spotify URI is available. (note: this API endpoint is no longer generally available as of 27/11/24). 

`select-zero-play-tracks.py` based on certain constraints, pulls songs from a previously prepared table based on https://www.kaggle.com/datasets/sergeserbinenko/2-million-song-spotify-dataset and inserts them into the main table. 
