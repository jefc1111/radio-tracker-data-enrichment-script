# radio-tracker-data-enrichment-script

## These scripts are not production ready and include hard-coded table names and such. The repo exists only as scrappy documentation. 

`fetch-uri-and-features.py` takes an artist and song title and uses the Spotify search API to try to get a Spotify URI. It then gets audio features if a Spotify URi is available. 

`select-zero-play-tracks.py` bsed on certain constraints, pulls songs from a previously prepared table based on https://www.kaggle.com/datasets/sergeserbinenko/2-million-song-spotify-dataset and inserts them into the main table. 
