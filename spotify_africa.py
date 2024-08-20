import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account
from google.cloud.bigquery import SchemaField
import time

# ... (keep your existing credential and settings)

# Spotify API credentials
SPOTIPY_CLIENT_ID = 'f83ded7fc8b44168aa1e71d0197eca35'
SPOTIPY_CLIENT_SECRET = '6a3f937c6cca411bac3ba7a716b1d39a'

# Google Cloud credentials
GOOGLE_APPLICATION_CREDENTIALS = '/Users/joshuaomondi/Documents/SPOTIFY/data-projects-392619-0e003cd74143.json'

# BigQuery settings
PROJECT_ID = 'data-projects-392619'
DATASET_ID = 'spotify'
TABLE_ID = 'african_artists'

def get_spotify_data():
    client_credentials_manager = SpotifyClientCredentials(
        client_id=SPOTIPY_CLIENT_ID, 
        client_secret=SPOTIPY_CLIENT_SECRET
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    african_countries = {
        'NG': 'Nigeria',
        'ZA': 'South Africa',
        'KE': 'Kenya',
        'GH': 'Ghana',
        'TZ': 'Tanzania'
    }

    artists = []
    for country_code, country_name in african_countries.items():
        print(f"Searching for artists in {country_name}...")
        try:
            # Search for playlists related to the country
            playlists = sp.search(q=f'top {country_name}', type='playlist', limit=1)
            if playlists['playlists']['items']:
                playlist = playlists['playlists']['items'][0]
                tracks = sp.playlist_tracks(playlist['id'], limit=50)
                
                # Extract unique artists from the playlist
                country_artists = {}
                for item in tracks['items']:
                    if item['track'] and item['track']['artists']:
                        artist = item['track']['artists'][0]
                        if artist['id'] not in country_artists:
                            country_artists[artist['id']] = artist['name']
                
                # Get details for each artist
                for artist_id, artist_name in list(country_artists.items())[:10]:  # Limit to top 10 artists
                    try:
                        artist_info = sp.artist(artist_id)
                        top_tracks = sp.artist_top_tracks(artist_id, country=country_code)
                        if top_tracks['tracks']:
                            top_track = top_tracks['tracks'][0]
                            artists.append({
                                'name': artist_name,
                                'country': country_name,
                                'popularity': artist_info['popularity'],
                                'followers': artist_info['followers']['total'],
                                'top_track_name': top_track['name'],
                                'top_track_popularity': top_track['popularity']
                            })
                            print(f"Added {artist_name} to the list")
                    except Exception as e:
                        print(f"Error getting info for {artist_name}: {str(e)}")
                    time.sleep(1)  # Add a small delay to avoid rate limiting
            else:
                print(f"No playlists found for {country_name}")
        except Exception as e:
            print(f"Error searching for artists in {country_name}: {str(e)}")
        time.sleep(2)  # Add a delay between countries to avoid rate limiting

    if not artists:
        print("No artists found. The DataFrame will be empty.")
    return pd.DataFrame(artists)


def upload_to_bigquery(df):
    if df.empty:
        print("The DataFrame is empty. No data to upload to BigQuery.")
        return

    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    schema = [
        SchemaField("name", "STRING"),
        SchemaField("country", "STRING"),
        SchemaField("popularity", "INTEGER"),
        SchemaField("followers", "INTEGER"),
        SchemaField("top_track_name", "STRING"),
        SchemaField("top_track_popularity", "INTEGER")
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print(f"Loaded {job.output_rows} rows into {DATASET_ID}:{TABLE_ID}")

if __name__ == "__main__":
    spotify_data = get_spotify_data()
    print(f"Retrieved data for {len(spotify_data)} artists")
    if not spotify_data.empty:
        upload_to_bigquery(spotify_data)
    else:
        print("No data to upload to BigQuery.")