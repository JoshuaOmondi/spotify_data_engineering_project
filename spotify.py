import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account
from google.cloud.bigquery import SchemaField

# Spotify API credentials
SPOTIPY_CLIENT_ID = 'f83ded7fc8b44168aa1e71d0197eca35'
SPOTIPY_CLIENT_SECRET = '6a3f937c6cca411bac3ba7a716b1d39a'

# Google Cloud credentials
GOOGLE_APPLICATION_CREDENTIALS = '/Users/joshuaomondi/Documents/SPOTIFY/data-projects-392619-0e003cd74143.json'

# BigQuery settings
PROJECT_ID = 'data-projects-392619'
DATASET_ID = 'spotify'
TABLE_ID = 'spotify_extract'

def get_spotify_data():
    # Initialize Spotify client
    client_credentials_manager = SpotifyClientCredentials(
        client_id=SPOTIPY_CLIENT_ID, 
        client_secret=SPOTIPY_CLIENT_SECRET
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # Get data from Spotify (example: top tracks from a playlist)
    playlist_id = 'spotify:playlist:37i9dQZEVXbMDoHDwVN2tF'  # Global Top 50 playlist
    results = sp.playlist_tracks(playlist_id)
    
    # Extract relevant information
    tracks = []
    for item in results['items']:
        track = item['track']
        tracks.append({
            'name': track['name'],
            'artist': track['artists'][0]['name'],
            'album': track['album']['name'],
            'popularity': track['popularity']
        })
    
    return pd.DataFrame(tracks)

def upload_to_bigquery(df):
    # Initialize BigQuery client
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    # Define table reference
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    # Define job config
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV

    # Define schema
    schema = [
        SchemaField("name", "STRING"),
        SchemaField("artist", "STRING"),
        SchemaField("album", "STRING"),
        SchemaField("popularity", "INTEGER")]
    # Define job config
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"  # This will overwrite the table if it exists
    )
    # Load data to BigQuery
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Loaded {job.output_rows} rows into {DATASET_ID}:{TABLE_ID}")

if __name__ == "__main__":
    # Get data from Spotify
    spotify_data = get_spotify_data()

    # Upload data to BigQuery
    upload_to_bigquery(spotify_data)