from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="/Users/joshuaomondi/Documents/SPOTIFY/.env")

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

print(f"Client ID: {client_id}")
print(f"Client Secret: {client_secret}")

