import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='.env')

API_KEY = "AIzaSyCLto8Kcw89YLH1mym4rIoUvdCtvBCYbU4"
CHANNEL_HANDLE = "MrBeast"

def get_playlist_id():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}' 
        response = requests.get(url)
        # print(response)
        data = response.json()
        # print(json.dumps(data, indent=4))
        channel_items = data['items'][0]
        channel_playlisId = channel_items['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlisId
    except requests.exceptions.RequestException as e:
        raise e

 
if __name__ == "__main__":
    print(get_playlist_id())
