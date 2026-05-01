import requests
import json
import os
from dotenv import load_dotenv
import datetime
from datetime import date
from airflow.decorators import task
from airflow.models import Variable

load_dotenv(dotenv_path='.env')

API_KEY = Variable.get('API_KEY')
CHANNEL_HANDLE = "MrBeast"
max_results = 50

@task
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
    
@task    
def get_video_ids(playlist_id):
    
    video_ids = []
    page_token = None

    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={API_KEY}' \

    try:
        while True:
            url = base_url
            if page_token:
                url += f'&pageToken={page_token}'
            response = requests.get(url)
            data = response.json()
            for item in data['items']:
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            if 'nextPageToken' in data:
                page_token = data['nextPageToken']
            else:
                break

        return video_ids
    except requests.exceptions.RequestException as e:
        raise e
@task
def get_video_details(video_ids):
    extracted_data = []

    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id : video_id + batch_size]

    try:
            for batch in batch_list(video_ids, max_results):
                video_id_str = ",".join(batch)
                url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_id_str}&key={API_KEY}'
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                for item in data.get('items', []):
                    video_id = item['id']
                    snippet = item['snippet']
                    contentDetails = item['contentDetails']
                    statistics = item['statistics']
                    video_data = {
                        'video_id': video_id,
                        'title': snippet['title'],
                        'published_at': snippet['publishedAt'],
                        'duration': contentDetails['duration'],
                        'view_count': statistics.get('viewCount', 0),
                        'like_count': statistics.get('likeCount', 0),
                        'comment_count': statistics.get('commentCount', 0)
                        }
            
                        
                    
                extracted_data.append(video_data)
                
    except requests.exceptions.RequestException as e:
        raise e
    return extracted_data

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"

    with open(file_path, 'w',encoding="utf-8") as json_file:
        json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)

        

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = get_video_details(video_ids)
    save_to_json(extracted_data)