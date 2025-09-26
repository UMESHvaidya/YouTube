import os
import pickle
import glob
from datetime import datetime as dt

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/youtube.upload']
CLIENT_SECRETS_FILE = 'client_secrets.json'
TOKEN_FILE = 'token.pickle'
CATEGORY_ID = '22'  # People & Blogs; change if needed

def get_authenticated_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)
    return build('youtube', 'v3', credentials=creds)

def upload_video(youtube, file_path, title, description, category_id=CATEGORY_ID, publish_at=None):
    body = {
        'snippet': {
            'title': title,
            'description': description,
            'tags': [],  # Add tags if needed
            'categoryId': category_id
        },
        'status': {
            'privacyStatus': 'private' if publish_at else 'public',
        }
    }
    if publish_at and publish_at > dt.now():
        body['status']['publishAt'] = publish_at.strftime('%Y-%m-%dT%H:%M:%SZ')

    insert_request = youtube.videos().insert(
        part=','.join(body.keys()),
        body=body,
        media_body=MediaFileUpload(file_path, chunksize=-1, resumable=True)
    )

    response = None
    while response is None:
        status, response = insert_request.next_chunk()
        if status:
            print(f"Uploaded {int(status.progress().resumable_progress * 100)}%")

    print(f"Video uploaded: https://youtu.be/{response['id']}")
    return response

def main():
    # Get input folder
    folder = input("Enter folder path (default: data/output_videos): ").strip() or 'data/output_videos'
    if not os.path.isabs(folder):
        folder = os.path.join(os.getcwd(), folder)

    video_files = glob.glob(os.path.join(folder, '*.mp4')) + glob.glob(os.path.join(folder, '*.mov')) + glob.glob(os.path.join(folder, '*.avi'))
    if not video_files:
        print("No video files found.")
        return

    print(f"Found {len(video_files)} videos.")

    youtube = get_authenticated_service()

    for video in video_files:
        print(f"\n--- Uploading {os.path.basename(video)} ---")
        title = input("Title: ").strip() or os.path.splitext(os.path.basename(video))[0]
        desc = input("Description (optional): ").strip()
        schedule_str = input("Schedule time (YYYY-MM-DD HH:MM UTC, optional for immediate): ").strip()

        publish_at = None
        if schedule_str:
            try:
                publish_at = dt.strptime(schedule_str, '%Y-%m-%d %H:%M')
                if publish_at <= dt.now():
                    print("Schedule time must be in the future. Uploading immediately.")
                    publish_at = None
            except ValueError:
                print("Invalid datetime format. Uploading immediately.")
                publish_at = None

        upload_video(youtube, video, title, desc, publish_at=publish_at)
        print("Upload complete.\n")

if __name__ == '__main__':
    main()