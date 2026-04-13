
import os
import time
import isodate
import pandas as pd
import sqlite3
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import (
    API_KEY,
    YOUTUBE_API_SERVICE,
    YOUTUBE_API_VERSION,
    DB_PATH,
    TABLE_NAME,
    VIRAL_QUERIES,
    RESULTS_PER_QUERY
)



# YOUTUBE API FUNCTIONS

def get_youtube_client():
    return build(YOUTUBE_API_SERVICE, YOUTUBE_API_VERSION, developerKey=API_KEY)


def search_videos(youtube, query, max_results=50, order="viewCount"):
    video_ids = []
    next_page_token = None

    while len(video_ids) < max_results:
        request = youtube.search().list(
            q=query,
            part="id",
            type="video",
            maxResults=min(50, max_results - len(video_ids)),
            order=order,
            pageToken=next_page_token,
        )
        response = request.execute()

        for item in response.get("items", []):
            video_ids.append(item["id"]["videoId"])

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids


def get_video_features(youtube, video_ids):
    all_features = []

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(batch)
            ).execute()

            for item in response.get("items", []):
                features = extract_features(item)
                all_features.append(features)

        except HttpError as e:
            print(f"API error: {e}")

        time.sleep(0.5)

    return all_features


def extract_features(item):
    snippet = item.get("snippet", {})
    stats = item.get("statistics", {})
    details = item.get("contentDetails", {})

    views = int(stats.get("viewCount", 0) or 0)
    likes = int(stats.get("likeCount", 0) or 0)
    comments = int(stats.get("commentCount", 0) or 0)

    raw_duration = details.get("duration", "PT0S")
    try:
        duration_sec = int(isodate.parse_duration(raw_duration).total_seconds())
    except:
        duration_sec = 0

    title = snippet.get("title", "")

    return {
        "video_id": item.get("id"),
        "title": title,
        "channel_title": snippet.get("channelTitle", ""),
        "published_at": snippet.get("publishedAt", ""),
        "views": views,
        "likes": likes,
        "comments": comments,
        "duration_seconds": duration_sec,
        "title_length": len(title),
        "title_word_count": len(title.split()),
        "has_number": int(any(c.isdigit() for c in title)),
        "has_question": int("?" in title),
        "has_exclamation": int("!" in title),
        "desc_length": len(snippet.get("description", "")),
        "tag_count": len(snippet.get("tags", [])),
        "is_viral": int(views >= 1_000_000),
    }



# DATA COLLECTION

def collect_dataset(queries, results_per_query=50):
    youtube = get_youtube_client()
    all_data = []

    for query in queries:
        print(f"\nFetching: {query}")
        video_ids = search_videos(youtube, query, results_per_query)
        features = get_video_features(youtube, video_ids)
        all_data.extend(features)

    df = pd.DataFrame(all_data)
    return df

# DATABASE FUNCTIONS

def save_to_database(df):
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    conn.close()
    print(f"\nData saved to database: {DB_PATH}")


def load_from_database():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
    conn.close()
    print("\nLoaded data from database")
    return df



# MAIN

if __name__ == "__main__":
    # Step 1: Collect data
    df = collect_dataset(VIRAL_QUERIES, results_per_query=50)

    # Step 2: Save to DB
    save_to_database(df)

    # Step 3: Load from DB
    df_loaded = load_from_database()

    print("\nPreview:")
    print(df_loaded.head())