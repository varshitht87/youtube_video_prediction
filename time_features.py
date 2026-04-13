import sqlite3
import pandas as pd
from googleapiclient.discovery import build

from config import DB_PATH, TABLE_NAME, API_KEY, YOUTUBE_API_SERVICE, YOUTUBE_API_VERSION

TIME_TABLE = "time_features"

def get_connection():
    return sqlite3.connect(DB_PATH)

def get_youtube_client():
    return build(YOUTUBE_API_SERVICE, YOUTUBE_API_VERSION, developerKey=API_KEY)

def load_raw_data():
    conn = get_connection()
    df = pd.read_sql(f"SELECT video_id, published_at FROM {TABLE_NAME}", conn)
    conn.close()
    return df

def create_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TIME_TABLE} (
            video_name TEXT,
            publish_hour INTEGER,
            publish_dayofweek INTEGER,
            publish_month INTEGER,
            is_weekend INTEGER,
            days_since_publish INTEGER
        )
    """)
    conn.commit()
    conn.close()

def fetch_video_names(video_ids):
    youtube = get_youtube_client()
    name_map = {}

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        response = youtube.videos().list(
            part="snippet",
            id=",".join(batch)
        ).execute()

        for item in response.get("items", []):
            name_map[item["id"]] = item["snippet"]["title"]

    return name_map

def extract_time_features(df):
    df = df.copy()

    pub_time = pd.to_datetime(df["published_at"], errors="coerce").dt.tz_localize(None)
    now = pd.Timestamp.now("UTC").tz_localize(None)

    df["publish_hour"] = pub_time.dt.hour.fillna(0).astype(int)
    df["publish_dayofweek"] = pub_time.dt.dayofweek.fillna(0).astype(int)
    df["publish_month"] = pub_time.dt.month.fillna(1).astype(int)
    df["is_weekend"] = pub_time.dt.dayofweek.isin([5, 6]).astype(int)
    df["days_since_publish"] = ((now - pub_time).dt.days.clip(lower=0)).fillna(0).astype(int)

    name_map = fetch_video_names(df["video_id"].dropna().unique().tolist())
    df["video_name"] = df["video_id"].map(name_map).fillna("Unknown Video")

    return df[[
        "video_name",
        "publish_hour",
        "publish_dayofweek",
        "publish_month",
        "is_weekend",
        "days_since_publish"
    ]]

def save_to_database(df):
    conn = get_connection()
    df.to_sql(TIME_TABLE, conn, if_exists="replace", index=False)
    conn.close()

def main():
    print("Extracting TIME features...")
    raw_df = load_raw_data()
    create_table()
    time_df = extract_time_features(raw_df)
    save_to_database(time_df)

    print(f"Saved {len(time_df)} rows to '{TIME_TABLE}'")
    print("\nSample:")
    print(time_df.head())

if __name__ == "__main__":
    main()