import sqlite3
import pandas as pd
import numpy as np

from config import DB_PATH, TABLE_NAME

CONTENT_TABLE = "content_features"

def get_connection():
    return sqlite3.connect(DB_PATH)

def load_raw_data():
    conn = get_connection()
    df = pd.read_sql(f"SELECT video_id, title, desc_length, tag_count, title_length, title_word_count, has_number, has_question, has_exclamation FROM {TABLE_NAME}", conn)
    conn.close()
    return df

def create_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {CONTENT_TABLE} (
            video_id TEXT PRIMARY KEY,
            title_clickbait_score REAL,
            avg_word_length_title REAL,
            desc_to_title_ratio REAL,
            tags_per_title_word REAL
        )
    """)
    conn.commit()
    conn.close()

def extract_content_features(df):
    df = df.copy()

    df["title_clickbait_score"] = df["has_number"] + df["has_question"] + df["has_exclamation"]
    df["avg_word_length_title"] = df["title_length"] / df["title_word_count"].replace(0, np.nan)
    df["desc_to_title_ratio"] = df["desc_length"] / df["title_length"].replace(0, np.nan)
    df["tags_per_title_word"] = df["tag_count"] / df["title_word_count"].replace(0, np.nan)

    return df[[
        "video_id",
        "title_clickbait_score",
        "avg_word_length_title",
        "desc_to_title_ratio",
        "tags_per_title_word"
    ]].fillna(0)

def save_to_database(df):
    conn = get_connection()
    df.to_sql(CONTENT_TABLE, conn, if_exists="replace", index=False)
    conn.close()

def main():
    raw_df = load_raw_data()
    create_table()
    content_df = extract_content_features(raw_df)
    save_to_database(content_df)
    print(f"Saved {len(content_df)} rows to {CONTENT_TABLE}")

if __name__ == "__main__":
    main()