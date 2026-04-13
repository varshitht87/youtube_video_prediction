import sqlite3
import numpy as np
import pandas as pd

from config import DB_PATH, TABLE_NAME

ENGAGEMENT_TABLE = "engagement_features"

def get_connection():
    return sqlite3.connect(DB_PATH)

def load_raw_data():
    """Load videos table from your database"""
    conn = get_connection()
    df = pd.read_sql(f"SELECT video_id, views, likes, comments FROM {TABLE_NAME}", conn)
    conn.close()
    print(f"✅ Loaded {len(df)} videos from '{TABLE_NAME}'")
    return df

def create_engagement_table():
    """Create engagement_features table"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {ENGAGEMENT_TABLE} (
            video_id TEXT PRIMARY KEY,
            views INTEGER,
            likes INTEGER,
            comments INTEGER,
            likes_per_view REAL,
            comments_per_view REAL,
            likes_per_100_views REAL,
            comments_per_100_views REAL,
            engagement_rate REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"✅ Created table '{ENGAGEMENT_TABLE}'")

def extract_engagement_features(df):
    """Extract ALL 5 engagement ratios"""
    df_eng = df[['video_id', 'views', 'likes', 'comments']].copy()
    
    # 1. likes_per_view = likes / views
    df_eng['likes_per_view'] = df_eng['likes'] / df_eng['views'].replace(0, np.nan)
    
    # 2. comments_per_view = comments / views  
    df_eng['comments_per_view'] = df_eng['comments'] / df_eng['views'].replace(0, np.nan)
    
    # 3. likes_per_100_views = (likes / views) * 100
    df_eng['likes_per_100_views'] = df_eng['likes_per_view'] * 100
    
    # 4. comments_per_100_views = (comments / views) * 100
    df_eng['comments_per_100_views'] = df_eng['comments_per_view'] * 100
    
    # 5. engagement_rate = (likes + comments) / views
    df_eng['engagement_rate'] = (df_eng['likes'] + df_eng['comments']) / df_eng['views'].replace(0, np.nan)
    
    # Fill NaN with 0 for database
    df_eng.fillna(0, inplace=True)
    
    print("✅ Extracted engagement features:")
    print(f"   • likes_per_100_views: {df_eng['likes_per_100_views'].mean():.2f}% avg")
    print(f"   • engagement_rate: {df_eng['engagement_rate'].mean():.4f} avg")
    
    return df_eng

def save_to_database(df):
    """Save to engagement_features table"""
    conn = get_connection()
    df.to_sql(ENGAGEMENT_TABLE, conn, if_exists='replace', index=False)
    conn.close()
    print(f"✅ Saved {len(df)} rows to '{ENGAGEMENT_TABLE}' table")

def preview_results():
    """Show sample results"""
    conn = get_connection()
    df_preview = pd.read_sql(f"""
        SELECT video_id, likes_per_100_views, engagement_rate, likes_per_view 
        FROM {ENGAGEMENT_TABLE} 
        ORDER BY engagement_rate DESC 
        LIMIT 5
    """, conn)
    conn.close()
    
    print("\n🔥 TOP 5 MOST ENGAGING VIDEOS:")
    print(df_preview.round(4))

def main():
    print("🚀 ENGAGEMENT FEATURES EXTRACTION")
    print("=" * 50)
    
    # 1. Load data
    df = load_raw_data()
    
    # 2. Create table
    create_engagement_table()
    
    # 3. Extract features
    df_eng = extract_engagement_features(df)
    
    # 4. Save to database
    save_to_database(df_eng)
    
    # 5. Show preview
    preview_results()
    
    print("\n🎉 DONE! Check your database:")
    print(f"   📊 Table: {ENGAGEMENT_TABLE}")
    print(f"   📈 Columns: likes_per_view, comments_per_view, likes_per_100_views, engagement_rate")

if __name__ == "__main__":
    main()