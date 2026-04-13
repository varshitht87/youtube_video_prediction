# YouTube Viral Video Prediction System

---

## Project Overview

The pipeline consists of 4 main stages:

- Data Collection  
- Content Feature Extraction  
- Engagement Feature Extraction  
- Time Feature Extraction  

All processed data is stored in a local SQLite database (`youtube_data.db`).

---

##  Project Structure

```
.
├── collect_data.py          # Fetch videos using YouTube API
├── content_features.py      # Extract title & description-based features
├── feature_extraction.py    # Extract engagement metrics
├── time_features.py         # Extract time-based features
├── config.py                # Configuration (API keys, DB settings)
├── youtube_data.db          # SQLite database
├── requirements.txt         # Dependencies
└── README.md                # Project documentation
```

---

##  Setup Instructions

### 1. Clone Repository
```
git clone https://github.com/your-username/youtube-viral-prediction.git
cd youtube-viral-prediction
```

### 2. Create Virtual Environment
```
python3 -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows
```

### 3. Install Dependencies
```
pip install -r requirements.txt
```

---

## Configuration

Create a `.env` file or edit `config.py`:

```
API_KEY = "YOUR_YOUTUBE_API_KEY"

YOUTUBE_API_SERVICE = "youtube"
YOUTUBE_API_VERSION = "v3"

DB_PATH = "youtube_data.db"
TABLE_NAME = "videos"

VIRAL_QUERIES = ["music", "viral videos", "funny clips"]
RESULTS_PER_QUERY = 30
```

---

##  Step 1: Collect Data

Run:
```
python collect_data.py
```

### What it does:
- Searches YouTube videos using queries  
- Fetches:
  - Views  
  - Likes  
  - Comments  
  - Duration  
  - Title metadata  
- Labels videos as **viral (≥1M views)**  

---

##  Step 2: Content Feature Extraction

Run:
```
python content_features.py
```

### Extracted Features:
- Clickbait score  
- Average word length (title)  
- Description-to-title ratio  
- Tags per title word  

---

## Step 3: Engagement Feature Extraction

Run:
```
python feature_extraction.py
```

### Extracted Metrics:
- likes_per_view  
- comments_per_view  
- likes_per_100_views  
- comments_per_100_views  
- engagement_rate  

---

##  Step 4: Time Feature Extraction

Run:
```
python time_features.py
```

### Extracted Features:
- Publish hour  
- Day of week  
- Month  
- Weekend indicator  
- Days since published  

---

##  Feature Engineering Details

###  1. Content Features

```
title_clickbait_score = has_number + has_question + has_exclamation
avg_word_length_title = title_length / title_word_count
desc_to_title_ratio = desc_length / title_length
tags_per_title_word = tag_count / title_word_count
```

---

###  2. Engagement Features

```
likes_per_view = likes / views
comments_per_view = comments / views
likes_per_100_views = (likes / views) * 100
comments_per_100_views = (comments / views) * 100
engagement_rate = (likes + comments) / views
```

---

###  3. Time Features

```
publish_hour = hour(published_at)
publish_dayofweek = day_of_week(published_at)
publish_month = month(published_at)
is_weekend = 1 if publish_dayofweek in [5, 6] else 0
days_since_publish = current_date - published_at
```

---

###  4. Viral Label

```
is_viral = 1 if views >= 1_000_000 else 0
```

---

## Database Schema

### Tables Created:
- `videos` → Raw data  
- `content_features`  
- `engagement_features`  
- `time_features`  

---

##  Example Output

```
 TOP 5 MOST ENGAGING VIDEOS:
video_id   likes_per_100_views   engagement_rate
-----------------------------------------------
abc123     4.23                  0.0521
xyz789     3.89                  0.0487
```

---

##  Tech Stack

- Python  
- Pandas  
- NumPy  
- SQLite  
- YouTube Data API v3  
- isodate  

---

##  Future Improvements

- Add ML model for viral prediction  
- Feature importance analysis  
- Dashboard (Streamlit)  
- Real-time data pipeline  
