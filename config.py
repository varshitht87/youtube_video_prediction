from dotenv import load_dotenv
import os

load_dotenv()

# Load from environment variables (safe way)
API_KEY = os.getenv("YOUTUBE_API_KEY")

YOUTUBE_API_SERVICE = "youtube"
YOUTUBE_API_VERSION = "v3"

DB_PATH = "youtube_data.db"
TABLE_NAME = "videos"

VIRAL_QUERIES = ["most viral videos 2025",
    "funny viral moments 2025",
    "viral life hacks 2025",
    "viral music 2025",
    "viral challenge 2025",
    "viral news 2025",
    "viral tech review 2025",
    "viral food recipe 2025",
]
RESULTS_PER_QUERY = 30