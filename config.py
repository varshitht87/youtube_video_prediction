from dotenv import load_dotenv
import os

load_dotenv()

# Load from environment variables (safe way)
API_KEY = os.getenv("YOUTUBE_API_KEY")

YOUTUBE_API_SERVICE = "youtube"
YOUTUBE_API_VERSION = "v3"

DB_PATH = "youtube_data.db"
TABLE_NAME = "videos"

VIRAL_QUERIES = ["music"]
RESULTS_PER_QUERY = 10