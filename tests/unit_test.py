import os
import sys
import unittest
from unittest.mock import patch
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import collect_data
import feature_extraction
import time_features
import content_features

# Collect Data
class TestCollectData(unittest.TestCase):

    @patch("collect_data.build")
    def test_get_client(self, mock_build):
        mock_build.return_value = "client"
        self.assertEqual(collect_data.get_youtube_client(), "client")

    def test_extract_features(self):
        item = {
            "id": "abc",
            "snippet": {
                "title": "Top 10!",
                "channelTitle": "Channel",
                "publishedAt": "2026-01-01T00:00:00Z",
                "description": "desc",
                "tags": ["a", "b"]
            },
            "statistics": {
                "viewCount": "1000",
                "likeCount": "100",
                "commentCount": "10"
            },
            "contentDetails": {"duration": "PT1M"}
        }

        r = collect_data.extract_features(item)

        self.assertEqual(r["video_id"], "abc")
        self.assertEqual(r["views"], 1000)
        self.assertEqual(r["tag_count"], 2)



# Engagement features
class TestEngagement(unittest.TestCase):

    def test_engagement(self):
        df = pd.DataFrame([{"video_id": "v1","views": 1000, "likes": 100, "comments": 20}])
        r = feature_extraction.extract_engagement_features(df).iloc[0]

        self.assertAlmostEqual(r["engagement_rate"], 0.12)



# Content features
class TestContent(unittest.TestCase):

    def test_content_features(self):
        df = pd.DataFrame([{
            "video_id": "v1",
            "desc_length": 100,
            "tag_count": 4,
            "title_length": 20,
            "title_word_count": 4,
            "has_number": 1,
            "has_question": 0,
            "has_exclamation": 1
        }])

        r = content_features.extract_content_features(df).iloc[0]

        self.assertEqual(r["title_clickbait_score"], 2)
        self.assertAlmostEqual(r["avg_word_length_title"], 5.0)

# Time features
class TestTime(unittest.TestCase):

    @patch("time_features.fetch_video_names")
    def test_time_features(self, mock_names):
        mock_names.return_value = {"v1": "Video"}

        df = pd.DataFrame([
            {"video_id": "v1", "published_at": "2026-01-01T10:00:00Z"}
        ])

        with patch("pandas.Timestamp.now") as mock_now:
            mock_now.return_value = pd.Timestamp("2026-01-05T00:00:00Z")

            r = time_features.extract_time_features(df).iloc[0]

            self.assertEqual(r["video_name"], "Video")
            self.assertEqual(r["publish_hour"], 10)
            self.assertEqual(r["days_since_publish"], 3)


#main
if __name__ == "__main__":
    unittest.main()
