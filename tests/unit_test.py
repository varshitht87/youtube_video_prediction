import os
import sys
import unittest
from unittest.mock import patch
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import collect_data
import feature_extraction


class TestCollectDataUnit(unittest.TestCase):

    @patch("collect_data.build")
    def test_get_youtube_client_calls_build(self, mock_build):
        mock_build.return_value = "fake_client"

        client = collect_data.get_youtube_client()

        self.assertEqual(client, "fake_client")

    def test_extract_features_returns_expected_fields(self):
        item = {
            "id": "abc123",
            "snippet": {
                "title": "Top 10 Python Tricks!",
                "channelTitle": "Code Channel",
                "publishedAt": "2026-01-01T00:00:00Z",
                "description": "Useful Python tips",
                "tags": ["python", "tips", "coding"]
            },
            "statistics": {
                "viewCount": "1500000",
                "likeCount": "50000",
                "commentCount": "1200"
            },
            "contentDetails": {
                "duration": "PT5M30S"
            }
        }

        result = collect_data.extract_features(item)

        self.assertEqual(result["video_id"], "abc123")
        self.assertEqual(result["views"], 1500000)
        self.assertEqual(result["likes"], 50000)
        self.assertEqual(result["comments"], 1200)
        self.assertEqual(result["duration_seconds"], 330)
        self.assertEqual(result["tag_count"], 3)
        self.assertEqual(result["is_viral"], 1)


class TestFeatureExtractionUnit(unittest.TestCase):

    def test_extract_engagement_features_computes_ratios(self):
        df = pd.DataFrame([
            {"video_id": "v1", "views": 1000, "likes": 100, "comments": 20}
        ])

        result = feature_extraction.extract_engagement_features(df)
        row = result.iloc[0]

        self.assertAlmostEqual(row["likes_per_view"], 0.1)
        self.assertAlmostEqual(row["comments_per_view"], 0.02)
        self.assertAlmostEqual(row["likes_per_100_views"], 10.0)
        self.assertAlmostEqual(row["comments_per_100_views"], 2.0)
        self.assertAlmostEqual(row["engagement_rate"], 0.12)


if __name__ == "__main__":
    unittest.main()