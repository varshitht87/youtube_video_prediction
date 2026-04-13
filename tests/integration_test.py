import os
import sys
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import collect_data
import feature_extraction


class TestDatabaseIntegration(unittest.TestCase):

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()

        self.raw_table = "videos"
        self.engagement_table = "engagement_features_test"

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _sample_raw_dataframe(self):
        return pd.DataFrame([
            {
                "video_id": "vid_1",
                "title": "Python Tutorial 1",
                "channel_title": "Code A",
                "published_at": "2026-01-01T00:00:00Z",
                "views": 1000,
                "likes": 100,
                "comments": 20,
                "duration_seconds": 300,
                "title_length": 17,
                "title_word_count": 3,
                "has_number": 1,
                "has_question": 0,
                "has_exclamation": 0,
                "desc_length": 50,
                "tag_count": 3,
                "is_viral": 0,
            },
            {
                "video_id": "vid_2",
                "title": "Amazing Video!",
                "channel_title": "Channel B",
                "published_at": "2026-01-02T00:00:00Z",
                "views": 2000000,
                "likes": 150000,
                "comments": 8000,
                "duration_seconds": 600,
                "title_length": 14,
                "title_word_count": 2,
                "has_number": 0,
                "has_question": 0,
                "has_exclamation": 1,
                "desc_length": 100,
                "tag_count": 5,
                "is_viral": 1,
            }
        ])

    def test_collect_data_save_and_load_database(self):
        df = self._sample_raw_dataframe()

        with patch.object(collect_data, "DB_PATH", self.db_path), \
             patch.object(collect_data, "TABLE_NAME", self.raw_table):

            collect_data.save_to_database(df)
            loaded_df = collect_data.load_from_database()

        self.assertEqual(len(loaded_df), 2)
        self.assertIn("video_id", loaded_df.columns)
        self.assertIn("views", loaded_df.columns)
        self.assertEqual(loaded_df.iloc[0]["video_id"], "vid_1")
        self.assertEqual(int(loaded_df.iloc[1]["views"]), 2000000)

    def test_feature_extraction_end_to_end(self):
        raw_df = self._sample_raw_dataframe()

        conn = sqlite3.connect(self.db_path)
        raw_df.to_sql(self.raw_table, conn, if_exists="replace", index=False)
        conn.close()

        with patch.object(feature_extraction, "DB_PATH", self.db_path), \
             patch.object(feature_extraction, "TABLE_NAME", self.raw_table), \
             patch.object(feature_extraction, "ENGAGEMENT_TABLE", self.engagement_table):

            loaded_raw_df = feature_extraction.load_raw_data()
            self.assertEqual(len(loaded_raw_df), 2)

            feature_extraction.create_engagement_table()
            df_eng = feature_extraction.extract_engagement_features(loaded_raw_df)
            feature_extraction.save_to_database(df_eng)

            conn = sqlite3.connect(self.db_path)
            saved_df = pd.read_sql(f"SELECT * FROM {self.engagement_table}", conn)
            conn.close()

        self.assertEqual(len(saved_df), 2)
        self.assertIn("likes_per_view", saved_df.columns)
        self.assertIn("comments_per_view", saved_df.columns)
        self.assertIn("likes_per_100_views", saved_df.columns)
        self.assertIn("comments_per_100_views", saved_df.columns)
        self.assertIn("engagement_rate", saved_df.columns)

        row1 = saved_df[saved_df["video_id"] == "vid_1"].iloc[0]
        self.assertAlmostEqual(row1["likes_per_view"], 0.1)
        self.assertAlmostEqual(row1["comments_per_view"], 0.02)
        self.assertAlmostEqual(row1["likes_per_100_views"], 10.0)
        self.assertAlmostEqual(row1["comments_per_100_views"], 2.0)
        self.assertAlmostEqual(row1["engagement_rate"], 0.12)

        row2 = saved_df[saved_df["video_id"] == "vid_2"].iloc[0]
        self.assertAlmostEqual(row2["likes_per_view"], 0.075)
        self.assertAlmostEqual(row2["comments_per_view"], 0.004)
        self.assertAlmostEqual(row2["likes_per_100_views"], 7.5)
        self.assertAlmostEqual(row2["comments_per_100_views"], 0.4)
        self.assertAlmostEqual(row2["engagement_rate"], 0.079)

    def test_feature_extraction_handles_zero_views_in_database_flow(self):
        df = pd.DataFrame([
            {
                "video_id": "zero_1",
                "title": "No Views Yet",
                "channel_title": "New Channel",
                "published_at": "2026-01-03T00:00:00Z",
                "views": 0,
                "likes": 10,
                "comments": 5,
                "duration_seconds": 100,
                "title_length": 11,
                "title_word_count": 3,
                "has_number": 0,
                "has_question": 0,
                "has_exclamation": 0,
                "desc_length": 20,
                "tag_count": 1,
                "is_viral": 0,
            }
        ])

        conn = sqlite3.connect(self.db_path)
        df.to_sql(self.raw_table, conn, if_exists="replace", index=False)
        conn.close()

        with patch.object(feature_extraction, "DB_PATH", self.db_path), \
             patch.object(feature_extraction, "TABLE_NAME", self.raw_table), \
             patch.object(feature_extraction, "ENGAGEMENT_TABLE", self.engagement_table):

            raw_df = feature_extraction.load_raw_data()
            df_eng = feature_extraction.extract_engagement_features(raw_df)
            feature_extraction.save_to_database(df_eng)

            conn = sqlite3.connect(self.db_path)
            saved_df = pd.read_sql(f"SELECT * FROM {self.engagement_table}", conn)
            conn.close()

        row = saved_df.iloc[0]
        self.assertEqual(row["likes_per_view"], 0)
        self.assertEqual(row["comments_per_view"], 0)
        self.assertEqual(row["likes_per_100_views"], 0)
        self.assertEqual(row["comments_per_100_views"], 0)
        self.assertEqual(row["engagement_rate"], 0)


if __name__ == "__main__":
    unittest.main()