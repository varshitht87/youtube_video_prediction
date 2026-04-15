from flask import Flask, render_template, request, jsonify
import pandas as pd
import sqlite3
import os
import re
from collections import Counter
from datetime import datetime

# Import from config.py (as used in collect_data.py)
try:
    from config import DBPATH, TABLENAME
except ImportError:
    # Fallback defaults if config.py missing
    DBPATH = 'youtube_data.db'  
    TABLENAME = 'videos'

app = Flask(__name__)

DAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
STOPWORDS = {
    'the','a','an','and','or','for','to','of','in','on','at','with','by','is','are','be','from','this','that',
    'you','your','how','what','why','when','best','top','using','use','into','about','after','before','vs','official',
    'video','music','song','songs','viral','new'
}

TIME_RECOMMENDATIONS = {
    'Monday': 'Evening (6-9 PM)',
    'Tuesday': 'Morning (9-11 AM)', 
    'Wednesday': 'Afternoon (2-5 PM)',
    'Thursday': 'Evening (7-10 PM)',
    'Friday': 'Morning (10 AM-12 PM)',
    'Saturday': 'Afternoon (1-4 PM)',
    'Sunday': 'Morning (11 AM-2 PM)'
}

def load_rows(limit=2000):
    try:
        conn = sqlite3.connect(DBPATH)
        df = pd.read_sql(f"SELECT * FROM {TABLENAME} LIMIT {limit}", conn)
        conn.close()
        print(f"Loaded {len(df)} rows from {DBPATH}/{TABLENAME}")
        return df.to_dict('records')  # List of dicts matching CSV DictReader
    except Exception as e:
        print(f"DB Error: {e}. Run collect_data.py first to create/populate DB.")
        return []

def tokenize(text):
    return re.findall(r"[a-zA-Z0-9]+", (text or '').lower())

def parse_day(published_at):
    try:
        dt = datetime.fromisoformat((published_at or '').replace('Z', '+00:00'))
        return DAY_NAMES[dt.weekday()]
    except Exception:
        return None

def safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default

def similarity_score(row, title, description, duration, category):
    input_tokens = set(tokenize(title) + tokenize(description) + tokenize(category))
    row_tokens = set(tokenize(row.get('title', '')) + tokenize(row.get('channel_title', '')))
    keyword_overlap = len(input_tokens & row_tokens)

    row_duration = safe_int(row.get('duration_seconds', 0))
    duration_gap = abs(row_duration - safe_int(duration, 0))
    duration_score = max(0, 6 - duration_gap / 120)

    structure_score = 0
    if any(char.isdigit() for char in title) and safe_int(row.get('has_number', 0)) == 1:
        structure_score += 1
    if '?' in title and safe_int(row.get('has_question', 0)) == 1:
        structure_score += 1
    if '!' in title and safe_int(row.get('has_exclamation', 0)) == 1:
        structure_score += 1

    return keyword_overlap * 3 + duration_score + structure_score

def find_similar_videos(rows, title, description, duration, category, limit=15):
    scored = []
    for row in rows:
        score = similarity_score(row, title, description, duration, category)
        if score > 0:
            scored.append((score, row))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [row for _, row in scored[:limit]]

def recommend_publish_day(similar_rows):
    day_counts = Counter()
    for row in similar_rows:
        day = parse_day(row.get('published_at', ''))
        if day:
            day_counts[day] += 1
    
    if not day_counts:
        return 'Wednesday', 'Evening (6-9 PM)', 'No strong day pattern found - midweek evening fallback! ⏰'
    
    best_day, count = day_counts.most_common(1)[0]
    best_time = TIME_RECOMMENDATIONS.get(best_day, 'Afternoon (2-5 PM)')
    reason = f'{count} similar videos published on {best_day} got best results. {best_time} optimal! 🚀'
    return best_day, best_time, reason

def suggest_tags(similar_rows, title, description, existing_tags):
    existing = [t.strip() for t in re.split(r',|\n', existing_tags or '') if t.strip()]
    existing_lower = {t.lower() for t in existing}
    pool = []

    for row in similar_rows:
        pool.extend(tokenize(row.get('title', '')))
        pool.extend(tokenize(row.get('channel_title', '')))

    pool.extend(tokenize(title))
    pool.extend(tokenize(description))

    counts = Counter([t for t in pool if len(t) > 2 and t not in STOPWORDS])
    new_tags = []
    for token, _ in counts.most_common(25):
        if token not in existing_lower and token not in new_tags:
            new_tags.append(token)

    return {
        'existing': existing,
        'new_suggestions': new_tags[:10],
        'recommended': (existing + new_tags[:10])[:15]
    }

def supporting_features(title, description, tags, duration):
    tag_list = [t.strip() for t in re.split(r',|\n', tags or '') if t.strip()]
    title_words = title.split() if title else []
    return {
        'title_length': len(title or ''),
        'title_word_count': len(title_words),
        'desc_length': len(description or ''),
        'tag_count_input': len(tag_list),
        'duration_seconds': safe_int(duration),
        'has_number': int(any(c.isdigit() for c in (title or ''))),
        'has_question': int('?' in (title or '')),
        'has_exclamation': int('!' in (title or '')),
    }

@app.route('/', methods=['GET'])
def index():
    rows = load_rows(limit=100)  # Preview data count
    return render_template('index.html', row_count=len(rows), db_path=DBPATH)

@app.route('/analyze', methods=['POST'])
def analyze():
    title = (request.form.get('title') or '').strip()
    description = (request.form.get('description') or '').strip()
    tags = (request.form.get('tags') or '').strip()
    duration = request.form.get('duration') or '0'
    category = (request.form.get('category') or '').strip()

    rows = load_rows()  # Full dataset from DB
    similar_rows = find_similar_videos(rows, title, description, duration, category)
    best_day, best_time, reason = recommend_publish_day(similar_rows)
    tag_data = suggest_tags(similar_rows, title, description, tags)
    features = supporting_features(title, description, tags, duration)

    return render_template('index.html', form=request.form, result={
        'best_day': best_day,
        'best_time': best_time,
        'reason': reason,
        'matched_count': len(similar_rows),
        'matches': similar_rows[:8],
        'tag_data': tag_data,
        'features': features,
    }, row_count=len(rows), db_path=DBPATH)

@app.route('/api/data', methods=['GET'])
def api_data():
    """Bonus: JSON endpoint for DB data."""
    limit = request.args.get('limit', 50, type=int)
    rows = load_rows(limit=limit)
    return jsonify({'rows': rows, 'total_loaded': len(rows)})

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000,debug=True)