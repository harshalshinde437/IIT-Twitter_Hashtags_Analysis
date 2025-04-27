# twitter_dashboard.py
import re
import toml
import time
import folium
import tweepy
import sqlite3
import numpy as np
import pandas as pd
import streamlit as st
import geopandas as gpd
import plotly.express as px
import matplotlib.pyplot as plt
import country_converter as coco
from textblob import TextBlob
from wordcloud import WordCloud
from pathlib import Path
from shapely.geometry import Point
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from streamlit_folium import folium_static
from pyspark.sql.functions import udf, col
from streamlit_autorefresh import st_autorefresh
from pyspark.sql.types import StringType, FloatType
from folium.plugins import HeatMap
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from gensim import corpora, models
from gensim.utils import simple_preprocess
import spacy
import warnings

warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
# Load spaCy model for NLP
try:
    nlp = spacy.load("en_core_web_sm")
except:
    from spacy.cli import download
    download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

# Configuration
SQLITE_DB = "twitter_analysis.db"
DEFAULT_REFRESH = 80  # minutes
MAX_TWEETS = 30000
COLOR_PALETTE = px.colors.qualitative.Vivid

# Initialize Spark
spark = SparkSession.builder \
    .appName("TwitterAnalysis") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Database Setup
def init_db():
    conn = sqlite3.connect(SQLITE_DB)
    c = conn.cursor()
    
    # Create tables with enhanced schema
    c.execute('''CREATE TABLE IF NOT EXISTS searches
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  topic TEXT NOT NULL,
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                  source TEXT DEFAULT 'api')''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS tweets
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  search_id INTEGER,
                  tweet_id TEXT,
                  text TEXT,
                  cleaned_text TEXT,
                  created_at DATETIME,
                  sentiment TEXT,
                  retweet_count INTEGER,
                  like_count INTEGER,
                  username TEXT,
                  user_followers INTEGER,
                  location TEXT DEFAULT NULL,
                  platform TEXT,
                  year INTEGER,
                  month INTEGER,
                  day INTEGER,
                  hour INTEGER,
                  FOREIGN KEY(search_id) REFERENCES searches(id))''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS hashtags
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  search_id INTEGER,
                  hashtag TEXT,
                  count INTEGER,
                  FOREIGN KEY(search_id) REFERENCES searches(id))''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS analytics
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  search_id INTEGER,
                  metric TEXT,
                  value TEXT,
                  FOREIGN KEY(search_id) REFERENCES searches(id))''')
    
    # Add indexes for performance
    c.execute("CREATE INDEX IF NOT EXISTS idx_searches_topic ON searches(topic)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tweets_search_id ON tweets(search_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hashtags_hashtag ON hashtags(hashtag)")
    
    conn.commit()
    conn.close()

init_db()

# Twitter Client
@st.cache_resource
def get_twitter_client():
    secrets_path = Path(__file__).parent / "secrets" / "secrets.toml"
    with open(secrets_path, "r") as f:
        secrets = toml.load(f)
    
    return tweepy.Client(
        bearer_token=secrets["TWITTER"]["BEARER_TOKEN"],
        consumer_key=secrets["TWITTER"]["API_KEY"],
        consumer_secret=secrets["TWITTER"]["API_SECRET"],
        access_token=secrets["TWITTER"]["ACCESS_TOKEN"],
        access_token_secret=secrets["TWITTER"]["ACCESS_TOKEN_SECRET"]
    )

# Text Processing Functions
def clean_tweet(text):
    if not text or not isinstance(text, str):
        return ""
    text = re.sub(r'@[A-Za-z0-9_]+', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r'RT[\s]+', '', text)
    text = re.sub(r'https?:\/\/\S+', '', text)
    return text.strip()

def get_sentiment(text):
    if not text:
        return "Neutral"
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0.1:
        return "Positive"
    elif analysis.sentiment.polarity < -0.1:
        return "Negative"
    return "Neutral"

def extract_entities(text):
    if not text or not isinstance(text, str):
        return []
    doc = nlp(text)
    return [(ent.text, ent.label_) for ent in doc.ents]

def topic_modeling(texts):
    if not texts:
        return []
    texts = [simple_preprocess(str(text)) for text in texts if text]
    if not texts:
        return []
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    lda_model = models.LdaModel(corpus=corpus, id2word=dictionary, num_topics=3, random_state=42)
    return lda_model.print_topics()

# Data Processing Functions
@st.cache_data(ttl=3600, show_spinner=False)
def extract_hashtags(text):
    return re.findall(r'#(\w+)', str(text))

@st.cache_data(ttl=3600, show_spinner=False)
def process_tweets(tweets, topic=None):
    if not tweets:
        return [], {}
    
    # Convert to DataFrame for vectorized operations
    df = pd.DataFrame([{
        'tweet_id': tweet.id if hasattr(tweet, 'id') else str(hash(str(tweet.text))),
        'text': tweet.text if hasattr(tweet, 'text') else str(tweet),
        'created_at': tweet.created_at if hasattr(tweet, 'created_at') else datetime.now(),
        'retweet_count': tweet.public_metrics['retweet_count'] if hasattr(tweet, 'public_metrics') else getattr(tweet, 'retweet_count', 0),
        'like_count': tweet.public_metrics['like_count'] if hasattr(tweet, 'public_metrics') else getattr(tweet, 'like_count', 0),
        'username': tweet.username if hasattr(tweet, 'username') else getattr(tweet, 'user', 'unknown'),
        'user_followers': tweet.user_followers if hasattr(tweet, 'user_followers') else 0,
        'location': getattr(tweet, 'location', None)
    } for tweet in tweets])
    
    # Vectorized cleaning and processing
    df['cleaned_text'] = df['text'].apply(clean_tweet)
    df['sentiment'] = df['cleaned_text'].apply(get_sentiment)
    
    # Filter hashtags by topic
    hashtag_counts = {}
    for text in df['text']:
        hashtags = extract_hashtags(text)
        if topic:
            hashtags = [tag.lower() for tag in hashtags if topic.lower() in tag.lower()]
        for tag in hashtags:
            hashtag_counts[tag] = hashtag_counts.get(tag, 0) + 1
    
    # Convert back to list of dicts for compatibility
    processed = df.to_dict('records')
    
    return processed, hashtag_counts

# Database Functions
def save_to_db(topic, tweets_data, hashtag_counts, source='api'):
    conn = sqlite3.connect(SQLITE_DB)
    c = conn.cursor()
    
    # Check if this topic exists in the last hour
    c.execute('''SELECT id FROM searches 
                 WHERE topic = ? AND timestamp > datetime('now', '-1 hour')
                 ORDER BY timestamp DESC LIMIT 1''', (topic,))
    result = c.fetchone()
    
    if result:
        search_id = result[0]  # Use existing search_id
    else:
        # Create new search entry
        c.execute("INSERT INTO searches (topic, source) VALUES (?, ?)", (topic, source))
        search_id = c.lastrowid
    
    for tweet in tweets_data:
        dt = pd.to_datetime(tweet['created_at'])
        c.execute('''INSERT INTO tweets 
                     (search_id, tweet_id, text, cleaned_text, created_at, 
                      sentiment, retweet_count, like_count, username, 
                      user_followers, location, platform, year, month, day, hour)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (search_id, str(tweet['tweet_id']), str(tweet['text']), str(tweet['cleaned_text']),
                   str(tweet['created_at']), str(tweet['sentiment']), int(tweet['retweet_count']),
                   int(tweet['like_count']), str(tweet['username']), int(tweet['user_followers']),
                   str(tweet.get('location')) if tweet.get('location') else None,
                   tweet.get('platform', 'twitter'),
                   dt.year, dt.month, dt.day, dt.hour))
    
    for tag, count in hashtag_counts.items():
        c.execute("INSERT INTO hashtags (search_id, hashtag, count) VALUES (?, ?, ?)",
                  (search_id, str(tag), int(count)))
    
    sentiment_counts = pd.Series([t['sentiment'] for t in tweets_data]).value_counts().to_dict()
    for sentiment, count in sentiment_counts.items():
        c.execute("INSERT INTO analytics (search_id, metric, value) VALUES (?, ?, ?)",
                  (search_id, f"sentiment_{sentiment}", str(count)))
    
    conn.commit()
    conn.close()
    return search_id

@st.cache_data(ttl=600)
def get_search_history():
    conn = sqlite3.connect(SQLITE_DB)
    c = conn.cursor()
    c.execute("SELECT DISTINCT topic FROM searches ORDER BY timestamp DESC LIMIT 10")
    history = [row[0] for row in c.fetchall()]
    conn.close()
    return history

@st.cache_data(ttl=600, show_spinner=False)
def get_cached_data(topic):
    conn = sqlite3.connect(SQLITE_DB)
    
    # Get the most recent search_id for this topic
    search_id = pd.read_sql(
        f"SELECT id FROM searches WHERE topic = '{topic}' ORDER BY timestamp DESC LIMIT 1", 
        conn
    )['id'].iloc[0] if pd.read_sql(
        f"SELECT 1 FROM searches WHERE topic = '{topic}' LIMIT 1", 
        conn
    ).shape[0] > 0 else None
    
    if not search_id:
        conn.close()
        return None
    
    # Get only data for this specific search_id (topic)
    tweets = pd.read_sql(f"SELECT * FROM tweets WHERE search_id = {search_id}", conn)
    hashtags = pd.read_sql(f"SELECT hashtag, count FROM hashtags WHERE search_id = {search_id} ORDER BY count DESC LIMIT 10", conn)
    analytics = pd.read_sql(f"SELECT metric, value FROM analytics WHERE search_id = {search_id}", conn)
    
    conn.close()
    
    return {
        "tweets": tweets,
        "hashtags": hashtags,
        "analytics": analytics
    }

# Visualization Functions
def plot_sentiment(data, topic):
    sentiment_counts = data['analytics'][data['analytics']['metric'].str.startswith('sentiment_')]
    sentiment_counts['metric'] = sentiment_counts['metric'].str.replace('sentiment_', '')
    sentiment_counts['value'] = pd.to_numeric(sentiment_counts['value'])
    
    fig = px.pie(sentiment_counts, names='metric', values='value', 
                 title=f'Sentiment Distribution for "{topic}"',
                 color='metric',
                 color_discrete_map={
                     'Positive': COLOR_PALETTE[0],
                     'Neutral': COLOR_PALETTE[2],
                     'Negative': COLOR_PALETTE[1]
                 },
                 hole=0.3)
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, use_container_width=True)

def plot_tweet_volume(data, topic):
    tweets = data['tweets']
    tweets['hour'] = pd.to_datetime(tweets['created_at']).dt.floor('H')
    volume = tweets.groupby('hour').size().reset_index(name='count')
    
    fig = px.area(volume, x='hour', y='count',
                 title=f'Tweet Volume Over Time for "{topic}"',
                 color_discrete_sequence=[COLOR_PALETTE[3]])
    fig.update_xaxes(rangeslider_visible=True)
    st.plotly_chart(fig, use_container_width=True)

def plot_top_hashtags(data, topic):
    fig = px.bar(data['hashtags'], x='hashtag', y='count', 
                 title=f'Top Hashtags Containing "{topic}"',
                 color='hashtag',
                 color_discrete_sequence=COLOR_PALETTE)
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

def plot_wordcloud(data, topic):
    text = " ".join(tweet for tweet in data['tweets']['cleaned_text'] if isinstance(tweet, str))
    if not text:
        st.warning("No text available for word cloud")
        return
    
    wordcloud = WordCloud(width=800, height=400, 
                         background_color='black',
                         colormap='viridis').generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    st.pyplot(plt)


def plot_top_users(data, topic):
    # Create two columns for side-by-side charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Existing: Top users by tweet count
        top_users_by_tweets = data['tweets'].groupby('username').agg({
            'tweet_id': 'count',
            'retweet_count': 'sum'
        }).sort_values('tweet_id', ascending=False).head(5)
        
        fig1 = px.bar(top_users_by_tweets, 
                     x=top_users_by_tweets.index, 
                     y='tweet_id',
                     title=f'Top 10 Users by Tweet Volume (Topic: "{topic}")',
                     color=top_users_by_tweets.index,
                     color_discrete_sequence=COLOR_PALETTE,
                     labels={'username': 'User', 'tweet_id': 'Tweet Count'})
        fig1.update_layout(showlegend=False, xaxis_tickangle=-45)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # New: Top users by follower count
        top_users_by_followers = data['tweets'].sort_values('user_followers', ascending=False) \
            .drop_duplicates('username') \
            .head(10)[['username', 'user_followers']]
        
        fig2 = px.bar(top_users_by_followers,
                     x='username',
                     y='user_followers',
                     title=f'Top 10 Influencers by Followers (Topic: "{topic}")',
                     color='username',
                     color_discrete_sequence=COLOR_PALETTE,
                     labels={'username': 'User', 'user_followers': 'Followers Count'})
        fig2.update_layout(showlegend=False, xaxis_tickangle=-45)
        fig2.update_yaxes(type="log")  # Logarithmic scale for better visualization
        st.plotly_chart(fig2, use_container_width=True)


def plot_engagement(data, topic):
    df = data['tweets']
    fig = px.scatter(
        df, 
        x='retweet_count', 
        y='like_count', 
        color='sentiment',
        color_discrete_map={
            'Positive': COLOR_PALETTE[0],
            'Neutral': COLOR_PALETTE[2],
            'Negative': COLOR_PALETTE[1]
        },
        hover_data=['text'],
        title=f'Engagement Analysis for "{topic}"',
        log_x=True,
        log_y=True
    )
    st.plotly_chart(fig, use_container_width=True)

def plot_entities(data, topic):
    all_entities = []
    for text in data['tweets']['cleaned_text']:
        if isinstance(text, str):
            all_entities.extend(extract_entities(text))
    
    if not all_entities:
        st.warning("No named entities found in tweets")
        return
    
    entity_df = pd.DataFrame(all_entities, columns=['Entity', 'Type'])
    entity_counts = entity_df.groupby(['Entity', 'Type']).size().reset_index(name='Count')
    
    fig = px.bar(entity_counts.nlargest(20, 'Count'), 
                 x='Entity', y='Count', color='Type',
                 title=f'Named Entities in "{topic}" Discussions',
                 color_discrete_sequence=COLOR_PALETTE)
    st.plotly_chart(fig, use_container_width=True)

def plot_geographical_heatmap(data, topic):
    try:
        tweets_df = data['tweets']
        
        # Standardize location text (uppercase)
        tweets_df['location'] = tweets_df['location'].str.upper().str.strip()
        tweets_df['location'] = tweets_df['location'].replace(
            ['', 'UNKNOWN', 'NONE', 'NULL', None], np.nan
        ).dropna()
        
        if tweets_df['location'].empty:
            st.warning("No valid location data available for heatmap")
            return
        
        # Count tweets by standardized location
        location_counts = tweets_df['location'].value_counts().reset_index()
        location_counts.columns = ['location', 'count']
        
        geolocator = Nominatim(user_agent="twitter_dashboard")
        cc = coco.CountryConverter()
        
        @st.cache_data(ttl=3600)
        def get_coordinates(location):
            try:
                country = cc.convert(location, to='name_short')
                if country != 'not found':
                    location = country
                
                location_obj = geolocator.geocode(location, timeout=10)
                if location_obj:
                    return (location_obj.latitude, location_obj.longitude)
                return None
            except (GeocoderTimedOut, GeocoderUnavailable, AttributeError):
                return None
        
        location_coords = {}
        for loc in location_counts['location'].unique():
            if pd.notna(loc):
                coords = get_coordinates(str(loc))
                if coords:
                    location_coords[loc] = coords
        
        heat_data = []
        for _, row in location_counts.iterrows():
            loc = row['location']
            if loc in location_coords:
                lat, lon = location_coords[loc]
                heat_data.append([lat, lon, row['count']])
        
        if not heat_data:
            st.warning("Could not geocode any locations for heatmap")
            return
        
        # Create the map
        lats = [point[0] for point in heat_data]
        lons = [point[1] for point in heat_data]
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=2)
        
        # Add heatmap
        HeatMap(heat_data, radius=15, blur=10).add_to(m)
        
        # Add markers for top locations
        top_locations = location_counts.nlargest(5, 'count')
        for _, row in top_locations.iterrows():
            loc = row['location']
            if loc in location_coords:
                lat, lon = location_coords[loc]
                folium.Marker(
                    [lat, lon],
                    popup=f"{loc}: {row['count']} tweets",
                    icon=folium.Icon(color='red', icon='info-sign')
                ).add_to(m)
        
        st.subheader(f"Geographical Heatmap for '{topic}'")
        folium_static(m, width=1300, height=600)
        
    except Exception as e:
        st.error(f"Could not generate heatmap: {str(e)}")

def detect_crisis(data, topic):
    try:
        tweets = data['tweets']
        crisis_keywords = [
            'emergency', 'disaster', 'help', 'urgent', 'ambulance', 'police'
            'flood', 'earthquake', 'fire', 'attack', 'danger', 'high alart', 'alart', 'tsunami'
        ]
        
        crisis_tweets = tweets[tweets['cleaned_text'].str.contains(
            '|'.join(crisis_keywords), case=False, na=False)]
        crisis_count = len(crisis_tweets)
        total_tweets = len(tweets)
        crisis_score = min(100, (crisis_count / total_tweets) * 1000) if total_tweets > 0 else 0
        
        st.subheader(f"Crisis Detection for '{topic}'")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Crisis Score",f"{crisis_score:.1f}", 
                     "🔴 EXTREME ALERT..!" if crisis_score > 75 else 
                     "🟠 HIGH ALERT" if crisis_score > 55 else 
                     "🟡 MODERATED ALERT" if crisis_score > 25 else 
                     "🟢 Normal, Everything looks good..!", 
                     "inverse" if crisis_score >= 55 else
                     "normal" if crisis_score >= 25 else
                     "off")
        with col2:
            st.metric("Crisis-related Tweets", f"{crisis_count}/{total_tweets}")
        
        if crisis_count > 0:
            with st.expander("View crisis-related tweets"):
                st.dataframe(crisis_tweets[['text', 'sentiment']].head(15))
    except Exception as e:
        st.error(f"Crisis detection error: {str(e)}")

# Enhanced CSV Import Functionality
def import_from_csv(uploaded_file, topic_name):
    try:
        # Read CSV with error handling
        try:
            df = pd.read_csv(uploaded_file)
        except Exception as e:
            st.error(f"Failed to read CSV file: {str(e)}")
            return False, None

        # Filter rows that mention the topic
        topic_lower = topic_name.lower()
        df = df[df['Hashtags'].str.lower().str.contains(topic_lower, na=False)]
        
        if len(df) == 0:
            st.error(f"No rows found containing the topic '{topic_name}'")
            return False, None

        # Standardize column names with flexible mapping
        column_mapping = {
            'Text': 'text',
            'text': 'text',
            'Tweet': 'text',
            'Tweet-Text': 'text',
            'Date': 'created_at',
            'DateTime': 'created_at',
            'Time': 'created_at',
            'Timestamp': 'created_at',
            'User': 'username',
            'Username': 'username',
            'ScreenName': 'username',
            'Author': 'username',
            'Retweets': 'retweet_count',
            'Likes': 'like_count',
            'Favorites': 'like_count',
            'Country': 'location',
            'Location': 'location',
            'Place': 'location',
            'Source': 'platform',
            'Device': 'platform',
            'Hashtags': 'hashtags',
            'hashtags': 'hashtags'
        }
        
        # Apply column name standardization
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

        # Ensure required columns exist
        if 'text' not in df.columns:
            st.error("CSV must contain a text column with tweet content")
            return False, None

        # Set defaults for missing columns
        defaults = {
            'created_at': datetime.now(),
            'retweet_count': 0,
            'like_count': 0,
            'username': 'unknown',
            'user_followers': 0,
            'location': None,
            'platform': 'twitter',
            'hashtags': ''
        }
        
        for col, default in defaults.items():
            if col not in df.columns:
                df[col] = default

        # Convert data types
        df['retweet_count'] = pd.to_numeric(df['retweet_count'], errors='coerce').fillna(0).astype(int)
        df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce').fillna(0).astype(int)
        df['user_followers'] = pd.to_numeric(df['user_followers'], errors='coerce').fillna(0).astype(int)
        
        # Process each tweet with proper type conversion
        processed_tweets = []
        hashtag_counts = {}
        
        for _, row in df.iterrows():
            # Clean and analyze text
            cleaned_text = clean_tweet(str(row['text']))
            sentiment = get_sentiment(cleaned_text)
            
            # Handle datetime conversion
            try:
                created_at = pd.to_datetime(row['created_at'])
            except:
                created_at = datetime.now()
            
            # Extract hashtags
            hashtags = []
            if pd.notna(row['hashtags']):
                if isinstance(row['hashtags'], str):
                    if row['hashtags'].startswith('['):
                        try:
                            hashtags = eval(row['hashtags'])
                        except:
                            hashtags = extract_hashtags(row['hashtags'])
                    else:
                        hashtags = extract_hashtags(row['hashtags'])
                elif isinstance(row['hashtags'], list):
                    hashtags = row['hashtags']
            
            # Also extract from text
            hashtags.extend(extract_hashtags(row['text']))
            
            # Filter hashtags by topic and count
            hashtags = [str(tag).lower() for tag in hashtags if topic_name.lower() in str(tag).lower()]
            for tag in hashtags:
                hashtag_counts[tag] = hashtag_counts.get(tag, 0) + 1
            
            # Create consistent tweet data structure
            tweet_data = {
                'tweet_id': str(hash(str(row['text']))),
                'text': str(row['text']),
                'cleaned_text': cleaned_text,
                'created_at': created_at,
                'sentiment': sentiment,
                'retweet_count': int(row['retweet_count']),
                'like_count': int(row['like_count']),
                'username': str(row['username']),
                'user_followers': int(row['user_followers']),
                'location': str(row['location']) if pd.notna(row['location']) else None,
                'platform': str(row['platform']),
                'year': created_at.year,
                'month': created_at.month,
                'day': created_at.day,
                'hour': created_at.hour
            }
            
            processed_tweets.append(tweet_data)
        
        # Save to database with transaction
        conn = sqlite3.connect(SQLITE_DB)
        try:
            c = conn.cursor()
            
            # First check if this topic exists in recent searches (last 24 hours)
            # AND timestamp > datetime('now', '-1 day')
            c.execute('''SELECT id FROM searches 
                        WHERE LOWER(topic) = LOWER(?)                         
                        ORDER BY timestamp DESC LIMIT 1''', (topic_name,))
            result = c.fetchone()

            if result:
                # Use existing search_id
                search_id = result[0]
                st.info(f"Appending to existing topic analysis for '{topic_name}'")
            else:
                # Create new search entry
                c.execute("INSERT INTO searches (topic, source) VALUES (?, ?)", 
                        (topic_name, 'csv'))
                search_id = c.lastrowid
                st.info(f"Created new topic analysis for '{topic_name}'")
            
            # Insert tweets with explicit type conversion
            for tweet in processed_tweets:
                c.execute('''INSERT INTO tweets 
                         (search_id, tweet_id, text, cleaned_text, created_at, 
                          sentiment, retweet_count, like_count, username, 
                          user_followers, location, platform, year, month, day, hour)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (
                          int(search_id),
                          str(tweet['tweet_id']),
                          str(tweet['text']),
                          str(tweet['cleaned_text']),
                          str(tweet['created_at']),
                          str(tweet['sentiment']),
                          int(tweet['retweet_count']),
                          int(tweet['like_count']),
                          str(tweet['username']),
                          int(tweet['user_followers']),
                          str(tweet['location']) if tweet['location'] else None,
                          str(tweet['platform']),
                          int(tweet['year']),
                          int(tweet['month']),
                          int(tweet['day']),
                          int(tweet['hour'])
                      ))
            
            # Insert hashtags
            for tag, count in hashtag_counts.items():
                c.execute("INSERT INTO hashtags (search_id, hashtag, count) VALUES (?, ?, ?)",
                         (int(search_id), str(tag), int(count)))
            
            # Insert analytics
            sentiment_counts = pd.Series([t['sentiment'] for t in processed_tweets]).value_counts().to_dict()
            for sentiment, count in sentiment_counts.items():
                c.execute("INSERT INTO analytics (search_id, metric, value) VALUES (?, ?, ?)",
                         (int(search_id), f"sentiment_{sentiment}", str(count)))
            
            conn.commit()
            return True, df
            
        except sqlite3.Error as e:
            conn.rollback()
            st.error(f"Database error during import: {str(e)}")
            return False, None
        finally:
            conn.close()
            
    except Exception as e:
        st.error(f"CSV import failed: {str(e)}")
        return False, None

# Streamlit App
def main():
    st.set_page_config(page_title="Twitter Hashtag Analysis", layout="wide")
    
    # Title and description
    st.header(":bird: :blue[_Twitter_] Hashtags Analysis Dashboard", divider="violet")
    st.subheader(''' :gray[_Contributors_ :-]
    - :green[Harshal Shinde] &mdash; :orange[M24DE3037]
    - :green[Aishwarya Salunkhe] &mdash; :orange[M24DE3006]
    ''')
    st.divider()
    
    # Initialize session state
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.now() - timedelta(minutes=DEFAULT_REFRESH + 1)
    if 'current_topic' not in st.session_state:
        st.session_state.current_topic = ""
    if 'search_history' not in st.session_state:
        st.session_state.search_history = get_search_history()
    
    # Sidebar controls
    with st.sidebar:
        st.header("Controls")
        
        # Data source toggle
        data_source = st.radio(
            "Data Source Mode",
            ["📁 Database (Default)", "🐦 Live Twitter API"],
            index=0,
            help="Switch between cached and real-time data"
        )
        
        # API-specific controls
        if data_source == "🐦 Live Twitter API":
            refresh_interval = st.slider(
                "Auto-refresh interval (minutes)",
                min_value=15,
                max_value=120,
                value=DEFAULT_REFRESH,
                step=5
            )
            
            tweet_limit = st.slider(
                "Max tweets per request",
                min_value=10,
                max_value=100,
                value=20,
                step=5,
                help="Lower values conserve your Twitter API quota"
            )
            st.caption(f"🔐 API REQUEST Quota = :green[**{100//tweet_limit}**] Requests/month")
        
            # Manual refresh button
            if st.button("Refresh Now"):
                st.session_state.last_refresh = datetime.now() - timedelta(minutes=refresh_interval + 1)
                st.rerun()
        
        st.divider()
        
        # CSV import section
        st.subheader("Import CSV Data")
        uploaded_file = st.file_uploader(
            "Upload tweet CSV", 
            type=["csv"],
            help="Expected columns: Text, Timestamp, User, Hashtags, Retweets, Likes, Location"
        )
        
        if uploaded_file:
            topic_name = st.text_input("Enter topic name for this import")
            if st.button("Import to Database") and topic_name:
                with st.spinner(f"Importing tweets about '{topic_name}'..."):
                    success, imported_df = import_from_csv(uploaded_file, topic_name)
                    if success:
                        st.success(f"Successfully imported {len(imported_df)} tweets about '{topic_name}'")
                        if topic_name not in st.session_state.search_history:
                            st.session_state.search_history.insert(0, topic_name)
                            st.session_state.search_history = st.session_state.search_history[:10]
                        st.rerun()

        st.divider()
        
        # Search history
        st.subheader("Search History")
        for topic in st.session_state.search_history:
            if st.button(topic):
                st.session_state.current_topic = topic
                st.rerun()
    
    # Main search area
    col1, col2 = st.columns([4, 1])
    with col1:
        topic = st.text_input(
            "Enter a hashtag or topic to analyze",
            value=st.session_state.current_topic,
            key="search_input"
        )
    with col2:
        st.write("")  # Spacer
        if st.button("Analyze"):
            st.session_state.current_topic = topic
            if topic and topic not in st.session_state.search_history:
                st.session_state.search_history.insert(0, topic)
                if len(st.session_state.search_history) > 10:
                    st.session_state.search_history = st.session_state.search_history[:10]
            st.session_state.last_refresh = datetime.now() - timedelta(minutes=refresh_interval + 1)
            st.rerun()
    
    # Check if we need to refresh data (only for API mode)
    if (data_source == "🐦 Live Twitter API" and 
        (datetime.now() - st.session_state.last_refresh).total_seconds() > refresh_interval * 60):
        st.session_state.last_refresh = datetime.now()
        st.rerun()
    
    # Display analysis if we have a topic
    if st.session_state.current_topic:
        current_topic = st.session_state.current_topic
        st.header(f"Analysis for: :blue[_#{current_topic}_]")
        
        # Get data based on selected source
        cached_data = get_cached_data(current_topic)
        should_fetch = (data_source == "🐦 Live Twitter API")
        
        if cached_data and not should_fetch:
            st.info("Showing data from database. Switch to 'Live Twitter API' mode for real-time data.")
        
        # Fetch new data if needed
        if should_fetch:
            try:
                with st.spinner(f"Fetching {tweet_limit} tweets about #{current_topic}..."):
                    try:
                        client = get_twitter_client()
                        tweets = client.search_recent_tweets(
                            query=f"#{current_topic} -is:retweet",
                            max_results=tweet_limit,
                            tweet_fields=["created_at", "public_metrics", "author_id"],
                            user_fields=["username", "public_metrics"],
                            expansions=["author_id"]
                        )
                        
                        if not tweets.data:
                            st.warning(f"No tweets found containing '{current_topic}'")
                            cached_data = get_cached_data(current_topic)
                            return
                        
                        users = {u.id: u for u in tweets.includes['users']}
                        processed_tweets = []
                        for tweet in tweets.data:
                            user = users[tweet.author_id]
                            processed_tweets.append({
                                "text": tweet.text,
                                "created_at": tweet.created_at,
                                "public_metrics": tweet.public_metrics,
                                "username": user.username,
                                "user_followers": user.public_metrics['followers_count'],
                                "location": None
                            })
                        
                        tweets_data, hashtag_counts = process_tweets([
                            type('Tweet', (), {
                                'text': t['text'],
                                'id': str(i),
                                'created_at': t['created_at'],
                                'public_metrics': t['public_metrics'],
                                'username': t['username'],
                                'user_followers': t['user_followers'],
                                'location': t['location']
                            }) for i, t in enumerate(processed_tweets)
                        ], current_topic)
                        
                        search_id = save_to_db(current_topic, tweets_data, hashtag_counts)
                        cached_data = get_cached_data(current_topic)
                        
                    except tweepy.TooManyRequests as e:
                        reset_time = e.reset_in if hasattr(e, 'reset_in') else 15
                        st.error(f"Twitter API rate limit exceeded. Try again in {reset_time} minutes.")
                        st.info(f"Currently showing cached data.")
                        cached_data = get_cached_data(current_topic)
                        
                    except tweepy.TweepyException as e:
                        st.error(f"Twitter API error: {str(e)}")
                        cached_data = get_cached_data(current_topic)
                        
                    except Exception as e:
                        st.error(f"Unexpected error: {str(e)}")
                        cached_data = get_cached_data(current_topic)
                        
            except Exception as e:
                st.error(f"Failed to fetch tweets: {str(e)}")
                cached_data = get_cached_data(current_topic)
        
        # Display visualizations if we have data
        if cached_data:
            # Metrics row
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Tweets", len(cached_data['tweets']))
            with col2:
                positive = cached_data['analytics'][
                    cached_data['analytics']['metric'] == 'sentiment_Positive'
                ]['value'].iloc[0] if 'sentiment_Positive' in cached_data['analytics']['metric'].values else 0
                st.metric("Positive Sentiment", positive)
            with col3:
                avg_retweets = round(cached_data['tweets']['retweet_count'].mean(), 1)
                st.metric("Avg Retweets", avg_retweets)

            # Crisis Detection
            st.markdown("---")
            detect_crisis(cached_data, current_topic)
            
            # Sentiment and Volume Analysis
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                plot_sentiment(cached_data, current_topic)
            with col2:
                plot_tweet_volume(cached_data, current_topic)
            
            # Hashtags and Top Users
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                plot_top_hashtags(cached_data, current_topic)
            with col2:
                plot_engagement(cached_data, current_topic)
            
            # Engagement Analysis
            st.markdown("---")
            plot_top_users(cached_data, current_topic)
            
            # NLP Features
            st.markdown("---")
            st.subheader("Advanced NLP Analysis")
            plot_entities(cached_data, current_topic)
            
            # Word Cloud
            st.markdown("---")
            st.subheader(f'Word Cloud for "{topic}" Discussions')
            plot_wordcloud(cached_data, current_topic)

            # Geographical Analysis
            st.markdown("---")
            plot_geographical_heatmap(cached_data, current_topic)
            
    # Auto-refresh component (only for API mode)
    if data_source == "🐦 Live Twitter API":
        st_autorefresh(interval=refresh_interval * 60 * 1000, key="auto_refresh")

if __name__ == "__main__":
    main()
