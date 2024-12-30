import requests
import yfinance as yf
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import pandas as pd
from pandas_datareader import data as pdr
import tweepy
import textblob 
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# dataframe format needed = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume']

import json
import asyncio
from typing import List, Dict, Union
from httpx import AsyncClient, Response
from parsel import Selector
from loguru import logger as log


def binanceapi(starting_date,ending_date):
    apikey = 'RyF5RlacKklZQfOdldqWknRJtwC6ONTcr9HcuY7NcODzTsVZzh5fQ90W9Y0DawjT'
    secret = 'OIwkmnrWPwBNTL4crujEQdtVjmhPH1YBQiRp1zb0A9uClPS2xIHCTEzGOIiKaSWk'
    client = Client(apikey, secret)
    cryptos = {"BTCUSDT", "ETHUSDT"}
    cryptos_data = []
    for i in cryptos:
        historical = client.get_historical_klines(i, Client.KLINE_INTERVAL_1DAY, starting_date, ending_date)
        hist_df = pd.DataFrame(historical)
        hist_df.head()
        hist_df.columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 
                        'Number of Trades', 'TB Base Volume', 'TB Quote Volume', 'Ignore']
        hist_df['Open Time'] = pd.to_datetime(hist_df['Open Time']/1000, unit='s')
        hist_df['Close Time'] = pd.to_datetime(hist_df['Close Time']/1000, unit='s')
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote Asset Volume', 'TB Base Volume', 'TB Quote Volume']
        hist_df[numeric_columns] = hist_df[numeric_columns].apply(pd.to_numeric, axis=1)
        hist_df.describe()
        cryptos_data.append(hist_df)
        print(cryptos_data)

        df = hist_df.to_parquet(f'./{i}.parquet.gzip',compression='gzip')  
    return cryptos_data
    ### Finished scrapping btc data 
# binanceapi()



def marketdata(starting_date,ending_date):
    
    # polygon_apikey = "3JKpRrsQjcdrfT8yLcFUu33Pzx4OsxLq"
    # headers = {"Authorization": "Bearer 3JKpRrsQjcdrfT8yLcFUu33Pzx4OsxLq"}
    # resp = requests.get("https://api.polygon.io/v2/aggs/ticker/SPX/range/1/day/2024-11-03/2024-11-04?adjusted=true&sort=asc&apiKey=3JKpRrsQjcdrfT8yLcFUu33Pzx4OsxLq")
    # print(resp.json())
    # POlygon api charges for gspc index 
    commodity ={"Gold": "^YH10150040", "Crude Oil": "CL=F" , "SP500": "^GSPC"}
    yfcodetocommodity ={ "^YH10150040": "Gold", "CL=F": "Crude Oil", "^GSPC": "SP500"}
    prices = {"^GSPC", "^YH10150040", "CL=F"} #SP500, Gold, crude oil 
    
    # for yfinance minute level data is only available for last 60 days 
    market_data = []
    for i in prices:
        data = yf.Ticker(i) #Gold price
        df = data.history(period="max", interval="1d", start=starting_date, end=ending_date)
        market_data.append(df)
        print(market_data)
        parquet = df.to_parquet(f'./{yfcodetocommodity.get(i)}.parquet.gzip',compression='gzip')  
    return market_data
        
def data_scrap():
    starting_date = "2022-01-01"
    ending_date = "2024-12-30"
    crypto_pricedata = binanceapi(starting_date,ending_date)
    tradition_market_data = marketdata(starting_date,ending_date)
    asyncio.run(reddit_scrap_run("Bitcoin", "hot", 4, "subreddit.json"))
    # tweets = tweets_scrap() Both Deprecated
    # news = news_scrap() Both Deprecated
    print("COMPLETED")
    
def initialize_client() -> AsyncClient:
    """Initialize and return the AsyncClient."""
    return AsyncClient(
        http2=True,
        headers={
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Cookie": "intl_splash=false"
        },
        follow_redirects=True
    )

def parse_subreddit(response: Response) -> Dict:
    """Parse article data from HTML."""
    selector = Selector(response.text)
    url = str(response.url)
    info = {
        "id": url.split("/r")[-1].replace("/", ""),
        "description": selector.xpath("//shreddit-subreddit-header/@description").get(),
        "members": int(selector.xpath("//shreddit-subreddit-header/@subscribers").get() or 0),
        "rank": int(selector.xpath("//strong[@id='position']/*/@number").get() or 0),
        "bookmarks": {
            item.xpath(".//a/span/span/span/text()").get(): item.xpath(".//a/@href").get()
            for item in selector.xpath("//div[faceplate-tracker[@source='community_menu']]/faceplate-tracker")
        },
        "url": url,
    }

    post_data = [
        {
            "authorProfile": f"https://www.reddit.com/user/{box.xpath('.//shreddit-post/@author').get()}" if box.xpath(".//shreddit-post/@author").get() else None,
            "authorId": box.xpath(".//shreddit-post/@author-id").get(),
            "title": box.xpath("./@aria-label").get(),
            "link": f"https://www.reddit.com{box.xpath('.//a/@href').get()}" if box.xpath(".//a/@href").get() else None,
            "publishingDate": box.xpath(".//shreddit-post/@created-timestamp").get(),
            "postId": box.xpath(".//shreddit-post/@id").get(),
            "postLabel": box.xpath(".//faceplate-tracker[@source='post']/a/span/div/text()").get().strip() if box.xpath(".//faceplate-tracker[@source='post']/a/span/div/text()").get() else None,
            "postUpvotes": int(box.xpath(".//shreddit-post/@score").get() or 0),
            "commentCount": int(box.xpath(".//shreddit-post/@comment-count").get() or 0),
            "attachmentType": box.xpath(".//shreddit-post/@post-type").get(),
            "attachmentLink": box.xpath(".//div[@slot='thumbnail']/*/*/@src").get() if box.xpath(".//shreddit-post/@post-type").get() == "image" else (
                box.xpath(".//shreddit-player/@preview").get() if box.xpath(".//shreddit-post/@post-type").get() == "video" else box.xpath(".//div[@slot='thumbnail']/a/@href").get())
        }
        for box in selector.xpath("//article")
    ]

    return {"post_data": post_data, "info": info, "cursor": selector.xpath("//shreddit-post/@more-posts-cursor").get()}

async def scrape_subreddit(subreddit_id: str, sort: str = "hot", max_pages: int = None) -> Dict:
    """Scrape articles on a subreddit."""
    client = initialize_client()
    base_url = f"https://www.reddit.com/r/{subreddit_id}/"
    response = await client.get(base_url)
    subreddit_data = parse_subreddit(response)
    cursor = subreddit_data["cursor"]

    def make_pagination_url(cursor_id: str) -> str:
        return f"https://www.reddit.com/svc/shreddit/community-more-posts/hot/?after={cursor_id}%3D%3D&t=DAY&name=wallstreetbets&feedLength=3&sort={sort}"

    while cursor and (max_pages is None or max_pages > 0):
        url = make_pagination_url(cursor)
        response = await client.get(url)
        data = parse_subreddit(response)
        cursor = data["cursor"]
        subreddit_data["post_data"].extend(data["post_data"])
        if max_pages is not None:
            max_pages -= 1

    log.success(f"Scraped {len(subreddit_data['post_data'])} posts from the subreddit: r/{subreddit_id}")
    return subreddit_data

async def reddit_scrap_run(subreddit_id: str, sort: str = "hot", max_pages: int = 4, output_file: str = "subreddit.json"):
    """Run the scraping process and save the data."""
    data = await scrape_subreddit(subreddit_id, sort, max_pages)
    with open(output_file, "w", encoding="utf-8-sig") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)



    
def tweets_scrap():
    
    # Replace these values with your own Twitter API credentials
    API_KEY = 'h91Gce9aYDrCXNpLAXsaQMgbR'
    API_SECRET = '6RiXIPuziMZpl61JKstlRRcV7CQLUovr2lcgfHhgnSCF9xUBX2'
    ACCESS_TOKEN = '1861235574105124864-wkpjNH6vnfbTvKZqJdwO0KPRLJSpfg'
    ACCESS_TOKEN_SECRET = 'UqEWQm9G5t5hheiLkjrjilDhuHA5V6RURHzWMZw5Dj2QN'

    # Authenticate with Twitter API
    auth = tweepy.OAuth1UserHandler(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    # Define the search query and parameters
    query = 'bitcoin OR btc -filter:retweets'
    max_tweets = 100  # Maximum number of tweets to retrieve

    # Fetch tweets
    tweets = tweepy.Cursor(api.search_tweets, q=query, lang='en', tweet_mode='extended').items(max_tweets)
    tweets_df = []

    for tweet in tweets:
        tweets_df = tweets_df.concat({
        'Username': tweet.user.screen_name,
        'Name': tweet.user.name,
        'Tweet': tweet.full_text,
        'Created At': tweet.created_at,
        'Favorites': tweet.favorite_count,
        'Retweets': tweet.retweet_count,
        'Tweet URL': f"https://twitter.com/{tweet.user.screen_name}/status/{tweet.id}"
    }, ignore_index=True)
    return tweets_df

def news_scrap(): # Using News API 
    API_KEY = '27e85539b251405db09badde5472dee2'
    query = 'bitcoin'
    url = ('https://newsapi.org/v2/everything?'
        f'q={query}&'
        'sortBy=publishedAt&'
        f'apiKey={API_KEY}')

    response = requests.get(url)
    data = response.json()
    articles_df = pd.DataFrame(columns=['Title', 'Source', 'Published At', 'Description', 'URL', 'Sentiment Score'])
    
    if data['status'] == 'ok':
        articles = data['articles']
        for article in articles:
            articles_df = pd.concat([pd.DataFrame([{
                'Title': article['title'],
                'Source': article['source']['name'],
                'Published At': article['publishedAt'],
                'Description': article['description'],
                'URL': article['url']
            }]), articles_df],ignore_index=True)
        df_to_parquet(articles_df)
        print('Fetched', len(articles_df), 'articles')
        return articles_df
    else:
        print('Error fetching data:', data['message'])
        return None    

# Sentiments Score Function 
# Textblob deprecated but Vader will be used 
def textblob_sentimentscore(df):
    for data in df:
        df['Sentiment Score'] = df['Description'].apply(lambda x: textblob.TextBlob(x).sentiment.polarity)
    print(f"Textblob Overall Sentiment score for {data} has been calculated: {df['Sentiment Score'].mean()}")
    return None

def vader_sentimentscore(df):
    sentiment_analyzer = SentimentIntensityAnalyzer()
    df.dropna(subset=['Description'], inplace=True)
    for data in df:
        df['Sentiment Score'] = df['Description'].apply(lambda x: sentiment_analyzer.polarity_scores(x)['compound'])
    print(f"Vader Overall Sentiment score for {data} has been calculated: {df['Sentiment Score'].mean()}")

    return None

def df_to_parquet(df):
    parquet = df.to_parquet(f'./news.parquet.gzip',compression='gzip')  

def parquet_to_df(file_path):
    df = pd.read_parquet(file_path, engine='pyarrow')
    return df
data_scrap()

# news = news_scrap()

# news.to_csv('news.csv')
# vader_sentimentscore(news)
# textblob_sentimentscore(news)
