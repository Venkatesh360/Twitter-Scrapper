# Twitter Data Scraper

This script fetches tweets from a specified Twitter account within a given date range, processes the tweet data, and saves it to a CSV file.

## Features

- Fetches tweets from a specified Twitter account.
- Processes tweet data including tweet ID, creation date, text, retweet count, likes, hashtags, user mentions, URLs, media, and conversation ID.
- Handles Twitter API rate limits gracefully.
- Saves the processed tweet data to a CSV file.

## Requirements

- Python 3.7+
- `twikit` library for interacting with the Twitter API.
- `asyncio` for asynchronous operations.
- `csv` for handling CSV file operations.
- `datetime` for handling date and time operations.
- `random` for generating random wait times.
- `traceback` for logging exceptions.

## Installation

1. Install the required Python libraries:

    ```sh
    pip install twikit
    ```

2. Ensure you have the `cookies.json` file containing your Twitter session cookies.

## Usage

1. Save the script to a file, e.g., `twitter_scraper.py`.

2. Run the script:

    ```sh
    python twitter_scraper.py
    ```

3. The script will create a `tweets.csv` file in the current directory and populate it with the fetched tweet data.

## Script Overview

```python
from twikit import Client, TooManyRequests
import asyncio
from datetime import datetime
import csv
from random import randint
import traceback

def processTweets(tweets, tweet_count):
    new_count = tweet_count
    tweet_data_list = []

    for tweet in tweets:
        tweet = vars(tweet)
        new_count += 1

        tweet_data = tweet.get('_data', {})
        core_data = tweet_data.get('core', {})
        user_results = core_data.get('user_results', {}).get('result', {})
        user_legacy = user_results.get('legacy', {})
        legacy_data = tweet_data.get('legacy', {})

        extracted_data = {
            'tweet_id': tweet_data.get('rest_id'),
            'created_at': legacy_data.get('created_at'),
            'full_text': legacy_data.get('full_text'),
            'conversation_id': legacy_data.get('conversation_id_str'),
            'user_id': user_results.get('rest_id'),
            'screen_name': user_legacy.get('screen_name'),
            'name': user_legacy.get('name'),
            'favorite_count': legacy_data.get('favorite_count'),
            'retweet_count': legacy_data.get('retweet_count'),
            'hashtags': [tag['text'] for tag in legacy_data.get('entities', {}).get('hashtags', [])],
            'user_mentions': [mention['screen_name'] for mention in legacy_data.get('entities', {}).get('user_mentions', [])],
            'urls': [url['expanded_url'] for url in legacy_data.get('entities', {}).get('urls', [])],
            'media': [media['media_url'] for media in legacy_data.get('entities', {}).get('media', [])],
        }

        tweet_data_list.append([
            new_count, extracted_data['user_id'], extracted_data['screen_name'], extracted_data['name'],
            extracted_data['tweet_id'], extracted_data['created_at'], extracted_data['full_text'], extracted_data['retweet_count'],
            extracted_data['favorite_count'], extracted_data['hashtags'], extracted_data['user_mentions'],
            extracted_data['urls'], extracted_data['media'], extracted_data['conversation_id']
        ])

    return new_count, tweet_data_list

async def main():
    try:
        with open("tweets.csv", "w", newline="", encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Index','User Id', 'Screen Name', 'Name', 'Tweet Id','Created At', 'Text', 'Retweet Count', 'Likes', 'Hashtags', 'User Mentions', 'Urls', 'Media', 'Conversation Id'])
        
        client = Client('en-US')
        client.load_cookies('cookies.json') 
        
        tweet_count = 0
        tweets = None
        QUERY = "(from:MamataOfficial) until:2019-12-31 since:2019-07-01"
        
        while tweet_count < 50:
            try:
                if tweets is None:
                    print(f"{datetime.now()} - getting Tweets...")
                    tweets = await client.search_tweet(QUERY, product='Latest')
                else:
                    print(f"{datetime.now()} - getting next batch of Tweets...")
                    tweets = await tweets.next()
            except TooManyRequests as e:
                rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
                wait_time = (rate_limit_reset - datetime.now()).total_seconds() + 20
                print(f"{datetime.now()} - Rate limit reached. Waiting for {wait_time} seconds.")
                await asyncio.sleep(wait_time)
                continue

            tweet_count, tweet_data_list = processTweets(tweets, tweet_count)
            
            with open('tweets.csv', 'a', newline="", encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerows(tweet_data_list)
            
            wait = randint(10, 20)
            print(f"Waiting for {wait} seconds before the next batch...")
            await asyncio.sleep(wait)

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    finally:
        print(f"{datetime.now()} finished execution total tweets. tweet count {tweet_count}")

if __name__ == "__main__":
    asyncio.run(main())