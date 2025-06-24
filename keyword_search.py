import datetime
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Function to initialize YouTube API client
def initialize_youtube_api(api_key):
    return build('youtube', 'v3', developerKey=api_key, cache_discovery=False)

# Function to search YouTube and return video IDs for a given keyword
def youtube_search(keyword, api_key, start_date, end_date):
    youtube = initialize_youtube_api(api_key)
    video_ids = []

    try:
        # Starting the search from the beginning
        next_page_token = None

        while True:
            request = youtube.search().list(
                q=keyword,
                part="id",
                maxResults=50,
                order="relevance",  # Sorting by relevance
                type="video",
                publishedAfter=start_date,
                publishedBefore=end_date,
                pageToken=next_page_token
            )

            response = request.execute()

            # Extracting video IDs from the search results
            for item in response.get('items', []):
                video_ids.append(item['id']['videoId'])

            next_page_token = response.get('nextPageToken')

            if not next_page_token:
                break

    except HttpError as e:
        print(f"An HTTP error occurred: {e.resp.status} {e.content}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return keyword, video_ids


# Reading API keys from a file
def read_api_keys(file_path):
    with open(file_path, 'r') as file:
        return [key.strip() for key in file.readlines()]

# Function to perform parallel searches
def parallel_youtube_search(api_keys, keywords, start_date, end_date):
    search_results = []
    with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
        # Create a future for each keyword with different API keys
        futures = {
            executor.submit(youtube_search, keyword, api_key, start_date, end_date): keyword
            for keyword, api_key in zip(keywords, api_keys)
        }

        # Collecting results as they complete
        for future in tqdm(as_completed(futures), total=len(futures), desc="Collecting video IDs"):
            try:
                keyword, video_ids = future.result()
                for video_id in video_ids:
                    search_results.append({'keyword': keyword, 'video_id': video_id})
            except Exception as e:
                print(f"Error collecting results: {e}")
    return search_results

def main():
    api_keys_file_path = 'keys_related/valid_api_keys.txt'
    api_keys = read_api_keys(api_keys_file_path)

    keywords = ["South China Sea dispute conflict Philippines China",
                "China nine-dash line South China Sea dispute",
                ]
    start_date = "2010-01-01T00:00:00Z"
    end_date = "2024-07-01T00:00:00Z"

    results = parallel_youtube_search(api_keys, keywords, start_date, end_date)

    # Save results to a single CSV file
    df = pd.DataFrame(results)

    len1 = len(df)

    df.drop_duplicates(subset=['video_id'], keep=False,inplace=True)

    len2 = len(df)

    print("Dropped:" + str(len1-len2))


    df.to_csv("input_data/New_SCS_YT_regular_video.csv", index=False)

if __name__ == "__main__":
    main()
