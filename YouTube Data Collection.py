from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm
import pandas as pd
import json
from itertools import cycle
import os
import isodate
import traceback
from datetime import datetime, timedelta
#from translate import translate_en

# Load API keys from file
with open('keys_related/valid_api_keys.txt', 'r') as f:
    api_keys = [line.strip() for line in f]

# Store expired keys here
expired_keys = []

# To keep track of channel_ids. Prevents duplicate.
channel_ids = []

# Build a service object for interacting with the API.
def get_authenticated_service(api_key):
    return build('youtube', 'v3', developerKey=api_key)

# Function to fetch comments from a video with additional details
def get_video_comments(service, video_id, channel_id, comment_attrs, comment_limit=None):
    comments = []

    try:
        results = service.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            textFormat="plainText",
        ).execute()

        while results and (comment_limit is None or len(comments) < comment_limit):
            for item in results["items"]:
                comment = item["snippet"]["topLevelComment"]
                comment_dict = {}

                if comment_attrs.get('comment_id', False):
                    comment_dict['comment_id'] = comment['id']
                if comment_attrs.get('commenter_name', False):
                    comment_dict['commenter_name'] = comment['snippet']['authorDisplayName']
                if comment_attrs.get('commenter_id', False):
                    if 'authorChannelId' in comment['snippet']:
                        comment_dict['commenter_id'] = comment['snippet']['authorChannelId']['value']
                    else:
                        comment_dict['commenter_id'] = None
                if comment_attrs.get('comment_display', False):
                    comment_dict['comment_display'] = comment['snippet']['textDisplay']
                if comment_attrs.get('comment_original', False):
                    comment_dict['comment_original'] = comment['snippet']['textOriginal']
                if comment_attrs.get('comment_likes', False):
                    comment_dict['comment_likes'] = comment['snippet']['likeCount']
                if comment_attrs.get('comment_total_replies', False):
                    comment_dict['comment_total_replies'] = item['snippet']['totalReplyCount']
                if comment_attrs.get('comment_published_date', False):
                    comment_dict['comment_published_date'] = comment['snippet']['publishedAt']
                if comment_attrs.get('comment_update_date', False):
                    comment_dict['comment_update_date'] = comment['snippet']['updatedAt']
                if comment_attrs.get('comment_extracted_date', False):
                    comment_dict['comment_extracted_date'] = datetime.now().isoformat()

                # Add channel_id to the comment dict
                comment_dict['channel_id'] = channel_id
                comments.append(comment_dict)

                # Break if comment limit is reached
                if comment_limit is not None and len(comments) >= comment_limit:
                    break

            # Check if there are more comments and fetch them
            if "nextPageToken" in results:
                results = service.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    pageToken=results["nextPageToken"],
                    textFormat="plainText",
                ).execute()
            else:
                break

    except HttpError as e:
        error_info = json.loads(e.content.decode())
        if error_info.get('error', {}).get('errors', [{}])[0].get('reason') == 'commentsDisabled':
            return None
        else:
            raise e  # Raise the error if it's due to any other reason

    return comments[:comment_limit]

# Function to fetch video details

def get_video_details(service, video_id, fetch_attrs, comment_limit, start_date, end_date):
    video_details = {}

    # Fetch video categories
    video_categories = {}
    categories = service.videoCategories().list(
        part = 'snippet',
        regionCode = 'US'
    ).execute()

    for item in categories['items']:
        video_categories[item['id']] = item['snippet']['title']

    # Fetch other video details if needed
    if any(fetch_attrs.values()):
        response = service.videos().list(
            part='snippet,contentDetails,statistics,topicDetails',
            id=video_id
        ).execute()

        # Check if any video details are available
        if 'items' in response:

            try:
                item = response['items'][0]
            except:
                print(video_id + ' is not available')
                return None
            if end_date is None:
                end_date = datetime.now().isoformat() + 'Z'
            else:
                end_date += ' 23:59:59'

            start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S') if isinstance(start_date, str) else start_date


            end_date = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S.%fZ') if isinstance(end_date, str) else end_date

            published_date = item['snippet']['publishedAt']
            published_date = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%SZ")

            if start_date and published_date < start_date:
                print('published_date:' , published_date, 'start_date:' , start_date)
                return None

            if end_date and published_date > end_date:
                print('published_date:' , published_date, 'end_date:' , end_date)
                return None

            if fetch_attrs.get('title', False):
                video_details['title'] = item['snippet']['title']

            if fetch_attrs.get('description', False):
                video_details['description'] = item['snippet']['description']

            if fetch_attrs.get('published_date', False):
                video_details['published_date'] = item['snippet']['publishedAt']

            if fetch_attrs.get('channel_id', False):
                video_details['channel_id'] = item['snippet']['channelId']

            if fetch_attrs.get('category', False):
                # Convert category id to category name
                category_id = item['snippet']['categoryId']
                video_details['category'] = video_categories.get(category_id, 'Unknown')

            if fetch_attrs.get('duration', False):
                video_details['duration'] = isodate.parse_duration(item['contentDetails']['duration']).total_seconds()

            if fetch_attrs.get('total_views', False):
                video_details['total_views'] = item['statistics'].get('viewCount', '0')

            if fetch_attrs.get('total_likes', False):
                video_details['total_likes'] = item['statistics'].get('likeCount', '0')

            if fetch_attrs.get('total_dislikes', False):
                video_details['total_dislikes'] = item['statistics'].get('dislikeCount', '0')

            if fetch_attrs.get('total_comments', False):
                video_details['total_comments'] = item['statistics'].get('commentCount', '0')

            if fetch_attrs.get('video_extracted_date', False):
                video_details['video_extracted_date'] = datetime.now().isoformat()

            if fetch_attrs.get('thumbnail', False):
                video_details['thumbnail'] = item['snippet']['thumbnails']['high']['url']

            if fetch_attrs.get('topic_categories', False):
                topic_categories = item.get('topicDetails', {}).get('topicCategories', [])
                video_details['topic_categories'] = ', '.join(topic_categories)

    # Fetch comments if needed
    if fetch_attrs.get('comments', False) and video_details:
        video_details['comments'] = get_video_comments(service, video_id, comment_limit, start_date, end_date)

    return video_details

# Function to fetch channel details
def get_channel_details(service, channel_id):
    try:
        response = service.channels().list(
            part="snippet,statistics,topicDetails",
            id=channel_id
        ).execute()

        if 'items' in response:
            channel = response['items'][0]
            channel_details = {}

            if channel_attrs.get('channel_id', False):
                channel_details['channel_id'] = channel_id
            if channel_attrs.get('channel_title', False):
                channel_details['channel_title'] = channel['snippet']['title']
            if channel_attrs.get('description', False):
                channel_details['description'] = channel['snippet']['description']
            if channel_attrs.get('joined_date', False):
                channel_details['joined_date'] = channel['snippet']['publishedAt']
            if channel_attrs.get('location', False):
                channel_details['location'] = channel['snippet'].get('country', 'Unknown')
            if channel_attrs.get('total_subscribers', False):
                channel_details['total_subscribers'] = channel['statistics']['subscriberCount']
            #if channel_attrs.get('total_views', False):
                #channel_details['total_views'] = channel['statistics']['viewCount']
            if channel_attrs.get('total_videos', False):
                channel_details['total_videos'] = channel['statistics']['videoCount']
            if channel_attrs.get('extracted_date', False):
                channel_details['extracted_date'] = datetime.now().isoformat()
            if channel_attrs.get('thumbnail', False):
                channel_details['thumbnail'] = channel['snippet']['thumbnails']['high']['url']
            if channel_attrs.get('language', False):
                channel_details['language'] = channel.get('snippet', {}).get('defaultLanguage', 'Unknown')
            if channel_attrs.get('channel_extracted_date', False):
                channel_details['channel_extracted_date'] = datetime.now().isoformat()
            if channel_details.get('topic_categories', False):
                topic_categories = channel.get('topicDetails', {}).get('topicCategories', [])
                channel_details['topic_categories'] = ', '.join(topic_categories)


            return channel_details
        else:
            return None

    except HttpError as e:
        print(f"An HTTP error occurred while fetching channel details for channel {channel_id}.")
        print(f"Error details: {e.content.decode()}")
        return None



# Function to handle video and comment details fetching for each video id
def get_details_from_video_ids(video_id, video_attrs, comment_attrs, additional_attrs,  comment_limit, start_time=None, end_time=None):
    video_details = None
    comments_details = None
    channel_details = None

    for api_key in api_keys:
        # Skip if the key is expired
        if api_key in expired_keys:
            continue
        try:
            service = get_authenticated_service(api_key)
            video_details = get_video_details(service, video_id, video_attrs, comment_limit, start_time, end_time)

            if video_details is None:
                break

            channel_id = video_details['channel_id']
            if channel_id not in channel_ids:
                channel_details = get_channel_details(service, channel_id)
                channel_ids.append(channel_id)

            # Fetch comments details if not ignored
            if not ignore_comments:
                channel_id = video_details.get('channel_id')
                comments_details = get_video_comments(service, video_id, channel_id, comment_attrs, comment_limit)

            # Include additional attributes in the returned data
            video_details.update(additional_attrs)

            break

        except HttpError as e:
            print(f"An HTTP error {e.resp.status} occurred with key {api_key}.")
            error_content = e.content.decode()
            print(f"Error details: {error_content}")

            # Check if the error is quota related
            if "quota" in error_content.lower():
                expired_keys.append(api_key)
            else:
                # If it's not quota related, break out of the loop
                break

    return video_details, comments_details, channel_details


# Function to fetch video ids from a channel
def get_video_ids_from_channel(service, channel_id, start_date=None, end_date=None):
    video_ids = []

    if start_date is None:
        start_date = '2005-04-23T00:00:00Z'  # YouTube's start date
    else:
        start_date += 'T00:00:00Z'

    if end_date is None:
        end_date = datetime.now().isoformat() + 'Z'
    else:
        end_date += 'T23:59:59.999Z'

    try:
        # Use the search() function to get videos from the channel
        results = service.search().list(
            channelId=channel_id,
            part="id,snippet",
            order="date",
            publishedAfter=start_date,
            publishedBefore=end_date,
            maxResults=50  # Maximum allowed results per API call
        ).execute()

        # Loop over the results and add video ids to the list
        while results:
            for item in results["items"]:
                if item['id']['kind'] == "youtube#video":
                    video_ids.append(item['id']['videoId'])

            # Check if there are more videos and fetch them
            if "nextPageToken" in results:
                results = service.search().list(
                    channelId=channel_id,
                    part="id,snippet",
                    order="date",
                    publishedAfter=start_date,
                    publishedBefore=end_date,
                    maxResults=50,
                    pageToken=results["nextPageToken"]
                ).execute()
            else:
                break

    except HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred while fetching videos from channel {channel_id}.")
        print(f"Error details: {e.content.decode()}")

    return video_ids

def is_in_date_range(service, video_id, start_date, end_date):
    response = service.videos().list(
        part='snippet',
        id=video_id
    ).execute()
    # Check if any video details are available
    if 'items' in response:
        # Video is removed or not available.
        if not response['items']:
            return False

        item = response['items'][0]
        published_date = item['snippet']['publishedAt']
        published_date = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%SZ")

        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        if start_date and published_date < start_date:
            return False
        if end_date and published_date > end_date:
            return False

    return True


# Translate attributes
"""
def translate(file_name):
    if translate_attrs['translate']:
        if translate_attrs['title']:
            translate_filename = 'output_data/' + file_name + '_video'
            translate_en(translate_filename, doc="title", attribute='title')

        if translate_attrs['description']:
            df_desc = translate_en(translate_filename + '_translated', doc="desc", attribute='description')

            # Empty descriptions
            df_desc["description"] = df_desc["description"].apply(lambda x: "" if x == "into" else x)
            df_desc.to_csv(translate_filename + '_translated_title_desc.csv', index = False)

        if translate_attrs['comment']:
            translate_filename = 'output_data/' + file_name + '_comment_combined'
            translate_en(translate_filename, doc="title", attribute='comment_display')
"""


def main(file_name, input_folder, video_attrs, comment_attrs, translate_attrs, keep_old_attr=False, comment_limit=None, ignore_comments=False, read_channel=False, start_date=None, end_date=None):
    try:
        # Try to read the CSV file
        if not os.path.exists(input_folder):
            os.makedirs(input_folder)

        df = pd.read_csv(input_folder + file_name + ".csv")
    except Exception as e:
        print(f"Could not read CSV file: {e}")
        print("Attempting to read Excel file...")

        try:
            # Try to read the Excel file
            df = pd.read_excel(input_folder + file_name + ".xlsx")
        except Exception as e:
            print(f"Could not read Excel file: {e}")
            return  # If we couldn't read either file, end the execution here

    api_cycle = cycle(api_keys)  # Cycle through the API keys
    if read_channel:
        # Read channel ids and fetch video ids
        channel_ids = df['channel_id'].tolist()
        video_ids = []
        for channel_id in channel_ids:
            api_key = next(api_cycle)  # Get the next API key
            while api_key in expired_keys:
                api_key = next(api_cycle)  # Get the next API key if the current one is expired
            service = get_authenticated_service(api_key)
            video_ids.extend(get_video_ids_from_channel(service, channel_id, start_date, end_date))
    else:
        raw_video_ids = df['video_id'].tolist()
        video_ids = []
        additional_attrs = df.drop(columns=['video_id']) if keep_old_attr else None

        if start_date == None and end_date == None:
            video_ids = raw_video_ids
        else:
            for video_id in tqdm(raw_video_ids, total=len(raw_video_ids), desc="Checking range..."):
                api_key = next(api_cycle)  # Get the next API key
                while api_key in expired_keys:
                    api_key = next(api_cycle)  # Get the next API key if the current one is expired
                service = get_authenticated_service(api_key)
                if is_in_date_range(service, video_id, start_date, end_date):
                    video_ids.append(video_id)

    pbar = tqdm(total=len(video_ids), desc="Processing video ids...")

    video_df = pd.DataFrame()
    comments_df = pd.DataFrame()
    channel_df = pd.DataFrame()

    with ThreadPoolExecutor() as executor:
        if keep_old_attr:
            future_to_video_id = {
                executor.submit(get_details_from_video_ids, video_id, video_attrs, comment_attrs, additional_attrs.iloc[idx].to_dict(), comment_limit, start_date, end_date): idx
                for idx, video_id in enumerate(video_ids)
            }
        else:
            future_to_video_id = {
                executor.submit(get_details_from_video_ids, video_id, video_attrs, comment_attrs, {}, comment_limit, start_date, end_date): idx
                for idx, video_id in enumerate(video_ids)
            }

        for future in as_completed(future_to_video_id):
            idx = future_to_video_id[future]
            try:
                video_data, comments_data, channel_data = future.result()

                # Update video data
                if video_data:
                    video_data['video_id'] = video_ids[idx]  # Add video id to video_data
                    video_df = pd.concat([video_df, pd.DataFrame(video_data, index=[0])], ignore_index=True)
                # Update comments data
                if comments_data and not ignore_comments:
                    for comment_data in comments_data:
                        comment_data['video_id'] = video_ids[idx]  # Add video id to each comment dict
                    comments_df = pd.concat([comments_df, pd.DataFrame(comments_data)], ignore_index=True)

                if channel_data:
                    channel_df = pd.concat([channel_df, pd.DataFrame(channel_data, index=[0])], ignore_index=True)

            except Exception as exc:
                tb_str = traceback.format_exception(etype=type(exc), value=exc, tb=exc.__traceback__)
                print('%r generated an exception: %s' % (video_ids[idx], "".join(tb_str)))
            pbar.update()

    # Save the video data to a CSV file
    output_folder = "output_data/"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    video_df.to_csv(output_folder + file_name+'_video.csv', index=False)

    # Drop duplicates if there is
    channel_df = channel_df.drop_duplicates()
    channel_df.to_csv(output_folder + file_name + '_channels.csv', index=False)

    if not ignore_comments:
        comments_df.to_csv(output_folder + file_name+'_comment.csv', index=False)

        # Combine comments
        df_combined_comments = comments_df.groupby('video_id')['comment_display'].agg(' '.join).reset_index()
        df_combined_comments.to_csv("output_data/" + file_name + '_comment_combined.csv', index=False)

    # Translation
    #translate(file_name)




if __name__ == '__main__':

    # Video Attributes to collect
    video_attrs = {
        'title': True,
        'description': True,
        'category': True,
        'duration': True,
        'published_date': True,
        'channel_id': True,
        'total_views': True,
        'total_likes': True,
        'total_dislikes': True,
        'total_comments': True,
        'video_extracted_date': True,
        'thumbnail': True,
        'topic_categories': True
    }

    # Comment Attributes to collect
    comment_attrs = {
        'comment_id': True,
        'commenter_name': True,
        'commenter_id': True,
        'comment_display': True,
        'comment_original': True,
        'comment_likes': True,
        'comment_total_replies': True,
        'comment_published_date': True,
        'comment_update_date': True,
        'comment_extracted_date':True
    }

    # Channel Attributes to collect
    channel_attrs = {
        'channel_id': True,
        'channel_title': True,
        'description': True,
        'joined_date': True,
        'location': True,
        'total_views': True,
        'total_subscribers': True,
        'total_videos': True,
        'channel_extracted_date': True,
        'thumbnail': True,
        'language': True,
        'topic_categories': True
    }

    # Translate Text
    translate_attrs = {
        'translate': False,
        'title' : False,
        'description' : False,
        'comment': False
    }



    # File name that contains video_ids or channel_ids

    # Read channel_ids (True) or read video_ids (False)
    read_channel = False

    # Skip comment related information (True). Collect (False)
    ignore_comments = True
    # Set the comment limit here, None for no limitation
    comment_limit = 1

    # Keep the existing attributes and add the new file.
    keep_old_attr = False

    # Specify your start_date and end_date here
    start_date = None #'2000-01-01'
    end_date =  None #'2023-12-31'

    # file_name = "metadata"
    input_folder = 'input_data/'
    file = 'channel_video_ids'

    """
    for file_name in os.listdir(input_folder):
        file = os.path.splitext(file_name)[0]
        print("Collecting data for", file )
    """

    main(file_name = file, input_folder= input_folder, video_attrs = video_attrs, comment_attrs = comment_attrs, translate_attrs = translate_attrs, comment_limit = comment_limit,
             ignore_comments = ignore_comments, read_channel = read_channel, start_date = start_date, end_date = end_date, keep_old_attr = keep_old_attr)

