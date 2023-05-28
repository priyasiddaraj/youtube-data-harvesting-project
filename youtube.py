import pandas as pd
from googleapiclient.discovery import build
from pymongo import MongoClient
import streamlit as st


def mongo_connect():
    client = MongoClient('localhost', 27017)
    db = client.youtube
    col = db.Channels
    return col


def API_connect():
    API_key = 'AIzaSyCPbBuCVwWYq6aXGrfVQSKvdlkfMixfHCM'
    api_service_name = 'youtube'
    api_version = 'v3'
    youtube = build('youtube', 'v3', developerKey=API_key)
    return youtube


def Channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet ,content Details,statistics",
        id=channel_id
    )
    response = request.execute()
    print(response)
    # =========senthil========
    all_data = {}
    for item in response["items"]:
        data = {'channel_id': item["id"],
                'channelName': item["snippet"]["title"],
                'subcription': item["statistics"]["subscriberCount"],
                'views': item["statistics"]["viewCount"],
                'total_videos': item["statistics"]["videoCount"],
                'playlist_id': item["contentDetails"]["relatedPlaylists"]["uploads"],
                'description': item["snippet"]["description"],
                'publishedAt': item["snippet"]["publishedAt"],
                'viewCount': item["statistics"]["viewCount"],
                }
        # =========senthil========
        all_data.update(data)
        return all_data


def get_video_ids(youtube, playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet ,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )

    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
    return video_ids


def get_video_details(youtube, video_ids):
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet ,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                             }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)
    return all_video_info


def get_comments_in_video(youtube, video_ids):
    all_comments = []
    for i in range(len(video_ids)):
        request = youtube.commentThreads().list(
            part="snippet ,replies",
            videoId=video_ids[i],
            maxResults=100
        )
        try:
            response = request.execute()
            c = 0
            for comment in response['items']:
                video_id = comment['snippet']['videoId']
                commentss = comment['snippet']['toplevelComment']['snippet']['textOriginal']
                comment_author = comment['snippet']['toplevelComment']['snippet']['authorDisplayName']
                comment_like_count = comment['snippet']['toplevelComment']['snippet']['likeCount']
                comment_Id = comment['snippet']['toplevelComment']['id']
                comment_publishedAt = comment['snippet']['toplevelComment']['snippet']['publishedAt']

                try:
                    comment_replies = comment['replies']['comments']
                    Relpy_dict = {}
                    for j in range(len(comment_replies)):
                        Comment_reply_id = comment['replies']['comments']['j']['id']
                        Comment_replay_author = comment['replies']['comments']['j']['snippet']['authorDisplayName']
                        Comment_reply_text = comment['replies']['comments']['j']['snippet']['textOriginal']
                        Comment_reply_like_count = comment['replies']['comments']['j']['snippet']['likeCount']
                        Comment_reply_publishedAt = comment['replies']['comments']['j']['snippet']['publishedAt']
                        Relpy_dict.update({'comment_Id': Comment_reply_id, 'comment': Comment_reply_text,
                                           'comment_author': Comment_replay_author, 'comment_like_count':
                                               Comment_reply_like_count,
                                           'comment_publishedAt': Comment_reply_publishedAt})
                except:
                    Relpy_dict = None
                all_comments.append({'video_id': video_id, 'comment': commentss, 'comment_author': comment_author,
                                     'comment_like_count': comment_like_count, 'comment_Id': comment_Id,
                                     'Comment_publishedAt': comment_publishedAt, 'Relpy_dict': Relpy_dict})

        except:
            pass

    return all_comments


def playlists1(play):
    x = get_video_ids(API_connect(), play)
    playlist_videos = []
    for i in range(len(x)):
        playlist_videos.append({'video_id': x[i], 'playlist_id': play})
    return playlist_videos


def main(channl_id):
    youtube = API_connect()
    CHNL_ID = channl_id
    CD = Channel_details(youtube, CHNL_ID)

    video_ids = get_video_ids(youtube, CD['playlist_id'])
    playlist = playlists1(CD['playlist_id'])

    video_df = pd.DataFrame(get_video_details(youtube, video_ids))

    Cmt_details = get_comments_in_video(youtube, video_ids)

    Data = {"Channel_Details": CD,
            "Playlist_Details": playlist,
            "Video_Details": video_df.to_dict('records'),
            "Comments": Cmt_details}
    return Data
