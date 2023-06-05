#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install pymysql


# In[2]:


pip install mysql-connector-python


# In[108]:


import googleapiclient.discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import json
import re
import ssl
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import numpy as np
import pymongo

# [Dash board libraries]
import streamlit as st
import plotly.express as px


# In[4]:


api_key = 'AIzaSyAtQO2qxtNTDWcWnvwZz9enkmnbnBw8OMI'

channel_ids = ['UCs4RzXHKYBXHqYne2MWCDBA', # Harsh Rajput
              'UCnz-ZXXER4jOvuED5trXfEA', # Tech tfq
              'UCCezIgC97PvUuR4_gbFUs5g', # Corey schafer
              'UCpZxF2O5dgn0s-du2AnYGOA', # Excel tutorial
              'UC7eHZXheF8nVOfwB2PEslMw', # Aashish chanchlani vines
            'UCGdPm5Aq081vVD7ih9jZf6Q',  # Facttechz
            'UCkcqmxjMXaBb0kDWqqSEU1g', # Power couple
            'UCKjZzaNre54cT4jPf45PCNQ',  # James cross
            'UCjLcSaJa8ZWdz3ihXEXCTZA',  # Still i rise
            'UC9vLdyHatOhRrCwQXq3HmQQ'  # Motivation ark
              ]
youtube = build('youtube', 'v3', developerKey=api_key)

        


# In[5]:


def get_channel_stats(youtube, channel_ids):
    channel_details = []
    
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=','.join(channel_ids)
    )
    response = request.execute()
    
    for item in response['items']:
        channel_data = {
            'channel_id': item['id'],
            'channel_name': item['snippet']['title'],
            'subscribers': item['statistics']['subscriberCount'],
            'views': item['statistics']['viewCount'],
            'Total_videos': item['statistics']['videoCount'],
            'channel_description':item['snippet']['description'],
            'playlist_id': item['contentDetails']['relatedPlaylists']['uploads']
        }
        channel_details.append(channel_data)
    
    return channel_details


# In[6]:


channel_statistics = get_channel_stats(youtube, channel_ids)


# In[7]:


channel_data = pd.DataFrame(channel_statistics)


# In[8]:


channel_data


# In[9]:


channel_data['subscribers'] = pd.to_numeric(channel_data['subscribers'])
channel_data['views'] = pd.to_numeric(channel_data['views'])
channel_data['Total_videos'] = pd.to_numeric(channel_data['Total_videos'])
channel_data.dtypes


# In[10]:


# Format channel_data into dictionary
   
channels = []
for channel_data in channel_statistics:
   channel = {
       "Channel_Details": {
           "Channel_Name": channel_data['channel_name'],
           "Channel_Id": channel_data['channel_id'],
           "Video_Count": channel_data['Total_videos'],
           "Subscriber_Count": channel_data['subscribers'],
           "Channel_Views": channel_data['views'],
           "Channel_Description": channel_data['channel_description'],
           "Playlist_Id": channel_data['playlist_id']
       }
   }
   channels.append(channel)


# In[11]:


def get_video_ids_from_playlist(youtube, playlist_id):
    video_ids = []

    request = youtube.playlistItems().list(
        part='contentDetails',
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
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids


# In[12]:


playlist_id = 'UUCezIgC97PvUuR4_gbFUs5g'  

video_ids = get_video_ids_from_playlist(youtube, playlist_id)

# Print the video IDs
for video_id in video_ids:
    print(video_ids)


# In[13]:


len(video_ids)


# In[14]:


def get_video_statistics(youtube, video_ids):
    all_video_stats = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            video_stats = {
                'channel_title':video['snippet']['channelTitle'],
                'video_id': video['id'],
                'Title': video['snippet']['title'],
                'Published_date': video['snippet']['publishedAt'],
                'views': video['statistics'].get('viewCount', 0),
                'likes': video['statistics'].get('likeCount', 0),
                'dislikes': video['statistics'].get('dislikeCount', 0),
                'comments': video['statistics'].get('commentCount', 0)
            }
            all_video_stats.append(video_stats)

    return all_video_stats


# In[15]:


video_details = get_video_statistics(youtube, video_ids)
video_data = pd.DataFrame(video_details)


# In[16]:


video_data


# In[17]:


video_data['Published_date'] = pd.to_datetime(video_data['Published_date']).dt.date
video_data['views'] = pd.to_numeric(video_data['views'])
video_data['likes'] = pd.to_numeric(video_data['likes'])
video_data['comments'] = pd.to_numeric(video_data['comments'])
video_data.dtypes


# In[18]:


# Format processed video data into a dictionary

videos = []

video_details = get_video_statistics(youtube, video_ids)

for i, video_data in enumerate(video_details):
    video = {
        f"Video_Id_{i + 1}": {
            'Video_Id': video_data['video_id'],
            'Video_Name': video_data['Title'],
            'PublishedAt': video_data['Published_date'],
            'View_Count': video_data['views'],
            'Like_Count': video_data['likes'],
            'Dislike_Count': video_data['dislikes'],
            'Comment_Count': video_data['comments'],
        }
    }

    videos.append(video)


# In[19]:


def get_video_comments(youtube, video_ids):
    all_comments = []
    
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            
            )
            response = request.execute()
        
            comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in response['items'][0:10]]
            comments_in_video_info = {'video_id': video_id, 'comments_words': comments_in_video}

            all_comments.append(comments_in_video_info)
    
        except HttpError as e:
            if e.resp.status == 404:
                print(f"Video with ID '{video_id}' not found.")
            else:
                print("An error occurred while retrieving comments:", e)
    
    return pd.DataFrame(all_comments)


# In[20]:


comments_df = get_video_comments(youtube, video_ids)
comments_df


# In[21]:


video_df = pd.DataFrame()
comments_df = pd.DataFrame()

for channel_stat in channel_statistics:
    channel_name = channel_stat['channel_name']
    print("Getting video information from channel: " + channel_name)
    playlist_id = channel_stat['playlist_id']
    video_ids = get_video_ids_from_playlist(youtube, playlist_id)
    
    # get video data
    video_data = get_video_statistics(youtube, video_ids)
    # get comment data
    comments_data = get_video_comments(youtube, video_ids)

    # append video data together and comment data together
    video_df = video_df.append(video_data, ignore_index=True)
    comments_df = comments_df.append(comments_data, ignore_index=True)


# In[22]:


video_df


# In[23]:


video_df['Published_date'] = pd.to_datetime(video_df['Published_date']).dt.date
video_df['views'] = pd.to_numeric(video_df['views'])
video_df['likes'] = pd.to_numeric(video_df['likes'])
video_df['comments'] = pd.to_numeric(video_df['comments'])
video_df.dtypes


# In[24]:


comments_df


# In[25]:


# Combine channel data and videos data into a dictionary
final_output = {}

# Add channel data to the final_output dictionary
final_output['Channel_Details'] = channel_data

# Add video data to the final_output dictionary
final_output['Videos'] = videos

# Print the final output
print(final_output)


# # mongodb part

# In[26]:


# Create a client instance of MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')


# In[27]:


# create a database
database = client['Yt_project']


# In[28]:


# create a collection
collection = database['Youtube_data']


# In[29]:


# Define the data to insert
data = {
    'Channel_Name': channel_name,
    'Channel_Data': final_output
}


# In[30]:


print(data)


# In[31]:


# Insert or update data in the collection
upload = collection.replace_one({'_id': channel_ids[0]},data, upsert=True)


# In[32]:


# Print the result
if upload.acknowledged:
    print("Data inserted or updated successfully.")
    print("Matched count:", upload.matched_count)
    print("Modified count:", upload.modified_count)
    print("Upserted ID:", upload.upserted_id)
else:
    print("Data insertion or update failed.")


# In[33]:


client.close()


# In[34]:


# Create a client instance of MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')


# In[35]:


# create a database
database = client['Yt_project']


# In[36]:


# create a collection
collection = database['Youtube_data']


# In[46]:


document_names = []
for document in collection.find():
    document_names.append(document["Channel_Name"])

# Fetch the result using document_names list
result = collection.find_one({"Channel_Name": {"$in": document_names}})
print(result)


# In[48]:


client.close()


# In[50]:


# Channel data json to df
channel_details_to_sql = {
            "Channel_Name": result['Channel_Name'],
            "Channel_Id": result['_id'],
            "Video_Count": result['Channel_Data']['Channel_Details']['Total_videos'],
            "Subscriber_Count": result['Channel_Data']['Channel_Details']['subscribers'],
            "Channel_Views": result['Channel_Data']['Channel_Details']['views'],
            "Channel_Description": result['Channel_Data']['Channel_Details']['channel_description'],
            "Playlist_Id": result['Channel_Data']['Channel_Details']['playlist_id']
            }
channel_df = pd.DataFrame.from_dict(channel_details_to_sql, orient='index').T
              


# In[51]:


print(channel_df)


# In[52]:


# playlist data json to df
   
playlist_to_sql = {"Channel_Id": result['_id'],
                       "Playlist_Id": result['Channel_Data']['Channel_Details']['playlist_id']
                       }
playlist_df = pd.DataFrame.from_dict(playlist_to_sql, orient='index').T


# In[53]:


print(playlist_df)


# In[65]:


video_details_list = []
videos = result['Channel_Data']['Videos']
for i in range(1, len(videos) + 1):
    video = videos[i-1]['Video_Id_{}'.format(i)]
    video_details_tosql = {
        'Playlist_Id': result['Channel_Data']['Channel_Details']['playlist_id'],
        'Video_Id': video['Video_Id'],
        'Video_Name': video['Video_Name'],
        'Published_date': video['PublishedAt'],
        'View_Count': video['View_Count'],
        'Like_Count': video['Like_Count'],
        'Dislike_Count': video['Dislike_Count'],
        'Comment_Count': video['Comment_Count']
    }
    video_details_list.append(video_details_tosql)

video_df = pd.DataFrame(video_details_list)


# In[66]:


print(video_df)


# In[75]:


# Handle case where comments are enabled
if 'comment_threads' in video and 'items' in video['comment_threads']:
    comments = {}
    for index, comment_thread in enumerate(video['comment_threads']['items']):
        comment = comment_thread['snippet']['topLevelComment']['snippet']
        comment_id = comment_thread['id']
        comment_text = comment['textDisplay']
        comment_author = comment['authorDisplayName']
        comment_published_at = comment['publishedAt']
        comments[f"Comment_Id_{index + 1}"] = {
            'Comment_Id': comment_id,
            'Comment_Text': comment_text,
            'Comment_Author': comment_author,
            'Comment_PublishedAt': comment_published_at
        }
    video_details_tosql['comments_words'] = comments
else:
    video_details_tosql['comments_words'] = 'Unavailable'


# In[ ]:


Comment_details_list = []

for i in range(1, len(video_df) + 1):
    video_id = f'Video_{i}'
    comments_access = comments_df.loc[comments_df['video_id'] == video_id]
    
    if comments_access.empty or ('Comment_Id_1' not in comments_access or 'Comment_Id_2' not in comments_access):
        Comment_details_tosql = {
            'Video_Id': video_id,
            'Comment_Id': 'Unavailable',
            'Comment_Text': 'Unavailable',
            'Comment_Author': 'Unavailable',
            'Comment_Published_date': 'Unavailable',
        }
        Comment_details_list.append(Comment_details_tosql)
    else:
        for j in range(1, 3):
            comment_key = f"Comment_Id_{j}"
            if comment_key in comments_access:
                Comment_details_tosql = {
                    'Video_Id': video_id,
                    'Comment_Id': comments_access[comment_key]['Comment_Id'],
                    'Comment_Text': comments_access[comment_key]['Comment_Text'],
                    'Comment_Author': comments_access[comment_key]['Comment_Author'],
                    'Comment_Published_date': comments_access[comment_key]['Comment_PublishedAt'],
                }
                Comment_details_list.append(Comment_details_tosql)

Comments_df = pd.DataFrame(Comment_details_list)


# In[82]:


print(Comments_df)


# # mysql

# In[83]:


# Establish a connection to the MySQL server
cnct = mysql.connector.connect(
    host='localhost',
    user='root',
    password='2018',
    
)

# Check if the connection is successful
if cnct.is_connected():
    print("Connected to MySQL server")
else:
    print("Failed to connect to MySQL server")



# In[84]:


# Create a new database
database_name = 'YTube_db'
create_database_query = f"CREATE DATABASE {database_name}"

cursor = cnct.cursor()
cursor.execute(create_database_query)

# Check if the database is created successfully
database_exists_query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{database_name}'"
cursor.execute(database_exists_query)

database_exists = cursor.fetchone()
if database_exists:
    print(f"Database '{database_name}' created successfully")
else:
    print(f"Failed to create database '{database_name}'")


# In[85]:


# Close the cursor and connection
cursor.close()
cnct.close()


# In[86]:


# Connect to the new created database using SQLAlchemy
engine = create_engine('mysql+mysqlconnector://root:2018@localhost/YTube_db', echo=False)


# In[87]:


# Channel data to SQL
channel_df.to_sql('channel', engine, if_exists='append', index=False,
                        dtype = {"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                                "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                "Video_Count": sqlalchemy.types.INT,
                                "Subscriber_Count": sqlalchemy.types.BigInteger,
                                "Channel_Views": sqlalchemy.types.BigInteger,
                                "Channel_Description": sqlalchemy.types.TEXT,
                                "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})


# In[88]:


print(channel_df.to_sql)


# In[89]:


# Playlist data to SQL
playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                   dtype={"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                          "Playlist_Id": sqlalchemy.types.VARCHAR(length=225)})


# In[90]:


print(playlist_df.to_sql)


# In[93]:


video_df.to_sql('video', engine, if_exists='append', index=False,
                dtype = {'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                            'Published_date': sqlalchemy.types.String(length=50),
                            'View_Count': sqlalchemy.types.BigInteger,
                            'Like_Count': sqlalchemy.types.BigInteger,
                            'Dislike_Count': sqlalchemy.types.INT,
                            'Comment_Count': sqlalchemy.types.INT,
                        })               


# In[96]:


print(video_df.to_sql)


# In[94]:


# Comment data to SQL
Comments_df.to_sql('comments', engine, if_exists='append', index=False,
                   
                        dtype = {'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Text': sqlalchemy.types.TEXT,
                                'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Published_date': sqlalchemy.types.String(length=50),})


                   


# In[95]:


print(Comments_df.to_sql)


# # streamlit

# In[155]:


import streamlit as st

# Initialize session state
def init_session_state():
    return {"Get_state": False}

# Configuring Streamlit GUI
st.set_page_config(layout='wide')

# Title
st.title(':red:[Youtube Data Harvesting Project]')

# Data collection zone
col1, col2 = st.columns(2)
with col1:
    st.header(':purple:[Data collection zone]')
    st.write('(Note: This zone **collects data** by using a channel ID and **stores it in the :green:[MongoDB] database**.)')
    channel_ids = st.text_input('**Enter 11 digit channel ID**')
    st.write('''Get data and store it in the MongoDB database by clicking the button below**: :blue:['Get data and store']''')
    Get_data = st.button('**Get data and store**')

# Retrieve and update session state based on button click
    state = init_session_state()
    if Get_data or state["Get_state"]:
        state["Get_state"] = True


# In[158]:


def get_channel_stats(youtube, channel_id):
    try:
        try:
            
            channel_request = youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=channel_id
            )
            channel_response = channel_request.execute()
            

            if 'items' not in channel_response:
                
                st.write(f"Invalid channel ID: {channel_id}")
                st.error("Enter the correct 11-digit channel ID")
                return None

            return channel_response

        except HttpError as e:
            st.error('Server error or check your internet connection or please try again after a few minutes', icon='🚨')
            st.write('An error occurred: %s' % e)
            return None
    except:
            st.write('You have exceeded your YouTube API quota. Please try again tomorrow.')


# In[117]:


st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")


# In[118]:


with col2:
    st.header(':purple[Data Migrate zone]')
    st.write ('''(Note:- This zone specific channel data **Migrate to :blue[MySQL] database from  :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.)''')
    


# In[120]:


document_name = st.selectbox('**Select Channel name**', options = document_names, key='document_names')
st.write('''Migrate to MySQL database from MongoDB database to click below **:blue['Migrate to MySQL']**.''')
Migrate = st.button('**Migrate to MySQL**')
    


# In[122]:


# Define Session state to Migrate to MySQL button
if 'migrate_sql' not in st.session_state:
        st.session_state_migrate_sql = False
if Migrate or st.session_state_migrate_sql:
        st.session_state_migrate_sql = True


# In[123]:


st.header(':purple[Channel Data Analysis zone]')
st.write ('''(Note:- This zone **Analysis of a collection of channel data** depends on your question selection and gives a table format output.)''')


# In[124]:


# Check available channel data
Check_channel = st.checkbox('**Check available channel data for analysis**')


# In[125]:


if Check_channel:
    
   # Create database connection
    engine = create_engine('mysql+mysqlconnector://root:2018@localhost/YTube_db', echo=False)
    # Execute SQL query to retrieve channel names
    query = "SELECT Channel_Name FROM channel;"
    results = pd.read_sql(query, engine)
    # Get channel names as a list
    channel_names_fromsql = list(results['Channel_Name'])
    # Create a DataFrame from the list and reset the index to start from 1
    df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
    # Reset index to start from 1 instead of 0
    df_at_sql.index += 1  
    # Show dataframe
    st.dataframe(df_at_sql)


# In[126]:


st.subheader(':purple[Channels Analysis ]')


# In[127]:


# Selectbox creation
question_tosql = st.selectbox('**Select your Question**',
('1. What are the names of all the videos and their corresponding channels?',
'2. Which channels have the most number of videos, and how many videos do they have?',
'3. What are the top 10 most viewed videos and their respective channels?',
'4. How many comments were made on each video, and what are their corresponding video names?',
'5. Which videos have the highest number of likes, and what are their corresponding channel names?',
'6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
'7. What is the total number of views for each channel, and what are their corresponding channel names?',
'8. What are the names of all the channels that have published videos in the year 2022?',
'9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')


# In[128]:


# Creat a connection to SQL
connect_for_question = pymysql.connect(host='localhost', user='root', password='2018', db='YTube_db')
cursor = connect_for_question.cursor()


# In[135]:


# Q1
if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
    cursor.execute("SELECT channel.Channel_Name, video.Video_Name FROM channel JOIN playlist JOIN video ON channel.Channel_Id = playlist.Channel_Id AND playlist.Playlist_Id = video.Playlist_Id;")
    result_1 = cursor.fetchall()
    df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
    df1.index += 1
    st.dataframe(df1)


# In[137]:


# Q2
if question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
    col1,col2 = st.columns(2)
    with col1:
        cursor.execute("SELECT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
        result_2 = cursor.fetchall()
        df2 = pd.DataFrame(result_2,columns=['Channel Name','Video Count']).reset_index(drop=True)
        df2.index += 1
        st.dataframe(df2)

    with col2:
        fig_vc = px.bar(df2, y='Video Count', x='Channel Name', text_auto='.2s', title="Most number of videos", )
        fig_vc.update_traces(textfont_size=16,marker_color='#E6064A')
        fig_vc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
        st.plotly_chart(fig_vc,use_container_width=True)


# In[139]:


# Q3
if question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':

    col1,col2 = st.columns(2)
    with col1:
        cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.View_Count DESC LIMIT 10;")
        result_3 = cursor.fetchall()
        df3 = pd.DataFrame(result_3,columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
        df3.index += 1
        st.dataframe(df3)

    with col2:
        fig_topvc = px.bar(df3, y='View count', x='Video Name', text_auto='.2s', title="Top 10 most viewed videos")
        fig_topvc.update_traces(textfont_size=16,marker_color='#E6064A')
        fig_topvc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
        st.plotly_chart(fig_topvc,use_container_width=True)


# In[141]:


if question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
    cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
    result_4 = cursor.fetchall()
    df4 = pd.DataFrame(result_4,columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
    df4.index += 1
    st.dataframe(df4)


# In[143]:


# Q5
if question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
    result_5= cursor.fetchall()
    df5 = pd.DataFrame(result_5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
    df5.index += 1
    st.dataframe(df5)


# In[145]:


# Q6
if question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
    cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count, video.Dislike_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
    result_6= cursor.fetchall()
    df6 = pd.DataFrame(result_6,columns=['Channel Name', 'Video Name', 'Like count','Dislike count']).reset_index(drop=True)
    df6.index += 1
    st.dataframe(df6)


# In[147]:


# Q7
if question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

    col1, col2 = st.columns(2)
    with col1:
        cursor.execute("SELECT Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
        result_7= cursor.fetchall()
        df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
        df7.index += 1
        st.dataframe(df7)
    
    with col2:
        fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s', title="Total number of views", )
        fig_topview.update_traces(textfont_size=16,marker_color='#E6064A')
        fig_topview.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
        st.plotly_chart(fig_topview,use_container_width=True)


# In[148]:


# Q8
if question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
    cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Published_date FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM Published_date) = 2022;")
    result_8= cursor.fetchall()
    df8 = pd.DataFrame(result_8,columns=['Channel Name','Video Name', 'Year 2022 only']).reset_index(drop=True)
    df8.index += 1
    st.dataframe(df8)


# In[149]:


# Q9
if question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    cursor.execute("SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
    result_9= cursor.fetchall()
    df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
    df9.index += 1
    st.dataframe(df9)


# In[150]:


# Q10
if question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Comment_Count DESC;")
    result_10= cursor.fetchall()
    df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
    df10.index += 1
    st.dataframe(df10)


# In[151]:


# SQL DB connection close
connect_for_question.close()


# In[ ]:




