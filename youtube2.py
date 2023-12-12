from googleapiclient.discovery import build
import streamlit as st
import pymongo
import pandas as pd
import pymysql


def Api_connect():
    Api_Id = 'AIzaSyBxDzD3QddZdqY0Qavzkd9izYPqyE3p6DQ'

    api_service_name = 'youtube'
    api_version = 'v3'

    youtube = build(api_service_name, api_version, developerKey=Api_Id)

    return youtube


youtube = Api_connect()


# channel

def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(channel_Name=i['snippet']['title'],
                    channel_id=i['id'],
                    Subscribers=i['statistics']['subscriberCount'],
                    views=i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Channel_Description=i['snippet']['description'],
                    Playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data


# video
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# video information
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response = request.execute()

        for item in response['items']:
            data = dict(channel_Name=item['snippet']['channelTitle'],
                        channel_id=item['snippet']['channelId'],
                        video_id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet']['description'],
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics']['viewCount'],
                        likes=item['statistics'].get('commentCount'),
                        Comments=item['statistics']['commentCount'],
                        Favorite_count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


def get_comment_info(vid_ids):
    Comment_data = []
    try:
        for video_id in vid_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                Comment_data.append(data)
    except:
        pass
    return Comment_data


# get playlist id
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


client = pymongo.MongoClient('mongodb://localhost:27017')

db = client['youtube_data']


def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db['channel_details']
    coll1.insert_one({'channel_information': ch_details, 'Playlist_information': pl_details,
                      'video_information': vi_details, 'comment_information': com_details})

    return 'upload completed successfully'


try:
    mydb = pymysql.Connection(host='127.0.0.1', user='root', passwd='Joel@123')

    cur = mydb.cursor()

    cur.execute('Create database if not exists youtube_data')
except:
    pass


def channels_table():
    mydb = pymysql.Connection(host='127.0.0.1', user='root', passwd='Joel@123', database='youtube_data')
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    # try:
    create_query = '''create table if not exists channels(channel_Name varchar(100),
                                                        channel_id varchar(100) primary key,
                                                        Subscribers bigint,
                                                        views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()

    # except:
    # print('channels table already created')

    ch_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({}, {'_id': 0, 'channel_information': 1}):
        ch_list.append(ch_data['channel_information'])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels(channel_Name,
                                             channel_id,
                                             Subscribers,
                                             views,
                                             Total_Videos,
                                             Channel_Description,
                                             Playlist_id)
                                             values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_Name'],
                  row['channel_id'],
                  row['Subscribers'],
                  row['views'],
                  row['Total_Videos'],
                  row['Channel_Description'],
                  row['Playlist_id'])

        # try:
        cursor.execute(insert_query, values)
        mydb.commit()

    # except:
    #    print('channels values are already inserted')


def playlist_table():
    mydb = pymysql.Connection(host='127.0.0.1', user='root', passwd='Joel@123', database='youtube_data')
    cursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                    Title varchar(100) ,
                                                    Channel_Id varchar(100),
                                                    Channel_Name varchar(100),
                                                    PublishedAt TIMESTAMP,
                                                    Video_Count int
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for pl_data in coll1.find({}, {'_id': 0, 'Playlist_information': 1}):
        for i in range(len(pl_data['Playlist_information'])):
            pl_list.append(pl_data['Playlist_information'][i])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = '''INSERT ignore INTO playlists(Playlist_Id,
                                             Title,
                                             Channel_Id,
                                             Channel_Name,
                                             PublishedAt,
                                             Video_Count
                                             )
                                             
                                             values(%s,%s,%s,%s,%s,%s)'''
        # row["PublishedAt"] = row["PublishedAt"].replace("T"," ").replace("Z"," ")
        values = (row['Playlist_Id'],
                  row['Title'],
                  row['Channel_Id'],
                  row['Channel_Name'],
                  row['PublishedAt'],
                  row['Video_Count']
                  )

        cursor.execute(insert_query, values)
        mydb.commit()


def videos_table():
    mydb = pymysql.Connection(host='127.0.0.1', user='root', passwd='Joel@123', database='youtube_data')
    cursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists videos(channel_Name varchar(100),
                                                     channel_id varchar(100),
                                                     video_id varchar(80) primary key,
                                                     Title varchar(150),
                                                     Tags text,
                                                     Thumbnail varchar(200),
                                                     Description text,
                                                     Published_Date TIMESTAMP,
                                                     Duration TIME,
                                                     Views BIGINT,
                                                     likes BIGINT,
                                                     Comments int,
                                                     Favorite_count int,
                                                     Definition varchar(30),
                                                     Caption_Status varchar(50)
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for vi_data in coll1.find({}, {'_id': 0, 'video_information': 1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query = '''INSERT ignore INTO videos(Channel_Name,
                                                      Channel_id,
                                                      Video_id,
                                                      Title,
                                                      Tags,
                                                      Thumbnail,
                                                      Description,
                                                      Published_Date,
                                                      Duration,
                                                      Views,
                                                      Likes,
                                                      Comments,
                                                      Favorite_Count,
                                                      Definition,
                                                      Caption_Status
                                                  )

                                                 values(%s,%s,%s,%s,%a,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        # row["PublishedAt"] = row["PublishedAt"].replace("T"," ").replace("Z"," ")
        values = (row['channel_Name'],
                  row['channel_id'],
                  row['video_id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'].replace('PT', ':').replace('H', ':').replace('M', ':').split('S')[0],
                  row['Views'],
                  row['likes'],
                  row['Comments'],
                  row['Favorite_count'],
                  row['Definition'],
                  row['Caption_Status']
                  )

        cursor.execute(insert_query, values)
        mydb.commit()


def comments_table():
    mydb = pymysql.Connection(host='127.0.0.1', user='root', passwd='Joel@123', database='youtube_data')
    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comments(comment_Id varchar(100) primary key,
                                                              Video_id varchar(50),
                                                              Comment_Text text,
                                                              Comment_Author varchar(150),
                                                              Comment_Published TIMESTAMP
                                                              )'''
    cursor.execute(create_query)
    mydb.commit()

    com_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for com_data in coll1.find({}, {'_id': 0, 'comment_information': 1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        insert_query = '''INSERT ignore INTO comments(comment_Id,
                                                            Video_id,
                                                            Comment_Text,
                                                            Comment_Author,
                                                            Comment_Published
                                                            )

                                                            values(%s,%s,%s,%s,%s)'''
        # row["PublishedAt"] = row["PublishedAt"].replace("T"," ").replace("Z"," ")
        values = (row['comment_Id'],
                  row['Video_id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  row['Comment_Published']
                  )

        cursor.execute(insert_query, values)
        mydb.commit()


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return 'Tables created successfully'


def show_channels_table():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    ch_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({}, {'_id': 0, 'channel_information': 1}):
        ch_list.append(ch_data['channel_information'])
    df = st.dataframe(ch_list)

    return df


def show_playlists_table():
    pl_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for pl_data in coll1.find({}, {'_id': 0, 'Playlist_information': 1}):
        for i in range(len(pl_data['Playlist_information'])):
            pl_list.append(pl_data['Playlist_information'][i])
    df1 = st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for vi_data in coll1.find({}, {'_id': 0, 'video_information': 1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2 = st.dataframe(vi_list)

    return df2


def show_comments_table():
    com_list = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for com_data in coll1.find({}, {'_id': 0, 'comment_information': 1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3 = st.dataframe(com_list)

    return df3


with st.sidebar:
    st.title(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
    st.header(':green[Skill Take Away]')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('MongoDB')
    st.caption('API Intergration')
    st.caption('Data Management using MongoDB and SQL')

channel_id = st.text_input('Enter the channel ID')

if st.button('collect and store data'):
    ch_ids = []
    db = client['youtube_data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({}, {'_id': 0, 'channel_information': 1}):
        ch_ids.append(ch_data['channel_information']['channel_id'])

    if channel_id in ch_ids:
        st.success('Channel Details of the Given ID Already Exists')

    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button('Migrate to Sql'):
    Table = tables()
    st.success(Table)

show_table = st.radio('SELECT THE TABLE FOR VIEW', ('CHANNELS', 'PLAYLISTS', 'VIDEOS', 'COMMENTS'), index=None)

if show_table == 'CHANNELS':
    show_channels_table()

elif show_table == 'PLAYLISTS':
    show_playlists_table()

elif show_table == 'VIDEOS':
    show_videos_table()

elif show_table == 'COMMENTS':
    show_comments_table()

question = st.selectbox('Select your question', ('1.All the videos and the channel name',
                                                 '2.Channels with most number of videos',
                                                 '3.Top 10 most viewed videos',
                                                 '4.Comments in each videos',
                                                 '5.Videos with highest likes',
                                                 '6.likes of all videos',
                                                 '7.Views of each channel',
                                                 '8.Videos published in the year of 2022',
                                                 '9.Average duration of all videos in each channel',
                                                 '10.Videos with highest number of comments',
                                                 ))

mydb = pymysql.Connection(host='127.0.0.1', user='root', passwd='Joel@123', database='youtube_data')
cursor = mydb.cursor()

if question == '1.All the videos and the channel name':
    query1 = '''select title videos,channel_name channelname from videos;'''
    cursor.execute(query1)
    mydb.commit()
    a1 = cursor.fetchall()
    df = pd.DataFrame(a1, columns=['Name of the video title', 'channel name'])
    st.write(df)

elif question == '2.Channels with most number of videos':
    query2 = '''select  channel_Name,Total_videos from channels order by Total_videos;'''
    cursor.execute(query2)
    mydb.commit()
    a2 = cursor.fetchall()
    df1 = pd.DataFrame(a2, columns=['Name of the channel name', 'Total videos'])
    st.write(df1)

elif question == '3.Top 10 most viewed videos':
    query3 = '''select views,channel_Name,Title videotitle  from videos where views 
                is not null order by views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    a3 = cursor.fetchall()
    df2 = pd.DataFrame(a3, columns=['views', 'channel name', 'videotitle'])
    st.write(df2)

elif question == '4.Comments in each videos':
    query4 = '''select comments totcom,Title videotitle from videos where comments is not null;'''
    cursor.execute(query4)
    mydb.commit()
    a4 = cursor.fetchall()
    df3 = pd.DataFrame(a4, columns=['Total Comments', 'Video Title'])
    st.write(df3)

elif question == '5.Videos with highest likes':
    query5 = '''select Title videotitle,likes from videos where likes is not null order by likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    a5 = cursor.fetchall()
    df4 = pd.DataFrame(a5, columns=['Video Title', 'likes'])
    st.write(df4)

elif question == '6.likes of all videos':
    query6 = '''select likes,title videotitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    a6 = cursor.fetchall()
    df5 = pd.DataFrame(a6, columns=['likes', 'Videos'])
    st.write(df5)

elif question == '7.Views of each channel':
    query7 = '''select views,channel_name from channels;'''
    cursor.execute(query7)
    mydb.commit()
    a7 = cursor.fetchall()
    df6 = pd.DataFrame(a7, columns=['Views', 'Channel Name'])
    st.write(df6)

elif question == '8.Videos published in the year of 2022':
    query8 = '''select title videotitle,published_date from videos where year(published_date)=2022;'''
    cursor.execute(query8)
    mydb.commit()
    a8 = cursor.fetchall()
    df7 = pd.DataFrame(a8, columns=['videos', 'Published Year'])
    st.write(df7)

elif question == '9.Average duration of all videos in each channel':
    query9 = '''select channel_name,avg(duration)from videos group by channel_name;'''
    cursor.execute(query9)
    mydb.commit()
    a9 = cursor.fetchall()
    df8 = pd.DataFrame(a9, columns=['Channel Name', 'Average Duration'])
    st.write(df8)

elif question == '10.Videos with highest number of comments':
    query10 = '''select title videotitle,channel_name channelname,comments  from videos where comments is not null order by comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    a10 = cursor.fetchall()
    df9 = pd.DataFrame(a10, columns=['Video Title', 'Channel Name', 'Comments'])
    st.write(df9)