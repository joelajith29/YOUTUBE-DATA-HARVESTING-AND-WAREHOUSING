from googleapiclient.discovery import build
import streamlit as st
import pymongo
import pandas as pd
import pymysql


def Api_connect():
    Api_Id = 'AIzaSyB0UdYcvOwVi2flCtEo6Hk0SDGJ4Xmnf2s'

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
                        likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
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


def channels_table(sel_ch):
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
        if row['channel_Name'] in sel_ch:
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


def playlist_table(sel_ch):
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
        if row['Channel_Name'] in sel_ch :
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


def videos_table(sel_ch):
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

    global vid
    vid=[]
    for index, row in df2.iterrows():
        if row['channel_Name'] in sel_ch :
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
            vid.append(row['video_id'])
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
        if row['Video_id'] in vid:
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


def tables(sel):
    channels_table(sel)
    playlist_table(sel)
    videos_table(sel)
    comments_table()

    return 'Tables created successfully'

with st.sidebar:
    st.title(':male-technologist: :blue[YOUTUBE DATA HARVESTING AND WAREHOUSE]')
    st.header(':green[Skill Take Away]')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('MongoDB')
    st.caption('API Intergration')
    st.caption('Data Management using MongoDB and SQL')

channel_id = st.text_input('Enter the channel ID')


col1,col2 = st.columns(2)

with col1 :

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
    
# create a dropdown for channel selection     

ch_list = []
db = client['youtube_data']
coll1 = db['channel_details']
for ch_data in coll1.find({}, {'_id': 0, 'channel_information': 1}):
    ch_list.append(ch_data['channel_information']['channel_Name'])
        
selected_channel =st.multiselect('select a channel  to migrate the details into sql:',ch_list)
st.write(f'You selected: {selected_channel}') 

try:
    mydb = pymysql.Connection(host='127.0.0.1', user='root', passwd='Joel@123', database='youtube_data')
    cursor = mydb.cursor()
except:
    pass    

if st.button('Migrate to Sql'):
    Table = tables(selected_channel)
    st.success(Table)

show = st.radio('SELECT THE TABLE FOR VIEW', ('CHANNELS', 'PLAYLISTS', 'VIDEOS', 'COMMENTS'), index=None)

if show == "CHANNELS":
        ch = pd.read_sql_query("select * from channels;", mydb)
        st.write(ch)
elif show == "PLAYLISTS":
        pl = pd.read_sql_query("select * from playlists;", mydb)
        st.write(pl)
elif show == "VIDEOS":
        vd = pd.read_sql_query("select * from videos;", mydb)
        st.write(vd)
elif show == "COMMENTS":
        ct = pd.read_sql_query("select * from comments;", mydb)
        st.write(ct)

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
                                                 ),index=None)

mydb = pymysql.Connection(host='127.0.0.1', user='root', passwd='Joel@123', database='youtube_data')
cursor = mydb.cursor()

if question == '1.All the videos and the channel name':
    query1 = '''select title videos,channel_name channelname from videos;'''
    res=pd.read_sql_query(query1,mydb)
    st.write(res)

elif question == '2.Channels with most number of videos':
    query2 = '''select  channel_Name,Total_videos from channels order by Total_videos;'''
    res=pd.read_sql_query(query2,mydb)
    st.write(res)

elif question == '3.Top 10 most viewed videos':
    query3 = '''select views,channel_Name,Title videotitle  from videos where views 
                is not null order by views desc limit 10;'''
    res=pd.read_sql_query(query3,mydb)
    st.write(res)

elif question == '4.Comments in each videos':
    query4 = '''select comments totcom,Title videotitle from videos where comments is not null;'''
    res=pd.read_sql_query(query4,mydb)
    st.write(res)

elif question == '5.Videos with highest likes':
    query5 = '''select Title videotitle,likes from videos where likes is not null order by likes desc;'''
    res=pd.read_sql_query(query5,mydb)
    st.write(res)

elif question == '6.likes of all videos':
    query6 = '''select likes,title videotitle from videos;'''
    res=pd.read_sql_query(query6,mydb)
    st.write(res)

elif question == '7.Views of each channel':
    query7 = '''select views,channel_name from channels;'''
    res=pd.read_sql_query(query7,mydb)
    st.write(res)

elif question == '8.Videos published in the year of 2022':
    query8 = '''select title videotitle,published_date from videos where year(published_date)=2022;'''
    res=pd.read_sql_query(query8,mydb)
    st.write(res)

elif question == '9.Average duration of all videos in each channel':
    query9 = '''select channel_name,avg(duration)from videos group by channel_name;'''
    res=pd.read_sql_query(query9,mydb)
    st.write(res)

elif question == '10.Videos with highest number of comments':
    query10 = '''select title videotitle,channel_name channelname,comments  from videos where comments is not null order by comments desc limit 1;'''
    res=pd.read_sql_query(query10,mydb)
    st.write(res)
