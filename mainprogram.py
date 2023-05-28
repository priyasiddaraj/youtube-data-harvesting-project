import streamlit as st
from youtube import *
from mongodata import *
from sqldata import *
def Get_Data_YT():
    channelId = st.sidebar.text_input("ENTER CHANNEL ID")
    getdata = st.sidebar.button('FETCH DATA')
    if channelId and getdata:
        Data = main(channelId)
        print("********")
        print(Data)
        display = st.json(Data)
        try:
            if Channel_id_uniq(channelId):
                st.sidebar.write('data already present in mongodb')
            else:
                insert_data_into_db(Data)
        except:
            st.write('already exist')
def migrate_data():
    channel_name = st.sidebar.selectbox('select the channel',list_channel())

    if channel_name != channel_details_mogo(channel_name):
        data_mogo = channel_details_mogo(channel_name)
        Ch_D = pd.DataFrame(data_mogo['Channel_Details'], index=[0])
        pl = pd.DataFrame(data_mogo['Playlist_Details'])
        vd = pd.DataFrame(data_mogo['Video_Details'])
        CMD = pd.DataFrame(data_mogo['Comments'])
        st.title('Channel_details')
        st.dataframe(Ch_D)
        st.title('Playlist_details')
        st.dataframe(pl)
        st.title('Video_details')
        st.dataframe(vd)
        st.title('Comments_details')
        st.dataframe(CMD)

    engine = create_My_engine()
    Migrate = st.sidebar.button('MIGRATE DATA')
    if Migrate:


        if channel_name in pd.read_sql_query('select * from Channels',engine)['channelName'].to_list():
            st.sidebar.write('ALREADY EXISTS')
        else:
            Ch_D.astype(str).to_sql('Channels', con= engine,if_exists ='append', index = True)
            pl.astype(str).to_sql('playlist_details', con = engine,if_exists ='append', index = True)
            vd.astype(str).to_sql('video_details', con=engine,if_exists ='append', index = True)
            CMD.astype(str).to_sql('comments', con=engine,if_exists ='append', index = True)
            st.sidebar.write("inserted successfully")

def Query_data():
    Query_list = [None,'# Channels published in 2022','# Highest likes','# COMMENT COUNT','# NAMES OF ALL VIDEOS ','# MOST NO OF VIDEOS ','# TOP 10 VIDEOS ',
                  '# NO OF LIKES ','# NO OF VIEWS ','# NO OF COMMENTS']
    Query = st.sidebar.selectbox('SELECT THE QUERY', Query_list)
    engine = create_My_engine()
    print("query: ",Query)
    if Query ==None:
        pass

    elif Query == '# Channels published in 2022':
        # print("Channels published in 2022")
        st.title('Channels published in 2022')
        df = pd.read_sql_query('select channelTitle,publishedAt from video_details where publishedAt like '%2022%' limit 10', engine)
        st.dataframe(df)

    elif Query== '# Highest likes':
        # print("enter into highest likes")
        st.title('Highest likes')
        df = pd.read_sql_query('select channeltitle ,title,likecount from video_details order by cast (likecount as integer)desc limit 10',engine)
        st.dataframe(df)

    elif Query== '# COMMENT COUNT':
        # print("COMMENT COUNT")
        st.title('COMMENT COUNT')
        df = pd.read_sql_query('select commentCount, title from video_details limit 100',engine)
        st.dataframe(df)

    elif Query== '# NAMES OF ALL VIDEOS ':
        # print("NAMES OF VIDEOS")
        st.title(' NAMES OF ALL VIDEOS ')
        df = pd.read_sql_query(' select channelTitle, title from video_details limit 100',engine)
        st.dataframe(df)

    elif Query== '# MOST NO OF VIDEOS ':
        # print("MOST NO  OF VIDEOS")
        st.title(' MOST NO OF VIDEOS ')
        df = pd.read_sql_query('select channelName,total_videos from Channels order by total_videos limit 100',engine)
        st.dataframe(df)

    elif Query== '# TOP 10 VIDEOS ':
        # print("TOP 10 VIDEOS")
        st.title(' TOP 10 VIDEOS ')
        df = pd.read_sql_query(' select title, channelTitle, viewCount  from video_details group by channelTitle order by viewCount',engine)
        st.dataframe(df)

    elif Query== '# NO OF LIKES ':
        # print("NO OF LIKES")
        st.title(' NO OF LIKES ')
        df = pd.read_sql_query(' select title,likeCount from video_details order by likeCount limit 100',engine)
        st.dataframe(df)

    elif Query== '# NO OF VIEWS ':
        # print("NO OF VIEWS")
        st.title(' NO OF VIEWS ')
        df = pd.read_sql_query(' select channelName, viewCount from Channels limit 10',engine)
        st.dataframe(df)

    elif Query== '# NO OF COMMENTS':
        # print("NO OF COMMENTS")
        st.title(' NO OF COMMENTS ')
        df = pd.read_sql_query(' select channelTitle, commentCount from video_details order by commentCount desc',engine)
        st.dataframe(df)

    # elif Query== '# NO OF VIEWS ':
    #     print("NO OF VIEWS")
    #     st.title(' NO OF VIEWS ')
    #     df = pd.read_sql_query(' select channelName, viewCount from Channels limit 10',engine)
    #     st.dataframe(df)

    else:
        print("query choosen wrongly")





YoutubeData = st.sidebar.selectbox('SELECT THE OPTION',['FETCH  DATA','MIGRATE','QUERY'])
if YoutubeData == 'FETCH  DATA':
    st.title('YOUTUBE DATA HARVESTING AND WAREHOUSING ')
    Get_Data_YT()
elif YoutubeData == 'MIGRATE':
    migrate_data()
elif YoutubeData == 'QUERY':
    Query_data()