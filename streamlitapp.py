# ["Youtube API libraries"]
from googleapiclient.discovery import build
import re

# [SQL libraries]
import mysql.connector
import sqlalchemy
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine
import pymysql
import pandas as pd

# [STREAMLIT libraries]
from streamlit_option_menu import option_menu
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="YouTube", layout="wide", initial_sidebar_state="expanded")

#___________________________________________________ CREATING OPTION MENU _____________________________________________________________________________________
with st.sidebar:
    selected = option_menu(None, ["Home","Data Zone","Analysis Zone","Query Zone"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px",
                                                "--hover-color": "#cacfd2"},
                                   "icon": {"font-size": "25px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#5b2c6f"}})
#__________________________________________________ HOME PAGE __________________________________________________________________________________________________
if selected == "Home":
    # Title
    st.title(':red[YOUTUBE DATA HARVESTING and WAREHOUSING using SQL and STREAMLIT]')
    st.markdown("## :violet[Domain] : Social Media")
    st.markdown("## :violet[Skills take away From This Project] : Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL")
    st.markdown("## :violet[Overall view] : Building a simple UI with Streamlit, retrieving data from YouTube API, storing the data SQL as a WH, querying the data warehouse with SQL, and displaying the data in the Streamlit app.")
    st.markdown("## :violet[Developed by] : Vignesh.G")

#__________________________________________________ Data Zone __________________________________________________________________________________________________
elif selected == "Data Zone":
    tab1, tab2 = st.tabs(["$\huge COLLECT $", "$\huge MIGRATE $"])

    # Initialize empty DataFrames for channel, playlist, video statistics, and comments
    channel_data = pd.DataFrame()
    pl_data = pd.DataFrame()
    v_stats_df = pd.DataFrame()
    channel_comment = pd.DataFrame()

    # Define function to fetch and display channel details
    def fetch_display_channel_details(channel_id):
        # Fetch channel details
        channel_data = pd.DataFrame(get_channel_stats(channel_id))

        # Convert columns to numeric values
        if not channel_data.empty:
            channel_data['Subscribers'] = pd.to_numeric(channel_data['subscribers'])
            channel_data['Views'] = pd.to_numeric(channel_data['views'])
            channel_data['Total Videos'] = pd.to_numeric(channel_data['Total_videos'])
            
            # Display channel details
            st.subheader("Channel Details")
            st.dataframe(channel_data)

    #________________________________________________ COLLECT TAB ______________________________________________________________________________________________
    with tab1:
        st.markdown('## :blue[Data Collection Zone]')
        st.write('(**collects data** by using channel id and **stores it in the :orange[SQL] database**.)')
        channel_id = st.text_input('**Enter the channel_id**')
        st.write('Click below to retrieve and store data.')
        Get_data = st.button('**Retrieve and store data**')

        # Define Session state to Get data button
        if "Get_state" not in st.session_state:
            st.session_state.Get_state = False
        if Get_data or st.session_state.Get_state:
            st.session_state.Get_state = True

            api_key = "AIzaSyAwr2kUHgtvhpnUlIzyZk-QVzrHLvzEuek"  # Replace with your YouTube API key
            youtube = build("youtube", "v3", developerKey=api_key)

            # Function to get channel statistics
            def get_channel_stats(channel_id):
                all_data = []
                try:
                    request = youtube.channels().list(
                     part="snippet,contentDetails,statistics",
                     id=channel_id
                    )
                    response = request.execute()
                
                    # Check if 'items' key exists in the response
                    if 'items' not in response or not response['items']:
                        st.error("No data found for this Channel ID. Please check the Channel ID and try again.")
                        return pd.DataFrame()  # Return empty DataFrame if no items are found
      
                    for item in response['items']:
                        data = {
                            'channel_name': item['snippet']['title'],
                            'channel_id': item['id'],
                            'subscribers': item['statistics']['subscriberCount'],
                            'views': item['statistics']['viewCount'],
                            'Total_videos': item['statistics']['videoCount'],
                            'channel_description': item['snippet']['description'],
                            'Playlist_Id': item['contentDetails']['relatedPlaylists']['uploads']
                        }
                        all_data.append(data)
                except HttpError as e:
                    st.error(f"An error occurred: {e}")
                    return pd.DataFrame()  # Return empty DataFrame if there's an API error   

                return pd.DataFrame(all_data)

            # Fetch and display channel details
            fetch_display_channel_details(channel_id)

            # Playlist information
            def get_pl_info(youtube, channel_id):
                playlist = []
                try:
                    # Check if channel_id is empty
                    if not channel_id:
                        st.error("Please enter a valid Channel ID.")
                        return pd.DataFrame()
                    
                    request = youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50
                    )
                    response = request.execute()

                    # Check if 'items' key exists in the response
                    if 'items' not in response or not response['items']:
                        st.error("No playlists found for this Channel ID.")
                        return pd.DataFrame()  # Return empty DataFrame if no items are found
                    
                    for item in response['items']:
                        data = {
                            'pl_id': item['id'],
                            'channel_name': item['snippet']['title'],
                            'channel_id': item['snippet']['channelId'],
                            'publish_at': item['snippet']['publishedAt'],
                            'videos_count': item['contentDetails']['itemCount']
                        }
                        playlist.append(data)
                except HttpError as e:
                    st.error(f"An error occurred: {e}")
                    return pd.DataFrame()

                return pd.DataFrame(playlist)

            pl_data = get_pl_info(youtube, channel_id)

            # Fetch and display playlists
            if not pl_data.empty:
                st.subheader("Playlists")
                st.dataframe(pl_data)

            #___________________________________________________ MIGRATE TAB ___________________________________________________________________________________________
            with tab2:
                st.markdown('## :blue[Data Migration Zone]')
                st.write('(**Migrates Channel Data to :green[MYSQL] Database**)')

                st.write('Click below for **Data Migration**.')
                Migrate = st.button('**Migrate to MySQL**')
                if 'migrate_sql' not in st.session_state:
                    st.session_state.migrate_sql = False
                if Migrate or st.session_state.migrate_sql:
                    st.session_state.migrate_sql = True

                    # Connection to SQL
                    connect = pymysql.connect(
                        host="localhost",
                        user="vicky",
                        password="Vicky123",
                        database="youtubedata",
                        port=3306
                    )
                    mycursor = connect.cursor()
                    mycursor.execute('CREATE DATABASE IF NOT EXISTS youtubedata')
                    mycursor.close()
                    connect.close()

                    engine = create_engine("mysql+pymysql://vicky:Vicky123@localhost:3306/youtubedata")
                    connection = engine.connect()

                    # Check and migrate data to SQL tables
                    if not channel_data.empty:
                        channel_data.to_sql(con=engine, name='channels', if_exists='append', index=False,
                                            dtype={
                                                'Channel_Name': sqlalchemy.types.VARCHAR(length=225),
                                                'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                                                'Subscribers': sqlalchemy.types.BigInteger,
                                                'Views': sqlalchemy.types.INT,
                                                'Total_Videos': sqlalchemy.types.BigInteger,
                                                'Channel_Description': sqlalchemy.types.TEXT,
                                                'Playlist_Id': sqlalchemy.types.VARCHAR(length=225)
                                            })
                    # Other migration steps for playlist, video statistics, comments, etc.
                    st.success("Data Migration Completed Successfully!")


#_______________________________________________________________Analysis Zone _________________________________________________________________________________                
if selected == "Analysis Zone":
    st.header(':blue[Channel Data Analysis zone]')
    st.write('''(Checks for available channels by clicking this checkbox)''')
    
    # Check available channel data
    Check_channel = st.checkbox('**Check available channel data for analysis**')
    if Check_channel:
        # Create database connection
        engine = create_engine("mysql+pymysql://vicky:Vicky123@localhost:3306/youtubedata")
        
        # Execute SQL query to retrieve channel names
        query = "SELECT channel_name FROM channels;"
        results = pd.read_sql(query, engine)
        
        # Get channel names as a list and create a DataFrame
        channel_names_fromsql = list(results['channel_name'])
        df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
        
        # Drop duplicates and reset index to start from 1
        df_at_sql.drop_duplicates(inplace=True)
        df_at_sql.index += 1
        
        # Display the DataFrame
        st.dataframe(df_at_sql)

#___________________________________________________________________ QUERY ZONE ________________________________________________________________________________

if selected == "Query Zone":
    st.subheader(':blue[Queries and Results ]')
    st.write('''(Queries were answered based on :orange[**Channel Data analysis**])''')
    
    # Selectbox for queries
    question_tosql = st.selectbox('Select your Question',
                                  ('1. What are the names of all the videos and their corresponding channels?',
                                   '2. Which channels have the most number of videos, and how many videos do they have?',
                                   '3. What are the top 10 most viewed videos and their respective channels?',
                                   '4. How many comments were made on each video, and what are their corresponding video names?',
                                   '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                   '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                   '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                   '8. What are the names of all the channels that have published videos in the year 2022?',
                                   '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                   '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                                  key='collection_question')

    # Create a connection to SQL
    connect_for_question = pymysql.connect(host='localhost', user='vicky', password='Vicky123', port=3306, db='youtubedata')
    cursor = connect_for_question.cursor()
    engine = create_engine("mysql+pymysql://vicky:Vicky123@localhost:3306/youtubedata")

    # Define query options
    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("SELECT video.Channel_name, video.Title FROM video")
        result_1 = cursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
        col1, col2 = st.columns(2)
        with col1:
            cursor.execute("SELECT channels.channel_name, channels.Total_videos FROM channels ORDER BY Total_videos DESC;")
            result_2 = cursor.fetchall()
            df2 = pd.DataFrame(result_2, columns=['Channel Name', 'Video Count']).reset_index(drop=True)
            df2.index += 1
            st.dataframe(df2)

        with col2:
            fig_vc = px.bar(df2, y='Video Count', x='Channel Name', text_auto='.2s', title="Most number of videos")
            fig_vc.update_traces(textfont_size=16, marker_color='#E6064A')
            fig_vc.update_layout(title_font_color='#1308C2', title_font=dict(size=25))
            st.plotly_chart(fig_vc, use_container_width=True)

    elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
        col1, col2 = st.columns(2)
        with col1:
            cursor.execute("SELECT video.Title, video.Views, video.Channel_name FROM video ORDER BY video.Views DESC LIMIT 10;")
            result_3 = cursor.fetchall()
            df3 = pd.DataFrame(result_3, columns=['Video Name', 'View count', 'Channel Name']).reset_index(drop=True)
            df3.index += 1
            st.dataframe(df3)

        with col2:
            fig_topvc = px.bar(df3, y='View count', x='Video Name', text_auto='.2s', title="Top 10 most viewed videos")
            fig_topvc.update_traces(textfont_size=16, marker_color='#E6064A')
            fig_topvc.update_layout(title_font_color='#1308C2', title_font=dict(size=25))
            st.plotly_chart(fig_topvc, use_container_width=True)

    elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("SELECT video.Title, video.Comments FROM video;")
        result_4 = cursor.fetchall()
        df4 = pd.DataFrame(result_4, columns=['Video Name', 'Comment count']).reset_index(drop=True)
        df4.index += 1
        st.dataframe(df4)

    elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("SELECT video.Channel_name, video.Title, video.Likes FROM video ORDER BY video.Likes DESC;")
        result_5 = cursor.fetchall()
        df5 = pd.DataFrame(result_5, columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
        df5.index += 1
        st.dataframe(df5)

    elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
        cursor.execute("SELECT video.Channel_name, video.Title, video.Likes FROM video ORDER BY video.Likes DESC;")
        result_6 = cursor.fetchall()
        df6 = pd.DataFrame(result_6, columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
        df6.index += 1
        st.dataframe(df6)

    elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        col1, col2 = st.columns(2)
        with col1:
            cursor.execute("SELECT channel_name, views FROM channels ORDER BY views DESC;")
            result_7 = cursor.fetchall()
            df7 = pd.DataFrame(result_7, columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
            df7.index += 1
            st.dataframe(df7)

        with col2:
            fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s', title="Total number of views")
            fig_topview.update_traces(textfont_size=16, marker_color='#E6064A')
            fig_topview.update_layout(title_font_color='#1308C2', title_font=dict(size=25))
            st.plotly_chart(fig_topview, use_container_width=True)

    elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("SELECT video.channel_name, video.Published_date FROM video WHERE EXTRACT(YEAR FROM Published_date) = 2022")
        result_8 = cursor.fetchall()
        df8 = pd.DataFrame(result_8, columns=['Channel Name', 'Year 2022 only']).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)

    elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""
            SELECT video.Channel_name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS Duration
            FROM video GROUP BY Channel_name ORDER BY Duration DESC
        """)
        result_9 = cursor.fetchall()
        df9 = pd.DataFrame(result_9, columns=['Channel Name', 'Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
        df9.index += 1
        st.dataframe(df9)

    elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("SELECT video.Channel_name, video.Title, video.Comments FROM video ORDER BY video.Comments DESC;")
        result_10 = cursor.fetchall()
        df10 = pd.DataFrame(result_10, columns=['Channel Name', 'Video Name', 'Number of comments']).reset_index(drop=True)
        df10.index += 1
        st.dataframe(df10)

    # Close SQL connection
    connect_for_question.close()
