# YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING

The "YouTube Data Harvesting and Warehousing" project is a comprehensive data management solution designed to harness the vast reservoir of information available on YouTube. This innovative application seamlessly integrates with the YouTube API, facilitating the retrieval of channel and video data. The key functionalities include storing this data in a MongoDB database, treating it as a dynamic data lake, and subsequently migrating it to a SQL database for efficient querying and in-depth analysis. To enhance user accessibility, the project incorporates a streamlined search and retrieval mechanism with a user-friendly web interface developed using Streamlit.

Key Functions:

YouTube API Integration:
The heart of the system lies in its ability to interact with the YouTube API, enabling the real-time extraction of comprehensive channel and video data. This includes metadata such as video titles, descriptions, upload dates, view counts, likes, dislikes, comments, and other pertinent information. The integration ensures that the application stays synchronized with the latest content and trends on YouTube.

MongoDB Data Lake:
The harvested data finds its initial repository in a MongoDB database, functioning as a flexible and scalable data lake. MongoDB's document-oriented architecture accommodates various data types, allowing for seamless storage and retrieval. This ensures that the data is stored in an organized manner, ready for subsequent processing.

SQL Database Migration:
Recognizing the significance of efficient querying and analysis, the application facilitates the migration of curated data from the MongoDB data lake to a SQL database. This transition optimizes the data for relational querying, setting the stage for more sophisticated analyses and insights.

Search and Retrieval with Streamlit Webpage:
The project enhances user interaction by providing a web interface developed using Streamlit. Users can perform targeted searches using a variety of search options within the SQL database. This includes keyword searches, time-based filters, popularity metrics, and channel-specific queries. The search results are dynamically presented on the Streamlit webpage, offering an intuitive and user-friendly experience for exploring and retrieving valuable insights.

# TOOLS USED

* Jupiter Notebook
* MongoDB
* MySQL Workbench 8.0
* Visual Studio Code

# LIBRARIES USED

From googleapiclient.discovery import build
import pymongo
import pymysql
import pandas
import streamlit

PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

GOOGLE API CLIENT: The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

MONGODB: MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data. It uses a JSON-like format to store documents.

MYSQL: MySQL is an open-source relational database management system, advanced, and highly scalable database management system (DBMS) known for its reliability and extensive features. It provides a platform for storing and managing structured data, offering support for various data types and advanced SQL capabilities.

STREAMLIT: Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.
