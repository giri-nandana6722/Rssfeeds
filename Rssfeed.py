import requests
import feedparser
import settings
import sqlite3
import pandas as pd
import logging
import pytz
from bs4 import BeautifulSoup
from datetime import datetime
import time

# Configure logging
logging.basicConfig(filename='app.log', level=logging.INFO)

# SQLite database connection
conn = sqlite3.connect('jobread.db')
cursor = conn.cursor()

# Function to format the CREATE TABLE SQL statement
def ddl_formatter(key_list, table_name):           
    table_ddl = f'CREATE TABLE IF NOT EXISTS {table_name} ( '
    for key in key_list:
        table_ddl = table_ddl + f'`{key}` TEXT NULL, '[:-1]
    # Add primary key constraint
    table_ddl += 'PRIMARY KEY (title, published))'
    return table_ddl

# Function to convert dates to GMT (UTC)
def convert_to_gmt(date_str, parsed_date=None):
    if parsed_date:
        # Convert struct_time to datetime and then to GMT
        date = datetime(*parsed_date[:6], tzinfo=pytz.utc)
        return date.strftime('%a, %d %b %Y %H:%M:%S %z')
    else:
        try:
            if "GMT" in date_str:
                date_str = date_str.replace(" GMT", " +0000")
            try:
                date = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
            except ValueError:
                try:
                    date = datetime.strptime(date_str, '%a, %d %B %Y %H:%M:%S')
                    date = date.replace(tzinfo=pytz.utc)
                except ValueError:
                    try:
                        date = datetime.strptime(date_str, '%a, %d %b %Y')
                        date = date.replace(tzinfo=pytz.utc)
                    except ValueError:
                        logging.error(f"Failed to parse the date string: {date_str}")
                        return 'NA'
            gmt_date = date.astimezone(pytz.utc)
            return gmt_date.strftime('%a, %d %b %Y %H:%M:%S %z')
        except Exception as e:
            logging.error(f"Error in date conversion: {e}")
            return 'NA'

# Function to insert bulk data into the database
def insert_bulk_data(cursor, data, table_name):
    df = pd.DataFrame(data)
    
    # Convert 'published' to GMT using both 'published' and 'published_parsed'
    if 'published' in df.columns and 'published_parsed' in df.columns:
        df['published'] = df.apply(lambda row: convert_to_gmt(row.get('published'), row.get('published_parsed')), axis=1)
    elif 'published' in df.columns:  # Fallback to using only 'published' if 'published_parsed' is not available
        df['published'] = df['published'].apply(convert_to_gmt)
    elif 'pubdate' in df.columns:
        df['pubdate'] = df['pubdate'].apply(convert_to_gmt)
    
    df.fillna('NA', inplace=True)
    data = df.to_dict(orient='records')
    
    list_of_tuple = []
    for job in data:
        job_data = []
        for key, value in job.items():
            job_data.append(str(value))
        print(f'Total Columns: {len(job_data)}')
        list_of_tuple.append(tuple(job_data))
    
    insert_query = f'INSERT OR REPLACE INTO {table_name} ('
    column_str = ''
    num_columns_str = ''
    for job in data:
        for key, value in job.items():
            column_str = column_str + f'`{key}`,'
            num_columns_str = num_columns_str + '?,'
        break   
    
    insert_query = insert_query + column_str[:-1] + ') VALUES (' + num_columns_str[:-1] + ')'
    
    logging.info(f'Constructed Insert Query: {insert_query}')
    logging.info(f'Total Entries to Insert: {len(list_of_tuple)}')
    logging.debug(f'First 10 Entries: {list_of_tuple[:10]}')
    try:
        cursor.executemany(insert_query, list_of_tuple)
        logging.info('Data successfully inserted into the database.')
    except sqlite3.Error as e:
        logging.error(f'Error during data insertion: {e}')

 
# Function to clean HTML tags from text             
def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, "lxml")
    return soup.get_text() 
 
# Function to clean summaries
def clean_summary(summary):
    return clean_html(summary) 
       
 # Dynamic SQL select query based on keywords       
def dynamic_select(item,cursor):
    list_of_keywords =  ['dsa', 'dba', 'res']
    list_of_columns=['title','summary','summary_detail']
    keyword_str = "|".join(list_of_keywords) 
    table_name = item['name']
    select_query = f"SELECT `title`,`summary`,`summary_detail` FROM {table_name} WHERE "
    conditions = []
    for column in list_of_columns:
            for keyword in list_of_keywords:
                conditions.append(f"{column} LIKE '%{keyword}%'")
    select_query += " OR ".join(conditions)
    cursor.execute(select_query)
    # Fetch all matching rows
    results = cursor.fetchall()    
    # Convert the results to a DataFrame
    df = pd.DataFrame(results,columns=['title', 'summary', 'summary_detail'])  
    return df

# Function to process jobs from feeds
def get_jobs():
    global cursor
    master_df = pd.DataFrame()
    
    for feed in settings.feed_list:
        df = dynamic_select(feed, cursor)
        print(len(df.index))
        master_df = pd.concat([master_df, df], ignore_index=True)
        
    logging.info(f'Total records: {len(master_df)}')

    print('Total Sum of records: ',len(master_df.index))
    if 'summary' in master_df.columns:
        master_df['summary'] = master_df['summary'].apply(clean_summary)
    else:
        print("No 'summary' column found in the DataFrame.")
    
    master_df['summary'] = master_df['summary'].apply(clean_summary)
    master_df.to_csv('jobs_clean.csv')
    return master_df
    
# Function to read and process RSS feeds
def feed_reader():
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Cookie": "visid_incap_2388351=SOL5CwWBRbanBipyfuHIg+bka2YAAAAAQUIPAAAAAAA1ZdQvctkS9pr6/DhRGwrm; _swb=66c5cf1e-9c53-44e2-9f6c-740a18ae099b; _ketch_consent_v1_=eyJhbmFseXRpY3MiOnsic3RhdHVzIjoiZ3JhbnRlZCIsImNhbm9uaWNhbFB1cnBvc2VzIjpbImFuYWx5dGljcyJdfSwiYmVoYXZpb3JhbF9hZHZlcnRpc2luZyI6eyJzdGF0dXMiOiJncmFudGVkIiwiY2Fub25pY2FsUHVycG9zZXMiOlsiYmVoYXZpb3JhbF9hZHZlcnRpc2luZyJdfSwiZXNzZW50aWFsX3NlcnZpY2VzIjp7InN0YXR1cyI6ImdyYW50ZWQiLCJjYW5vbmljYWxQdXJwb3NlcyI6WyJlc3NlbnRpYWxfc2VydmljZXMiXX19; _ga=GA1.1.77564915.1718346991; _gcl_au=1.1.1502627644.1718346991; _ga_6ZQNJ4ELG2=GS1.1.1718368845.3.0.1718368845.0.0.0; CFID=137823350; CFTOKEN=469a605e841da95e-2E7E6AC1-DC6F-2C5F-097B3AB440AB5156; nlbi_2388351=egxneoD1iVVo0KwnokRrFAAAAACpqj0MtF+6I6LPRjFrOnSc; incap_ses_48_2388351=i6LIDCCVvys30KUR6YeqAIUB12YAAAAAMWXP76y7X33WNzJVV8vaew==",
}

    for feed in settings.feed_list:
        feed_url = feed['url']
        response = requests.get(feed_url, headers=headers)
        
        if response.status_code == 200:
            data = feedparser.parse(response.content)
        
        print(data.entries)
        
        key_list = []
        for job in data.entries:
            for key, value in job.items():
                key_list.append(key)
            break
        table_name = feed['name']
        print('--->',key_list)
        custom_ddl = ddl_formatter(key_list, table_name)
        print('CREATE DDL: ', custom_ddl)
        
        if key_list:
            cursor.execute(custom_ddl)

        insert_bulk_data(cursor, data.entries, table_name)
        conn.commit()
        
 #Run the feed reader and process jobs
feed_reader()
get_jobs()

# Delay execution
time.sleep(100)






# lxml