import requests 
import sqlite3
import pandas as pd
import numpy as np 
from datetime import datetime 
from bs4 import BeautifulSoup 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchnge_rate_csv_path = 'E:/Data Engineeing/ETL_Project/exchange_rate.csv'
table_attrbs = ['Name', 'MC_USD_Billion']
output_csv_path = 'E:/Data Engineeing/ETL_Project/Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
Log_file = 'E:/Data Engineeing/ETL_Project/code_log.txt'

def log_progress(message):

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(Log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')



def extract(url, table_attrbs): 
    
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attrbs)
    
    tbody = soup.find_all('tbody')
    rows = tbody[0].find_all('tr')
    
    data_list = []
    
    for row in rows:
        cols = row.find_all('td')
        
        if len(cols) >= 3:
            data_dict = {
                'Name': cols[1].get_text(strip=True),
                'MC_USD_Billion':cols[2].get_text(strip=True)}
            
            data_list.append(data_dict)
            
    df1 = pd.DataFrame(data_list)
    df = pd.concat([df,df1], ignore_index=True)
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    
    return df

def transform(df, exchnge_rate_csv_path):
    exchange_rate_df = pd.read_csv(exchnge_rate_csv_path)
    
    # Converting the contents to a dictionary
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df 


def load_to_csv(df, output_csv_path): 
    return df.to_csv(output_csv_path) 


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attrbs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, exchnge_rate_csv_path)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
