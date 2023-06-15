import kaggle
import boto3
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import os
import json
from sqlalchemy import create_engine
import pymysql

# add kaggle api credentials in environment


# Call and Authenticate API
api = KaggleApi()
api.authenticate()

# Use AWS Secrets to get database credentials
client = boto3.client('secretsmanager')
response = client.get_secret_value(
    ## SecretId = [insert secret name here]
)

secretDict = json.loads(response['SecretString'])
host_name = secretDict['host']
port = secretDict['port']
user_id = secretDict['username']
pwd = secretDict['password']
db_name = secretDict['db_name']

# Connect to DB
conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
sqlEngine = create_engine(conn_str, pool_recycle=3600)

#download files using Kaggle API
api.dataset_download_file('wittmannf/episode-ratings-from-imdb-top-250-tv-series', file_name = 'imdb_top_250_series_global_ratings.csv', path='/tmp')
api.dataset_download_file('wittmannf/episode-ratings-from-imdb-top-250-tv-series', file_name = 'imdb_top_250_series_episode_ratings.csv', path='/tmp')
api.dataset_download_file('harshitshankhdhar/imdb-dataset-of-top-1000-movies-and-tv-shows', file_name = 'imdb_top_1000.csv', path='/tmp')
api.dataset_download_file('shivamb/amazon-prime-movies-and-tv-shows', file_name = 'amazon_prime_titles.csv', path='/tmp')
api.dataset_download_file('shivamb/hulu-movies-and-tv-shows', file_name = 'hulu_titles.csv', path='/tmp')
api.dataset_download_file('shivamb/disney-movies-and-tv-shows', file_name = 'disney_plus_titles.csv', path='/tmp')
api.dataset_download_file('shivamb/netflix-shows', file_name = 'netflix_titles.csv', path='/tmp')

#Read in CSVs
amazon_prime = pd.read_csv("/tmp/amazon_prime_titles.csv.zip", compression = 'zip')

netflix = pd.read_csv("/tmp/netflix_titles.csv.zip", compression = 'zip')

hulu = pd.read_csv("/tmp/hulu_titles.csv.zip", compression = 'zip')

top_250_global = pd.read_csv("/tmp/imdb_top_250_series_global_ratings.csv")

top_250_usa = pd.read_csv("/tmp/imdb_top_250_series_episode_ratings.csv")

disney_plus = pd.read_csv("/tmp/disney_plus_titles.csv")

imdb_1000 = pd.read_csv("/tmp/imdb_top_1000.csv")

#Add subscription column
amazon_prime["subscription"] = "amazon_prime"

netflix["subscription"] = "netflix"

hulu["subscription"] = "hulu"

disney_plus["subscription"] = "disney_plus"

#Drop duplicate movies with title and release year
netflix.drop_duplicates(subset=['title', 'release_year'], keep='first', inplace=True)
hulu.drop_duplicates(subset=['title', 'release_year'], keep='first', inplace=True)
amazon_prime.drop_duplicates(subset=['title', 'release_year'], keep='first', inplace=True)
disney_plus.drop_duplicates(subset=['title', 'release_year'], keep='first', inplace=True)

# combining the movies data csvs and cleaning
movies_data = pd.concat([amazon_prime, netflix, hulu, disney_plus])

# Make titles column lowercase, get rid of double space and leading and trailing white space
movies_data['title'] = movies_data['title'].str.lower()

movies_data['title'] = movies_data['title'].str.replace('  ', ' ')

movies_data['title'] = movies_data['title'].str.strip()

# clean release year column (remove special characters, leading and trailing white space)
movies_data['release_year'] = movies_data['release_year'].astype(str)
movies_data['release_year'] = movies_data['release_year'].str.replace('\W','',regex=True)
movies_data['release_year'] = movies_data['release_year'].str.strip()

movies_data['release_year'] = movies_data['release_year'].astype(int)

# list of valid rating inputs
valid_inputs = ['13+', 'ALL', '18+', 'R', 'TV-Y', 'TV-Y7', 'NR', '16+',
       'TV-PG', '7+', 'TV-14', 'TV-NR', 'TV-G', 'PG-13', 'TV-MA', 'G',
      'PG', 'NC-17', 'UNRATED', '16', 'AGES_16_', 'AGES_18_', 'ALL_AGES',
       'NOT_RATE', 'TV-Y7-FV', 'UR', 'NOT RATED']

#list of invalid duration inputs
invalid_rating = ['74 min', '84 min', '66 min', '2 Seasons',
       '93 min', '4 Seasons', '136 min', '91 min', '85 min', '98 min',
       '89 min', '94 min', '86 min', '3 Seasons', '121 min', '88 min',
       '101 min', '1 Season', '83 min', '100 min', '95 min', '92 min',
       '96 min', '109 min', '99 min', '75 min', '87 min', '67 min',
       '104 min', '107 min', '103 min', '105 min', '119 min', '114 min',
       '82 min', '90 min', '130 min', '110 min', '80 min', '6 Seasons',
       '97 min', '111 min', '81 min', '49 min', '45 min', '41 min',
       '73 min', '40 min', '36 min', '39 min', '34 min', '47 min',
       '65 min', '37 min', '78 min', '102 min', '129 min', '115 min',
       '112 min', '61 min', '106 min', '76 min', '77 min', '79 min',
       '157 min', '28 min', '64 min', '7 min', '5 min', '6 min',
       '127 min', '142 min', '108 min', '57 min', '118 min', '116 min',
       '12 Seasons', '71 min']


#some durations are in the ratings. This moves those over
movies_data.loc[movies_data['rating'].isin(invalid_rating), 'duration'] = movies_data.loc[movies_data['rating'].isin(invalid_rating), 'rating'] 
movies_data.loc[movies_data['rating'].isin(invalid_rating), 'rating'] = 'NOT RATED'

valid_duration = ['113 min', '110 min', '74 min', '69 min', '45 min', '52 min',
       '98 min', '131 min', '87 min', '92 min', '88 min', '93 min',
       '94 min', '46 min', '96 min', '1 Season', '104 min', '62 min',
       '50 min', '3 Seasons', '2 Seasons', '86 min', '36 min', '37 min',
       '103 min', '9 min', '18 min', '14 min', '20 min', '19 min',
       '22 min', '60 min', '6 min', '54 min', '5 min', '84 min',
       '126 min', '125 min', '109 min', '89 min', '85 min', '56 min',
       '40 min', '111 min', '33 min', '34 min', '95 min', '99 min',
       '78 min', '4 Seasons', '77 min', '55 min', '53 min', '115 min',
       '58 min', '49 min', '135 min', '91 min', '64 min', '59 min',
       '48 min', '122 min', '90 min', '102 min', '65 min', '114 min',
       '136 min', '70 min', '138 min', '100 min', '480 min', '4 min',
       '30 min', '152 min', '68 min', '57 min', '7 Seasons', '31 min',
       '151 min', '149 min', '9 Seasons', '141 min', '121 min', '79 min',
       '140 min', '51 min', '106 min', '75 min', '27 min', '107 min',
       '108 min', '38 min', '157 min', '43 min', '118 min', '139 min',
       '6 Seasons', '112 min', '15 min', '72 min', '5 Seasons', '116 min',
       '142 min', '71 min', '42 min', '81 min', '32 min', '66 min',
       '127 min', '159 min', '67 min', '29 min', '132 min', '101 min',
       '164 min', '73 min', '61 min', '80 min', '83 min', '44 min',
       '120 min', '26 min', '97 min', '23 min', '105 min', '82 min',
       '11 min', '148 min', '161 min', '123 min', '29 Seasons',
       '124 min', '143 min', '35 min', '47 min', '170 min', '19 Seasons',
       '3 min', '146 min', '601 min', '24 min', '21 Seasons', '154 min',
       '128 min', '133 min', '153 min', '119 min', '63 min', '169 min',
       '174 min', '144 min', '7 min', '137 min', '76 min', '39 min',
       '8 Seasons', '12 Seasons', '134 min', '163 min', '1 min',
       '145 min', '162 min', '41 min', '147 min', '155 min', '117 min',
       '167 min', '11 Seasons', '28 min', '25 min', '180 min', '2 min',
       '541 min', '240 min', '129 min', '178 min', '171 min', '21 min',
       '172 min', '173 min', '10 min', '166 min', '160 min', '130 min',
       '479 min', '13 min', '8 min', '10 Seasons', '17 min', '16 min',
       '158 min', '183 min', '12 min', '14 Seasons', '150 min', '481 min',
       '181 min', '156 min', '540 min', '177 min', '550 min', '485 min',
       '176 min', '193 min', '165 min', '175 min', '188 min', '187 min',
       '168 min', '190 min', '185 min', '209 min', '192 min', '182 min',
       '207 min', '269 min', '15 Seasons', '191 min', '229 min',
       '189 min', '17 Seasons', '273 min', '204 min', '212 min',
       '224 min', '13 Seasons', '203 min', '194 min', '233 min',
       '237 min', '230 min', '195 min', '253 min', '208 min', '186 min',
       '312 min', '214 min', '179 min', '200 min', '196 min', '228 min',
       '205 min', '201 min', '23 Seasons', '16 Seasons',
       '20 Seasons', '30 Seasons', '22 Seasons', '25 Seasons',
       '34 Seasons', '26 Seasons', '32 Seasons']

#making sure the durations are valid (non zero), otherwise delete the row if invalid
movies_data = movies_data[movies_data['duration'].isin(valid_duration)]

#change date format
movies_data['date_added'] = pd.to_datetime(movies_data['date_added'])

movies_data['date_added'] = movies_data['date_added'].dt.strftime("%Y-%m-%d")

#rename column from type to show_type
movies_data.rename(columns={'type': 'show_type'}, inplace = True)
movies_data.rename(columns={'cast': 'people_cast'}, inplace = True)
movies_data.rename(columns={'description': 'show_description'}, inplace = True)

#getting rid of special characters in gross
imdb_1000['Gross'] = imdb_1000['Gross'].astype(str)
imdb_1000['Gross'] = imdb_1000['Gross'].str.replace('\W','',regex=True)
imdb_1000['Gross'] = imdb_1000['Gross'].astype('double')

#rename code to show_code
top_250_usa.rename(columns={'Code': 'show_code'}, inplace = True)
top_250_global.rename(columns={'Code': 'show_code'}, inplace = True)


##code that removes leading and trailing whitespace, removes special characters, and makes everything lowercase

top_250_global['Title'] = top_250_global['Title'].str.lower()

top_250_global['Title'] = top_250_global['Title'].str.replace('  ', ' ')

top_250_usa['Title'] = top_250_usa['Title'].str.lower()

top_250_usa['Title'] = top_250_usa['Title'].str.replace('  ', ' ')

imdb_1000['Series_Title'] = imdb_1000['Series_Title'].str.lower()

imdb_1000['Series_Title'] = imdb_1000['Series_Title'].str.replace('  ', ' ')

top_250_global['Title'] = top_250_global['Title'].str.strip()

top_250_usa['Title'] = top_250_usa['Title'].str.strip()

imdb_1000['Series_Title'] = imdb_1000['Series_Title'].str.strip()

#moving columns around
top_250_global = top_250_global[['Title', 'show_code', 'Rating', 'Rating Count']]
top_250_usa = top_250_usa[['Title', 'show_code', 'Season', 'Episode', 'Rating']]
top_250_global['Rating Count'] = top_250_global['Rating Count'].str.replace('\W','',regex=True)
top_250_global['Rating Count'] = top_250_global['Rating Count'].astype('double')

top_250_global.rename(columns={'Rating Count': 'rating_count'}, inplace=True)

# export to csv
imdb_1000.to_csv('/tmp/final_imdb_1000.csv', index = None)
top_250_usa.to_csv('/tmp/final_top_250_usa.csv', index = None)
top_250_global.to_csv('/tmp/final_top_250_global.csv', index = None)

groups = movies_data.groupby('subscription')

for name, group in groups:
    group.to_csv('/tmp/'+'final_'+f"{name}.csv", index = False)

f_netflix = pd.read_csv('/tmp/final_netflix.csv')
f_amazon_prime = pd.read_csv('/tmp/final_amazon_prime.csv')
f_hulu = pd.read_csv('/tmp/final_hulu.csv')
f_disney_plus = pd.read_csv('/tmp/final_disney_plus.csv')

client = boto3.client('s3')
# bucket = 'insert bucket here'

#Upload to S3
client.upload_file('/tmp/final_imdb_1000.csv', bucket, 'imdb_1000.csv')
client.upload_file('/tmp/final_top_250_usa.csv', bucket, 'top_250_usa.csv')
client.upload_file('/tmp/final_top_250_global.csv', bucket, 'top_250_global.csv')
client.upload_file('/tmp/final_netflix.csv', bucket, 'netflix.csv')
client.upload_file('/tmp/final_hulu.csv', bucket, 'hulu.csv')
client.upload_file('/tmp/final_amazon_prime.csv', bucket, 'amazon_prime.csv')
client.upload_file('/tmp/final_disney_plus.csv', bucket, 'disney_plus.csv')

#Upload to RDS
f_netflix.to_sql('netflix', con=sqlEngine, if_exists="replace", index=False, chunksize=1000)
f_hulu.to_sql('hulu', con=sqlEngine, if_exists="replace", index=False, chunksize=1000)
f_amazon_prime.to_sql('amazon_prime', con=sqlEngine, if_exists="replace", index=False, chunksize=1000)
f_disney_plus.to_sql('disney_plus', con=sqlEngine, if_exists="replace", index=False, chunksize=1000)

imdb_1000.to_sql('imdb_1000', con=sqlEngine, if_exists="replace", index=False, chunksize=1000)
top_250_usa.to_sql('top_250_usa', con=sqlEngine, if_exists="replace", index=False, chunksize=1000)
top_250_global.to_sql('top_250_global', con=sqlEngine, if_exists="replace", index=False, chunksize=1000)



def lambda_handler(event, context):


    return {
        
    }
